"""gold 层：面向双方核对的可读仓表。

项目组视角（按原样）：
- sample_coverage        覆盖、尝试次数（失败复测留在分母）、最近证据与缺口原因
- sample_retained        可用留样：物理余额与根成分体积两个口径并列
- coverage_gap           未覆盖原样的缺口明细，可下钻

实验室视角（按真实运行，质控失效不抹账）：
- lab_run_workload       实际运行数与有效运行数分列
- lab_well_workload      实际占孔与有效占孔分列
- lab_consumption        实际耗用与有效耗用，含混池按份额的根原样分摊
- root_consumption       每个根原样被各孔分摊的真实消耗（不重复记整池）

下钻：
- material_transform_lineage  从汇总下钻到材料转化与原文件测量
- drilldown_measurement       孔到归一化/原始测量的下钻
"""
from __future__ import annotations

import duckdb

VIEW_BUILDERS: list[str] = []


def build_marts(con: duckdb.DuckDBPyConnection) -> dict:
    # ---------------- 项目组：覆盖 ----------------
    con.execute("DROP TABLE IF EXISTS gold.sample_coverage")
    con.execute("""
        CREATE TABLE gold.sample_coverage AS
        WITH root AS (
            SELECT material_id AS root_material_id, project
            FROM silver.material WHERE is_root_sample
        ),
        agg AS (
            SELECT root_material_id,
                   count(*) AS attempt_wells,
                   count(DISTINCT run_id) AS attempt_runs,
                   sum(CAST(eligible AS INTEGER)) AS eligible_wells,
                   count(DISTINCT CASE WHEN eligible THEN run_id END) AS eligible_runs,
                   bool_or(eligible) AS has_evidence,
                   max(CASE WHEN eligible THEN 1 ELSE 0 END) AS any_eligible,
                   string_agg(DISTINCT CASE WHEN eligible THEN coverage_kind END, ',')
                       FILTER (WHERE eligible) AS evidence_kinds
            FROM silver.well_root_evidence
            GROUP BY root_material_id
        ),
        latest AS (
            SELECT root_material_id, run_id AS latest_evidence_run, reason AS latest_reason
            FROM (
                SELECT e.*, row_number() OVER (
                    PARTITION BY root_material_id ORDER BY r.started DESC, e.run_id DESC) rn
                FROM silver.well_root_evidence e JOIN silver.run_fact r USING (run_id)
            ) WHERE rn = 1
        )
        SELECT root.root_material_id, root.project,
               coalesce(agg.attempt_wells, 0) AS attempt_wells,
               coalesce(agg.attempt_runs, 0) AS attempt_runs,
               coalesce(agg.eligible_wells, 0) AS eligible_wells,
               coalesce(agg.eligible_runs, 0) AS eligible_runs,
               coalesce(agg.has_evidence, false) AS has_evidence,
               agg.evidence_kinds AS evidence_kinds,
               latest.latest_evidence_run, latest.latest_reason
        FROM root LEFT JOIN agg USING (root_material_id)
        LEFT JOIN latest USING (root_material_id)
    """)

    # ---------------- 项目组：留样（两个体积口径） ----------------
    # 每个当前仍持有的物理管只计一次：祖先管与含其成分的子管不重复相加。
    # 根等效体积 = 该管余额按入流时根成分份额折算；初始量减全部耗用/废弃应可对账。
    con.execute("DROP TABLE IF EXISTS gold.sample_retained")
    con.execute("""
        CREATE TABLE gold.sample_retained AS
        WITH per_tube AS (
            SELECT o.root_material_id, o.material_id,
                   greatest(b.balance_ul, 0) AS tube_balance,
                   greatest(b.balance_ul, 0)
                       * o.component_volume_ul / nullif(b.inflow_ul, 0) AS root_equiv,
                   b.negative_balance
            FROM silver.material_origin o
            JOIN silver.material_balance b ON b.material_id = o.material_id
            WHERE coalesce(b.balance_ul, 0) > 0
        )
        SELECT root_material_id,
               count(DISTINCT material_id) AS surviving_tubes,
               string_agg(DISTINCT material_id, ', ' ORDER BY material_id) AS tube_ids,
               sum(tube_balance) AS retained_physical_ul,
               sum(root_equiv) AS retained_root_equiv_ul,
               max(CAST(negative_balance AS INTEGER)) AS has_negative_balance
        FROM per_tube
        GROUP BY root_material_id
    """)

    # ---------------- 项目组：缺口与原因 ----------------
    con.execute("DROP TABLE IF EXISTS gold.coverage_gap")
    con.execute("""
        CREATE TABLE gold.coverage_gap AS
        SELECT c.root_material_id, c.project,
               c.attempt_wells, c.has_evidence,
               CASE
                 WHEN c.attempt_wells = 0 THEN 'no_experiment'
                 WHEN c.latest_reason = 'qc_failed' THEN 'qc_failed'
                 WHEN c.latest_reason = 'pool_not_allowed' THEN 'pool_not_allowed'
                 WHEN c.latest_reason = 'dilution_exceeded' THEN 'dilution_exceeded'
                 WHEN c.latest_reason = 'freeze_thaw_exceeded' THEN 'freeze_thaw_exceeded'
                 WHEN c.latest_reason = 'lineage_unclosed' THEN 'lineage_unclosed'
               END AS gap_reason,
               c.latest_evidence_run AS related_run
        FROM gold.sample_coverage c
        WHERE NOT c.has_evidence
    """)

    # ---------------- 实验室：运行工作量（失败保留） ----------------
    con.execute("DROP TABLE IF EXISTS gold.lab_run_workload")
    con.execute("""
        CREATE TABLE gold.lab_run_workload AS
        SELECT method, method_version,
               count(*) AS actual_runs,
               sum(CAST(qc_passed AS INTEGER)) AS effective_runs,
               sum(CAST(NOT qc_passed AS INTEGER)) AS failed_runs,
               sum(CAST(is_rerun AS INTEGER)) AS rerun_runs
        FROM silver.run_fact
        GROUP BY method, method_version
    """)

    # ---------------- 实验室：占孔工作量 ----------------
    con.execute("DROP TABLE IF EXISTS gold.lab_well_workload")
    con.execute("""
        CREATE TABLE gold.lab_well_workload AS
        SELECT r.method, r.method_version,
               count(*) AS actual_wells,
               sum(CAST(w.qc_passed AS INTEGER)) AS effective_wells,
               sum(CAST(NOT w.qc_passed AS INTEGER)) AS failed_wells,
               sum(w.input_volume_ul) AS actual_input_ul,
               sum(CASE WHEN w.qc_passed THEN w.input_volume_ul ELSE 0 END) AS effective_input_ul
        FROM silver.well_occupancy w JOIN silver.run_fact r USING (run_id)
        GROUP BY r.method, r.method_version
    """)

    # ---------------- 实验室：材料耗用（物理口径，失败不抹） ----------------
    con.execute("DROP TABLE IF EXISTS gold.lab_consumption")
    con.execute("""
        CREATE TABLE gold.lab_consumption AS
        SELECT wi.material_id, m.kind,
               count(DISTINCT wi.run_id || '/' || wi.plate_id || '/' || wi.well) AS wells_used,
               sum(wi.volume_ul) AS consumed_ul,
               sum(CASE WHEN r.qc_passed THEN wi.volume_ul ELSE 0 END) AS consumed_in_effective_runs_ul,
               sum(CASE WHEN NOT r.qc_passed THEN wi.volume_ul ELSE 0 END) AS consumed_in_failed_runs_ul
        FROM silver.well_input wi
        JOIN silver.run_fact r USING (run_id)
        LEFT JOIN silver.material m ON m.material_id = wi.material_id
        GROUP BY wi.material_id, m.kind
    """)

    # ---------------- 根原样分摊消耗（混池按份额，不重复整池） ----------------
    con.execute("DROP TABLE IF EXISTS gold.root_consumption")
    con.execute("""
        CREATE TABLE gold.root_consumption AS
        SELECT l.root_material_id, l.run_id, l.plate_id, l.well,
               wi.material_id, r.qc_passed,
               wi.volume_ul * l.component_share AS root_charged_ul
        FROM silver.well_root_link l
        JOIN silver.well_input wi
          ON wi.run_id = l.run_id AND wi.plate_id = l.plate_id
         AND wi.well = l.well AND wi.material_id = l.material_id
        JOIN silver.run_fact r ON r.run_id = l.run_id
    """)

    # ---------------- 下钻：材料转化链 ----------------
    con.execute("DROP TABLE IF EXISTS gold.material_transform_lineage")
    con.execute("""
        CREATE TABLE gold.material_transform_lineage AS
        SELECT e.transform_id, e.transform_kind, e.occurred_at,
               e.parent_material_id, e.child_material_id,
               e.input_volume_ul, e.output_volume_ul, e.output_share,
               c.volume_closed, c.sources_complete, c.dilution_factor
        FROM silver.lineage_edge e
        LEFT JOIN silver.transform_closure c USING (transform_id)
    """)

    # ---------------- 下钻：孔到测量 ----------------
    con.execute("DROP TABLE IF EXISTS gold.drilldown_measurement")
    con.execute("""
        CREATE TABLE gold.drilldown_measurement AS
        WITH scope AS (
            SELECT analyte, method, from_version AS version, from_channel AS channel
            FROM silver.channel_mapping
            UNION ALL
            SELECT analyte, method, to_version, to_channel
            FROM silver.channel_mapping
        )
        SELECT nm.run_id, nm.plate_id, nm.well, nm.replicate, nm.analyte,
               nm.canonical_unit, nm.canonical_value, nm.method_version,
               nm.comparable_versions,
               mf.channel, mf.unit AS raw_unit, mf.raw_value,
               mf.export_ids, mf.export_count
        FROM silver.normalized_measurement nm
        JOIN silver.run_fact r ON r.run_id = nm.run_id
        JOIN silver.measurement_fact mf
          ON mf.run_id = nm.run_id AND mf.plate_id = nm.plate_id
         AND mf.well = nm.well AND mf.replicate = nm.replicate
        LEFT JOIN scope s
          ON s.method = r.method AND s.version = nm.method_version
         AND s.channel = mf.channel AND s.analyte = nm.analyte
        WHERE (nm.analyte LIKE '__unmapped__%' AND mf.channel = substring(nm.analyte, 13))
           OR s.analyte IS NOT NULL
    """)

    return {table: con.execute(f"SELECT count(*) FROM gold.{table}").fetchone()[0]
            for table in [
                "sample_coverage", "sample_retained", "coverage_gap",
                "lab_run_workload", "lab_well_workload", "lab_consumption",
                "root_consumption", "material_transform_lineage", "drilldown_measurement",
            ]}
