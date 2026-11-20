"""实验执行与证据资格（silver）。

两层口径严格分开：
- 实际执行：按 run_id / plate / well 计占孔，失败运行仍计工作量与耗用；
  仪器重复导出（同 run/plate/well/channel/replicate）去重，不新计工作。
- 有效证据：质控通过且满足方法契约（混池、稀释、冻融、检测对象）才成立；
  混池只给定性覆盖，不产生单原样数值；失败复测保留在分母里。

方法换版：channel_mapping 显式声明可比较口径（含线性换算），未声明的
新通道作为不可比较测量原样保留，不硬套历史同义词。
"""
from __future__ import annotations

import duckdb

from . import shared

# 内置的版本可比性声明（合成资料口径：v1 signal(ng/mL) 与 v2 mass_concentration(ug/L)
# 数值可比，换算 1 ng/mL = 1 ug/L；v2 ratio 为无量纲新指标，不映射）。
DEFAULT_CHANNEL_MAPPING = [
    {
        "analyte": "analyte_conc",
        "method": "M2",
        "from_version": "v1", "from_channel": "signal",
        "to_version": "v2", "to_channel": "mass_concentration",
        "canonical_unit": "ug/L", "slope": 1.0, "intercept": 0.0,
        "comparable": True,
    },
    # ratio 刻意不出现：保留为不可比较测量
]


def build_experiment(con: duckdb.DuckDBPyConnection,
                     channel_mapping: list[dict] | None = None,
                     as_of=None) -> dict:
    mapping = DEFAULT_CHANNEL_MAPPING if channel_mapping is None else channel_mapping
    _ddl(con)
    _load_runs_and_wells(con, as_of)
    _load_measurements(con, mapping)
    _build_well_roots(con)
    _build_evidence(con)
    return {
        "runs": con.execute("SELECT count(*) FROM silver.run_fact").fetchone()[0],
        "well_occupancy": con.execute("SELECT count(*) FROM silver.well_occupancy").fetchone()[0],
        "measurements": con.execute("SELECT count(*) FROM silver.measurement_fact").fetchone()[0],
        "evidence_links": con.execute("SELECT count(*) FROM silver.well_root_evidence").fetchone()[0],
    }


def _ddl(con) -> None:
    con.execute("DROP TABLE IF EXISTS silver.run_fact")
    con.execute("""
        CREATE TABLE silver.run_fact (
            run_id VARCHAR PRIMARY KEY, method VARCHAR, method_version VARCHAR,
            started TIMESTAMP, qc VARCHAR, qc_passed BOOLEAN,
            rerun_of VARCHAR, is_rerun BOOLEAN
        )""")
    con.execute("DROP TABLE IF EXISTS silver.well_occupancy")
    con.execute("""
        CREATE TABLE silver.well_occupancy (
            run_id VARCHAR, plate_id VARCHAR, well VARCHAR,
            method VARCHAR, method_version VARCHAR, qc_passed BOOLEAN,
            input_volume_ul DOUBLE, distinct_materials INTEGER,
            PRIMARY KEY (run_id, plate_id, well)
        )""")
    con.execute("DROP TABLE IF EXISTS silver.well_input")
    con.execute("""
        CREATE TABLE silver.well_input (
            input_id VARCHAR PRIMARY KEY, run_id VARCHAR, plate_id VARCHAR, well VARCHAR,
            material_id VARCHAR, volume_ul DOUBLE
        )""")
    con.execute("DROP TABLE IF EXISTS silver.measurement_fact")
    con.execute("""
        CREATE TABLE silver.measurement_fact (
            run_id VARCHAR, plate_id VARCHAR, well VARCHAR, channel VARCHAR,
            unit VARCHAR, replicate INTEGER, method_version VARCHAR,
            raw_value DOUBLE, export_ids VARCHAR, export_count INTEGER,
            PRIMARY KEY (run_id, plate_id, well, channel, replicate)
        )""")
    con.execute("DROP TABLE IF EXISTS silver.channel_mapping")
    con.execute("""
        CREATE TABLE silver.channel_mapping (
            analyte VARCHAR, method VARCHAR, from_version VARCHAR, from_channel VARCHAR,
            to_version VARCHAR, to_channel VARCHAR, canonical_unit VARCHAR,
            slope DOUBLE, intercept DOUBLE, comparable BOOLEAN,
            PRIMARY KEY (method, from_version, from_channel, to_version, to_channel)
        )""")
    con.execute("DROP TABLE IF EXISTS silver.normalized_measurement")
    con.execute("""
        CREATE TABLE silver.normalized_measurement (
            run_id VARCHAR, plate_id VARCHAR, well VARCHAR, replicate INTEGER,
            analyte VARCHAR, canonical_unit VARCHAR, canonical_value DOUBLE,
            method_version VARCHAR, comparable_versions BOOLEAN
        )""")
    con.execute("DROP TABLE IF EXISTS silver.method_rule")
    con.execute("""
        CREATE TABLE silver.method_rule (
            method VARCHAR PRIMARY KEY, allows_pool BOOLEAN,
            max_dilution DOUBLE, max_freeze_thaw INTEGER
        )""")
    con.execute("DROP TABLE IF EXISTS silver.well_root_link")
    con.execute("""
        CREATE TABLE silver.well_root_link (
            run_id VARCHAR, plate_id VARCHAR, well VARCHAR, material_id VARCHAR,
            root_material_id VARCHAR, component_share DOUBLE, is_pooled BOOLEAN
        )""")
    con.execute("DROP TABLE IF EXISTS silver.well_root_evidence")
    con.execute("""
        CREATE TABLE silver.well_root_evidence (
            run_id VARCHAR, plate_id VARCHAR, well VARCHAR, root_material_id VARCHAR,
            material_id VARCHAR, qc_passed BOOLEAN, is_pooled BOOLEAN,
            dilution_factor DOUBLE, freeze_thaw_count INTEGER,
            eligible BOOLEAN, reason VARCHAR, coverage_kind VARCHAR
        )""")


def _load_runs_and_wells(con, as_of=None) -> None:
    from .ingest import latest_rows
    rows = latest_rows(con, as_of)
    for r in rows["runs"]:
        con.execute("INSERT INTO silver.run_fact VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
            r["run_id"], r["method"], r["method_version"], shared.parse_ts(r["started"]),
            r["qc"], r["qc"] == "passed", r["rerun_of"], r["rerun_of"] is not None,
        ])
    for r in rows["method_rules"]:
        con.execute("INSERT INTO silver.method_rule VALUES (?, ?, ?, ?)", [
            r["method"], r["allows_pool"], float(r["max_dilution"]), int(r["max_freeze_thaw"]),
        ])
    for w in rows["well_inputs"]:
        con.execute("INSERT INTO silver.well_input VALUES (?, ?, ?, ?, ?, ?)", [
            w["input_id"], w["run_id"], w["plate_id"], w["well"], w["material_id"],
            float(w["volume_ul"]),
        ])
    con.execute("""
        INSERT INTO silver.well_occupancy
        SELECT w.run_id, w.plate_id, w.well,
               r.method, r.method_version, r.qc_passed,
               sum(w.volume_ul), count(DISTINCT w.material_id)
        FROM silver.well_input w JOIN silver.run_fact r USING (run_id)
        GROUP BY w.run_id, w.plate_id, w.well, r.method, r.method_version, r.qc_passed
    """)


def _load_measurements(con, mapping: list[dict]) -> None:
    for m in mapping:
        con.execute("INSERT INTO silver.channel_mapping VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            m["analyte"], m["method"], m["from_version"], m["from_channel"],
            m["to_version"], m["to_channel"], m["canonical_unit"],
            float(m["slope"]), float(m["intercept"]), bool(m["comparable"]),
        ])
    # 去重：同 run/plate/well/channel/replicate 只保留一行；重复导出计入 export_count。
    con.execute("""
        INSERT INTO silver.measurement_fact
        SELECT run_id, plate_id, well, channel, unit, replicate,
               any_value(method_version) AS method_version,
               arg_min(value, file_id) AS raw_value,
               string_agg(DISTINCT export_id, ', ' ORDER BY export_id) AS export_ids,
               count(DISTINCT export_id) AS export_count
        FROM bronze.instrument_raw_measurement
        GROUP BY run_id, plate_id, well, channel, unit, replicate
    """)
    # 归一化：有可比映射的版本归到共同 analyte 与单位；没有映射的通道单独保留、不可比。
    # 把每条映射展开成两个版本方向，使任一版本的测量都能命中。
    con.execute("""
        CREATE TEMP TABLE _channel_scope AS
        SELECT analyte, method, from_version AS version, from_channel AS channel,
               canonical_unit, slope, intercept, comparable
        FROM silver.channel_mapping
        UNION ALL
        SELECT analyte, method, to_version, to_channel,
               canonical_unit, slope, intercept, comparable
        FROM silver.channel_mapping
    """)
    con.execute("""
        INSERT INTO silver.normalized_measurement
        SELECT m.run_id, m.plate_id, m.well, m.replicate,
               coalesce(cm.analyte, '__unmapped__' || m.channel) AS analyte,
               coalesce(cm.canonical_unit, m.unit) AS canonical_unit,
               CASE WHEN cm.analyte IS NULL THEN m.raw_value
                    ELSE m.raw_value * cm.slope + cm.intercept END AS canonical_value,
               m.method_version,
               cm.analyte IS NOT NULL AS comparable_versions
        FROM silver.measurement_fact m
        LEFT JOIN silver.run_fact r ON r.run_id = m.run_id
        LEFT JOIN _channel_scope cm
          ON cm.method = r.method AND cm.version = m.method_version
         AND cm.channel = m.channel
    """)
    con.execute("DROP TABLE _channel_scope")


def _build_well_root_links(con) -> None:
    """把每个孔消费的材料展开到根原样，记录成分份额与是否混池。"""
    con.execute("""
        INSERT INTO silver.well_root_link
        SELECT w.run_id, w.plate_id, w.well, w.material_id,
               o.root_material_id,
               o.component_volume_ul / nullif(m.inflow_ul, 0) AS component_share,
               count(*) OVER (PARTITION BY w.run_id, w.plate_id, w.well) > 1
                 OR (SELECT count(*) FROM silver.material_origin o2
                     WHERE o2.material_id = w.material_id) > 1 AS is_pooled
        FROM silver.well_input w
        JOIN silver.material_origin o ON o.material_id = w.material_id
        JOIN silver.material_balance m ON m.material_id = w.material_id
    """)


def _build_well_roots(con) -> None:
    _build_well_root_links(con)
    # 孔-根去重（同一孔同一根可能经多材料进入）
    con.execute("""
        CREATE OR REPLACE TABLE silver.well_root_link AS
        SELECT run_id, plate_id, well, material_id, root_material_id,
               max(component_share) AS component_share,
               bool_or(is_pooled) AS is_pooled
        FROM silver.well_root_link
        GROUP BY run_id, plate_id, well, material_id, root_material_id
    """)


def _build_evidence(con) -> None:
    """逐孔-根判定证据资格；失败与不合规都保留并给出原因。"""
    rows = con.execute("""
        SELECT l.run_id, l.plate_id, l.well, l.root_material_id, l.material_id,
               l.is_pooled, r.method, r.qc_passed,
               p.cum_dilution_factor, p.freeze_thaw_count,
               mr.allows_pool, mr.max_dilution, mr.max_freeze_thaw,
               tc.volume_closed, tc.sources_complete
        FROM silver.well_root_link l
        JOIN silver.run_fact r USING (run_id)
        LEFT JOIN silver.method_rule mr ON mr.method = r.method
        LEFT JOIN silver.root_path p
          ON p.material_id = l.material_id AND p.root_material_id = l.root_material_id
        LEFT JOIN silver.material mm ON mm.material_id = l.material_id
        LEFT JOIN silver.transform_closure tc
          ON tc.transform_id = mm.producing_transform_id
    """).fetchall()
    cols = [d[0] for d in con.description]
    for row in rows:
        rec = dict(zip(cols, row))
        reason = "eligible"
        if not rec["qc_passed"]:
            reason = "qc_failed"
        elif rec["is_pooled"] and not rec["allows_pool"]:
            reason = "pool_not_allowed"
        elif rec["cum_dilution_factor"] is not None and rec["max_dilution"] is not None \
                and rec["cum_dilution_factor"] > rec["max_dilution"] + 1e-9:
            reason = "dilution_exceeded"
        elif rec["freeze_thaw_count"] is not None and rec["max_freeze_thaw"] is not None \
                and rec["freeze_thaw_count"] > rec["max_freeze_thaw"]:
            reason = "freeze_thaw_exceeded"
        elif rec["volume_closed"] is False or rec["sources_complete"] is False:
            reason = "lineage_unclosed"
        eligible = reason == "eligible"
        coverage_kind = "pooled_qualitative" if (eligible and rec["is_pooled"]) else \
                        ("direct" if eligible else "none")
        con.execute("INSERT INTO silver.well_root_evidence VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            rec["run_id"], rec["plate_id"], rec["well"], rec["root_material_id"],
            rec["material_id"], rec["qc_passed"], rec["is_pooled"],
            rec["cum_dilution_factor"], rec["freeze_thaw_count"],
            eligible, reason, coverage_kind,
        ])
