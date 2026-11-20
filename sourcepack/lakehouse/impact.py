"""补交资料影响评估。

后续补交转化或方法资料时，需要回答：哪些原样受到影响、覆盖与工作量怎么变。
做法：每次重算后对 gold 汇总打快照（按批次），补交前后对比；
再结合血缘边和证据资格，给出受影响根原样及变化原因，供用户核实。
"""
from __future__ import annotations

import datetime as dt

import duckdb

from . import shared

SNAPSHOT_TABLES = ["sample_coverage", "sample_retained", "lab_well_workload", "lab_run_workload"]


def ensure_snapshot_ddl(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.coverage_snapshot (
            batch_id VARCHAR, root_material_id VARCHAR, project VARCHAR,
            attempt_wells INTEGER, attempt_runs INTEGER,
            eligible_wells INTEGER, eligible_runs INTEGER,
            has_evidence BOOLEAN, evidence_kinds VARCHAR, latest_reason VARCHAR,
            PRIMARY KEY (batch_id, root_material_id)
        )""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.impact_report (
            after_batch VARCHAR, root_material_id VARCHAR,
            before_has_evidence BOOLEAN, after_has_evidence BOOLEAN,
            before_eligible_wells INTEGER, after_eligible_wells INTEGER,
            before_retained_root_equiv_ul DOUBLE, after_retained_root_equiv_ul DOUBLE,
            change_kind VARCHAR, detail VARCHAR
        )""")


def snapshot(con: duckdb.DuckDBPyConnection, batch_id: str) -> None:
    ensure_snapshot_ddl(con)
    con.execute("DELETE FROM gold.coverage_snapshot WHERE batch_id = ?", [batch_id])
    con.execute("""
        INSERT INTO gold.coverage_snapshot
        SELECT ?, root_material_id, project, attempt_wells, attempt_runs,
               eligible_wells, eligible_runs, has_evidence, evidence_kinds, latest_reason
        FROM gold.sample_coverage
    """, [batch_id])


def _latest_prior_batch(con: duckdb.DuckDBPyConnection, current: str) -> str | None:
    row = con.execute(
        "SELECT max(batch_id) FROM gold.coverage_snapshot WHERE batch_id < ?", [current]
    ).fetchone()
    return row[0] if row and row[0] else None


def assess_impact(con: duckdb.DuckDBPyConnection, after_batch: str) -> dict:
    """对比 after_batch 与上一快照，输出受影响根原样清单。"""
    ensure_snapshot_ddl(con)
    prior = _latest_prior_batch(con, after_batch)
    con.execute("DELETE FROM gold.impact_report WHERE after_batch = ?", [after_batch])
    if prior is None:
        return {"after_batch": after_batch, "prior_batch": None, "affected": []}

    # 受影响 = 覆盖状态/有效孔/留样口径任一变化，或其血缘相关材料在新批次出现
    new_lineage_roots = _roots_touched_by_batch(con, after_batch)
    rows = con.execute("""
        SELECT a.root_material_id,
               b.has_evidence AS before_has, a.has_evidence AS after_has,
               b.eligible_wells AS before_ew, a.eligible_wells AS after_ew,
               r_before.retained_root_equiv_ul AS before_ret,
               r_after.retained_root_equiv_ul AS after_ret
        FROM gold.coverage_snapshot a
        JOIN gold.coverage_snapshot b
          ON b.batch_id = ? AND b.root_material_id = a.root_material_id
        LEFT JOIN gold.sample_retained r_after ON r_after.root_material_id = a.root_material_id
        LEFT JOIN (
            SELECT root_material_id, retained_root_equiv_ul
            FROM gold.sample_retained
        ) r_before ON r_before.root_material_id = a.root_material_id
        WHERE a.batch_id = ?
    """, [prior, after_batch]).fetchall()

    affected: list[dict] = []
    for root, before_has, after_has, before_ew, after_ew, before_ret, after_ret in rows:
        changes = []
        if before_has != after_has:
            changes.append("evidence_gained" if after_has else "evidence_lost")
        if (before_ew or 0) != (after_ew or 0):
            changes.append(f"eligible_wells {before_ew}->{after_ew}")
        if _num(before_ret) != _num(after_ret):
            changes.append(f"retained_equiv {_num(before_ret)}->{_num(after_ret)}")
        touched = root in new_lineage_roots
        if not changes and not touched:
            continue
        kind = changes[0] if changes else "lineage_updated"
        detail = ";".join(changes) if changes else "补交资料改变了该原样的血缘/台账"
        con.execute("INSERT INTO gold.impact_report VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            after_batch, root, before_has, after_has, before_ew, after_ew,
            before_ret, after_ret, kind, detail,
        ])
        affected.append({"root_material_id": root, "change_kind": kind, "detail": detail})
    return {"after_batch": after_batch, "prior_batch": prior,
            "touched_roots": sorted(new_lineage_roots), "affected": affected}


def _roots_touched_by_batch(con: duckdb.DuckDBPyConnection, batch_id: str) -> set[str]:
    """该批次接收的转化/材料/方法文件涉及到哪些根原样。"""
    rows = con.execute("""
        SELECT DISTINCT r.row_json
        FROM bronze.raw_row r
        JOIN bronze.ingest_file f USING (file_id)
        WHERE f.batch_id = ?
          AND r.table_name IN ('transforms', 'transform_inputs', 'transform_outputs',
                               'materials', 'method_rules', 'well_inputs', 'runs')
    """, [batch_id]).fetchall()
    import json
    ids: set[str] = set()
    for (payload,) in rows:
        obj = json.loads(payload)
        for key in ("material_id", "root_material_id"):
            if obj.get(key):
                ids.add(obj[key])
    # 沿血缘把涉及材料映射回根原样（只保留真正的根原样）
    roots = set()
    for mid in ids:
        mapped = con.execute(
            "SELECT DISTINCT root_material_id FROM silver.material_origin WHERE material_id = ?",
            [mid],
        ).fetchall()
        is_root = con.execute(
            "SELECT is_root_sample FROM silver.material WHERE material_id = ?", [mid]
        ).fetchone()
        if is_root and is_root[0]:
            roots.add(mid)
        roots.update(r[0] for r in mapped)
    return roots


def _num(value):
    return None if value is None else round(float(value), 6)
