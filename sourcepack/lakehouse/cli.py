"""数据湖命令行。

示例：
  python3 -m sourcepack.lakehouse.cli init --db lake/lake.duckdb --pack .
  python3 -m sourcepack.lakehouse.cli ingest --db lake/lake.duckdb --pack <补交目录> --label L2
  python3 -m sourcepack.lakehouse.cli show   --db lake/lake.duckdb --table gold.sample_coverage
  python3 -m sourcepack.lakehouse.cli drill  --db lake/lake.duckdb --root S1
  python3 -m sourcepack.lakehouse.cli impact --db lake/lake.duckdb
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

from . import impact, ingest, lake, shared
from .shared import connect


def _ts(text: str | None) -> dt.datetime:
    return shared.parse_ts(text) if text else dt.datetime.now(shared.DEFAULT_TZ)


def _print(columns, rows) -> None:
    widths = [len(c) for c in columns]
    text_rows = [[("" if v is None else str(v)) for v in row] for row in rows]
    for row in text_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    header = " | ".join(c.ljust(widths[i]) for i, c in enumerate(columns))
    print(header)
    print("-+-".join("-" * w for w in widths))
    for row in text_rows:
        print(" | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def cmd_init(args) -> None:
    con = connect(args.db)
    result = lake.load_pack(con, args.pack, _ts(args.at), args.label, args.note)
    batch_id = result["ingest"]["batch_id"]
    impact.snapshot(con, batch_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def cmd_ingest(args) -> None:
    con = connect(args.db)
    pack = Path(args.pack)
    sources = {t: pack / "data" / f"{t}.csv" for t in shared.CSV_TABLES}
    sources = {t: p for t, p in sources.items() if p.exists()}
    inst = pack / shared.INSTRUMENT_FILE
    batch = ingest.ingest_batch(
        con, sources, inst if inst.exists() else None,
        _ts(args.at), args.label, args.note,
    )
    lake.rebuild_silver_gold(con)
    impact.snapshot(con, batch["batch_id"])
    report = impact.assess_impact(con, batch["batch_id"])
    print(json.dumps({"ingest": batch, "impact": report}, ensure_ascii=False, indent=2))


def cmd_show(args) -> None:
    con = connect(args.db)
    cols, rows = lake.query(con, f"SELECT * FROM {args.table}")
    _print(cols, rows)


def cmd_drill(args) -> None:
    con = connect(args.db)
    print(f"== 原样 {args.root} 覆盖 ==")
    cols, rows = lake.query(
        con, f"SELECT * FROM gold.sample_coverage WHERE root_material_id = '{args.root}'")
    _print(cols, rows)
    print("\n== 留样 ==")
    cols, rows = lake.query(
        con, f"SELECT * FROM gold.sample_retained WHERE root_material_id = '{args.root}'")
    _print(cols, rows)
    print("\n== 材料血缘（含转化闭合） ==")
    cols, rows = lake.query(con, f"""
        SELECT * FROM gold.material_transform_lineage
        WHERE parent_material_id IN (
            SELECT material_id FROM silver.material_origin WHERE root_material_id = '{args.root}')
           OR child_material_id IN (
            SELECT material_id FROM silver.material_origin WHERE root_material_id = '{args.root}')
        ORDER BY occurred_at
    """)
    _print(cols, rows)
    print("\n== 证据资格逐孔 ==")
    cols, rows = lake.query(
        con, f"SELECT * FROM silver.well_root_evidence WHERE root_material_id = '{args.root}'")
    _print(cols, rows)
    print("\n== 原文件测量下钻 ==")
    cols, rows = lake.query(con, f"""
        SELECT * FROM gold.drilldown_measurement WHERE well IN (
            SELECT well FROM silver.well_root_evidence WHERE root_material_id = '{args.root}')
    """)
    _print(cols, rows)


def cmd_impact(args) -> None:
    con = connect(args.db)
    cols, rows = lake.query(con, "SELECT * FROM gold.impact_report ORDER BY after_batch, root_material_id")
    _print(cols, rows)


def cmd_replay(args) -> None:
    """按历史接收时点重算到一组临时表并查询，不影响当前仓表。"""
    con = connect(args.db)
    stamp = _ts(args.at)
    # 备份当前 silver 表名，回放写入带后缀的表，避免覆盖
    lake.rebuild_silver_gold(con, as_of=stamp)
    print(f"已按 {stamp.isoformat()} 时点重算 silver/gold（当前库内即该时点口径）")
    cols, rows = lake.query(con, "SELECT * FROM gold.sample_coverage ORDER BY root_material_id")
    _print(cols, rows)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="研发样本实验数据湖")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="建库并接收首个资料包")
    p.add_argument("--db", required=True)
    p.add_argument("--pack", required=True)
    p.add_argument("--label", default="basepack")
    p.add_argument("--at")
    p.add_argument("--note")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("ingest", help="补交一个资料批次并重算")
    p.add_argument("--db", required=True)
    p.add_argument("--pack", required=True)
    p.add_argument("--label", required=True)
    p.add_argument("--at")
    p.add_argument("--note")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("show", help="查看任一仓表")
    p.add_argument("--db", required=True)
    p.add_argument("--table", required=True)
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("drill", help="按原样下钻覆盖/留样/血缘/测量")
    p.add_argument("--db", required=True)
    p.add_argument("--root", required=True)
    p.set_defaults(func=cmd_drill)

    p = sub.add_parser("impact", help="查看补交影响报告")
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_impact)

    p = sub.add_parser("replay", help="按历史接收时点重算口径")
    p.add_argument("--db", required=True)
    p.add_argument("--at", required=True)
    p.set_defaults(func=cmd_replay)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
