"""编排：把一批资料从接收、事实整理到仓表构建串起来。"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb

from . import experiment, ingest, lineage, marts, shared


def rebuild_silver_gold(con: duckdb.DuckDBPyConnection,
                        channel_mapping: list[dict] | None = None,
                        as_of: dt.datetime | None = None) -> dict:
    """基于 bronze 事实重算 silver 与 gold（幂等、可重放）。

    as_of 给定时只使用该接收时点之前（含）的资料版本，用于历史口径回放；
    注意仪器原始测量为不可变事实，不随 as_of 裁剪。
    """
    lineage_result = lineage.build_lineage(con, as_of)
    experiment_result = experiment.build_experiment(con, channel_mapping, as_of)
    mart_result = marts.build_marts(con)
    return {"lineage": lineage_result, "experiment": experiment_result, "marts": mart_result}


def load_pack(con: duckdb.DuckDBPyConnection, root: str | Path,
              received_at: dt.datetime | None = None,
              source_label: str = "basepack", note: str | None = None) -> dict:
    """接收一个标准资料包（data/*.csv + raw/instrument.json）并重算仓表。"""
    root = Path(root)
    stamp = received_at or dt.datetime.now(shared.DEFAULT_TZ)
    sources = {table: root / "data" / f"{table}.csv" for table in shared.CSV_TABLES}
    sources = {t: p for t, p in sources.items() if p.exists()}
    instrument = root / shared.INSTRUMENT_FILE
    batch = ingest.ingest_batch(
        con, sources, instrument if instrument.exists() else None,
        stamp, source_label, note,
    )
    rebuild = rebuild_silver_gold(con)
    return {"ingest": batch, **rebuild}


def query(con: duckdb.DuckDBPyConnection, sql: str):
    """只读查询入口，返回（列名, 行）。"""
    cur = con.execute(sql)
    return [d[0] for d in cur.description], cur.fetchall()
