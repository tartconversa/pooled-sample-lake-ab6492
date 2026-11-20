"""bronze 层：持续接收入湖。

设计要点：
- 接收是追加式、不可变的；原始整份文件与每行记录都长期保存。
- 内容摘要按归一化行尾计算；相同内容重复提交不新增事实（receipt 留痕）。
- 同一业务键的整行修订以新版本共存，业务时点与接收时点分开记录。
- 仪器 JSON 保留整份原件，并按 export/plate/well/measurement 层级展开成原始行，
  重复导出（EX2COPY）原样保留，去重留到 silver 层解释。
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import duckdb

from . import shared

DDL = [
    "CREATE SEQUENCE IF NOT EXISTS bronze.receipt_seq START 1",
    "CREATE SEQUENCE IF NOT EXISTS bronze.raw_row_seq START 1",
    "CREATE SEQUENCE IF NOT EXISTS bronze.inst_seq START 1",
    """
    CREATE TABLE IF NOT EXISTS bronze.ingest_batch (
        batch_id      VARCHAR PRIMARY KEY,
        source_label  VARCHAR NOT NULL,
        received_at   TIMESTAMP NOT NULL,
        note          VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze.file_receipt (
        receipt_id    BIGINT PRIMARY KEY DEFAULT nextval('bronze.receipt_seq'),
        batch_id      VARCHAR NOT NULL,
        file_path     VARCHAR NOT NULL,
        content_hash  VARCHAR NOT NULL,
        received_at   TIMESTAMP NOT NULL,
        status        VARCHAR NOT NULL  -- accepted | duplicate
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze.ingest_file (
        file_id       VARCHAR PRIMARY KEY,  -- 内容哈希前12位
        batch_id      VARCHAR NOT NULL,
        file_path     VARCHAR NOT NULL,
        table_name    VARCHAR,             -- CSV 表名；仪器文件为 NULL
        content_hash  VARCHAR NOT NULL UNIQUE,
        byte_length   BIGINT NOT NULL,
        received_at   TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze.raw_row (
        row_id        BIGINT PRIMARY KEY DEFAULT nextval('bronze.raw_row_seq'),
        file_id       VARCHAR NOT NULL,
        table_name    VARCHAR NOT NULL,
        business_key  VARCHAR NOT NULL,
        row_seq       INTEGER NOT NULL,
        row_json      VARCHAR NOT NULL,
        received_at   TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze.instrument_doc (
        file_id       VARCHAR PRIMARY KEY,
        batch_id      VARCHAR NOT NULL,
        doc_json      VARCHAR NOT NULL,
        received_at   TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze.instrument_raw_measurement (
        row_id        BIGINT PRIMARY KEY DEFAULT nextval('bronze.inst_seq'),
        file_id       VARCHAR NOT NULL,
        export_id     VARCHAR NOT NULL,
        run_id        VARCHAR NOT NULL,
        method_version VARCHAR NOT NULL,
        plate_id      VARCHAR NOT NULL,
        well          VARCHAR NOT NULL,
        channel       VARCHAR NOT NULL,
        unit          VARCHAR NOT NULL,
        replicate     INTEGER NOT NULL,
        value         DOUBLE NOT NULL,
        received_at   TIMESTAMP NOT NULL
    )
    """,
]


def ensure_ddl(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in DDL:
        con.execute(stmt)


def _next_batch_id(con: duckdb.DuckDBPyConnection) -> str:
    n = con.execute("SELECT count(*) FROM bronze.ingest_batch").fetchone()[0]
    return f"B{n + 1:03d}"


def _ingest_csv(con, table: str, path: Path, batch_id: str, received_at: dt.datetime) -> str:
    text = path.read_text(encoding="utf-8")
    digest = shared.content_hash(text)
    existing = con.execute(
        "SELECT file_id FROM bronze.ingest_file WHERE content_hash = ?", [digest]
    ).fetchone()
    if existing:
        con.execute(
            "INSERT INTO bronze.file_receipt (batch_id, file_path, content_hash, received_at, status) "
            "VALUES (?, ?, ?, ?, 'duplicate')",
            [batch_id, str(path), digest, received_at],
        )
        return "duplicate"
    file_id = digest[:12]
    con.execute(
        "INSERT INTO bronze.ingest_file VALUES (?, ?, ?, ?, ?, ?, ?)",
        [file_id, batch_id, str(path), table, digest, len(text.encode("utf-8")), received_at],
    )
    rows = shared.read_csv_rows(path)
    for seq, raw_row in enumerate(rows, 1):
        typed = shared.coerce(table, raw_row)
        key = shared.table_key(table, typed)
        con.execute(
            "INSERT INTO bronze.raw_row (file_id, table_name, business_key, row_seq, row_json, received_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [file_id, table, key, seq, json.dumps(typed, ensure_ascii=False, sort_keys=True), received_at],
        )
    con.execute(
        "INSERT INTO bronze.file_receipt (batch_id, file_path, content_hash, received_at, status) "
        "VALUES (?, ?, ?, ?, 'accepted')",
        [batch_id, str(path), digest, received_at],
    )
    return "accepted"


def _ingest_instrument(con, path: Path, batch_id: str, received_at: dt.datetime) -> str:
    text = path.read_text(encoding="utf-8")
    digest = shared.content_hash(text)
    existing = con.execute(
        "SELECT file_id FROM bronze.ingest_file WHERE content_hash = ?", [digest]
    ).fetchone()
    if existing:
        con.execute(
            "INSERT INTO bronze.file_receipt (batch_id, file_path, content_hash, received_at, status) "
            "VALUES (?, ?, ?, ?, 'duplicate')",
            [batch_id, str(path), digest, received_at],
        )
        return "duplicate"
    file_id = digest[:12]
    doc = json.loads(text)
    con.execute(
        "INSERT INTO bronze.ingest_file VALUES (?, ?, ?, NULL, ?, ?, ?)",
        [file_id, batch_id, str(path), digest, len(text.encode("utf-8")), received_at],
    )
    con.execute(
        "INSERT INTO bronze.instrument_doc VALUES (?, ?, ?, ?)",
        [file_id, batch_id, text, received_at],
    )
    for export in doc["exports"]:
        for plate in export["plates"]:
            for well in plate["wells"]:
                for m in well["measurements"]:
                    con.execute(
                        "INSERT INTO bronze.instrument_raw_measurement "
                        "(file_id, export_id, run_id, method_version, plate_id, well, channel, unit, replicate, value, received_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [file_id, export["export_id"], export["run_id"], export["method_version"],
                         plate["plate_id"], well["well"], m["channel"], m["unit"],
                         int(m["replicate"]), float(m["value"]), received_at],
                    )
    con.execute(
        "INSERT INTO bronze.file_receipt (batch_id, file_path, content_hash, received_at, status) "
        "VALUES (?, ?, ?, ?, 'accepted')",
        [batch_id, str(path), digest, received_at],
    )
    return "accepted"


def ingest_batch(con: duckdb.DuckDBPyConnection, sources: dict[str, Path],
                 instrument: Path | None, received_at: dt.datetime,
                 source_label: str, note: str | None = None) -> dict:
    """接收一个资料批次。

    sources 为 {表名: CSV 路径}；instrument 为仪器 JSON 路径，可空。
    相同内容重复提交只记 receipt，不产生新事实。返回接收结果摘要。
    """
    ensure_ddl(con)
    batch_id = _next_batch_id(con)
    con.execute("INSERT INTO bronze.ingest_batch VALUES (?, ?, ?, ?)",
                [batch_id, source_label, received_at, note])
    detail: dict[str, str] = {}
    for table, path in sources.items():
        if table not in shared.CSV_TABLES:
            raise ValueError(f"未知资料表：{table}")
        detail[table] = _ingest_csv(con, table, Path(path), batch_id, received_at)
    if instrument is not None:
        detail["instrument"] = _ingest_instrument(con, Path(instrument), batch_id, received_at)
    return {"batch_id": batch_id, "received_at": received_at.isoformat(), "files": detail}


def latest_rows(con: duckdb.DuckDBPyConnection, as_of: dt.datetime | None = None) -> dict[str, list[dict]]:
    """取每张资料表在给定接收时点（默认最新）的最新版本行。"""
    cutoff = "AND received_at <= ?" if as_of else ""
    params: list[object] = [as_of] if as_of else []
    result: dict[str, list[dict]] = {}
    for table in shared.CSV_TABLES:
        rows = con.execute(
            f"""
            SELECT row_json FROM (
                SELECT row_json, row_number() OVER (
                    PARTITION BY business_key ORDER BY received_at DESC, row_id DESC
                ) AS rn
                FROM bronze.raw_row
                WHERE table_name = ? {cutoff}
            ) WHERE rn = 1
            """,
            [table] + params,
        ).fetchall()
        result[table] = [json.loads(r[0]) for r in rows]
    return result
