"""共享工具：数据库连接、模式初始化、资料清单与解析约定。"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# 资料契约：与 docs/资料口径.md 和 data/catalog.json 对齐
# ---------------------------------------------------------------------------
CSV_TABLES = {
    "materials": {
        "key": ["material_id"],
        "columns": ["material_id", "kind", "project", "volume_ul"],
        "bools": set(),
    },
    "transforms": {
        "key": ["transform_id"],
        "columns": ["transform_id", "kind", "at"],
        "bools": set(),
    },
    "transform_inputs": {
        "key": ["input_id"],
        "columns": ["input_id", "transform_id", "material_id", "volume_ul"],
        "bools": set(),
    },
    "transform_outputs": {
        "key": ["material_id"],
        "columns": ["material_id", "transform_id", "volume_ul"],
        "bools": set(),
    },
    "runs": {
        "key": ["run_id"],
        "columns": ["run_id", "method", "method_version", "started", "qc", "rerun_of"],
        "bools": set(),
    },
    "well_inputs": {
        "key": ["input_id"],
        "columns": ["input_id", "run_id", "plate_id", "well", "material_id", "volume_ul"],
        "bools": set(),
    },
    "material_events": {
        "key": ["event_id"],
        "columns": ["event_id", "material_id", "kind", "at", "volume_ul"],
        "bools": set(),
    },
    "method_rules": {
        "key": ["method"],
        "columns": ["method", "allows_pool", "max_dilution", "max_freeze_thaw"],
        "bools": {"allows_pool"},
    },
}

# 嵌套原始仪器文件（非 CSV 资料）
INSTRUMENT_FILE = "raw/instrument.json"

DEFAULT_TZ = dt.timezone(dt.timedelta(hours=8))


def connect(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """打开（必要时创建）湖数据库并建立三层模式。"""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    for schema in ("bronze", "silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return con


def parse_ts(value: str) -> dt.datetime:
    """解析带时区的 ISO 时刻；无时区时按 +08:00 解释。"""
    stamp = dt.datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=DEFAULT_TZ)
    return stamp


def content_hash(text: str) -> str:
    """按归一化行尾（LF）计算内容摘要，使换行差异不影响接收幂等。"""
    return hashlib.sha256(text.replace("\r\n", "\n").encode("utf-8")).hexdigest()


def file_content_hash(path: Path) -> str:
    return content_hash(path.read_text(encoding="utf-8"))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """读取 CSV 为字符串行字典；空串保留为空串（表示未知，不是零）。"""
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def coerce(table: str, row: dict[str, str]) -> dict[str, object]:
    """按契约把字符串行转成可入库的类型；空串转 None。"""
    spec = CSV_TABLES[table]
    out: dict[str, object] = {}
    for col in spec["columns"]:
        raw = row.get(col, "")
        if raw == "":
            out[col] = None
            continue
        if col in spec["bools"]:
            out[col] = raw == "true"
        elif col in {"volume_ul", "max_dilution"}:
            out[col] = float(raw)
        elif col == "max_freeze_thaw":
            out[col] = int(raw)
        else:
            out[col] = raw
    return out


def table_key(table: str, row: dict[str, object]) -> str:
    return "|".join(str(row[k]) for k in CSV_TABLES[table]["key"])


def read_instrument(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def sql_value(value: object) -> str:
    """把 Python 值渲染为 SQL 字面量（本湖输入为受控合成资料）。"""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"
