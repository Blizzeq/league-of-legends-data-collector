"""Export collected tables to CSV or Parquet, and load them as typed DataFrames."""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from .columns import TABLES
from .storage import Store

log = logging.getLogger(__name__)

PANDAS_DTYPES: dict[str, Any] = {
    "int": "Int64",
    "float": "Float64",
    "pct": "Float64",
    "bool": "boolean",
    "str": "string",
}


class ExportError(RuntimeError):
    """The requested export cannot be produced (e.g. a missing optional dependency)."""


def read_frame(store: Store, table: str) -> pd.DataFrame:
    """Load a table with proper nullable dtypes and UTC timestamps."""
    frame = pd.read_sql_query(store.select_sql(table), store.connection())
    for column in TABLES[table]:
        if column.kind == "datetime":
            frame[column.name] = pd.to_datetime(frame[column.name], utc=True)
        else:
            frame[column.name] = frame[column.name].astype(PANDAS_DTYPES[column.kind])
    return frame


def export_csv(store: Store, table: str, path: Path) -> int:
    names = [c.name for c in TABLES[table]]
    rows = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        for row in store.iter_rows(table):
            writer.writerow(row)
            rows += 1
    return rows


def export_parquet(store: Store, table: str, path: Path) -> int:
    try:
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise ExportError(
            "Parquet export needs pyarrow: pip install 'lol-data-collector[parquet]'"
        ) from exc
    frame = read_frame(store, table)
    frame.to_parquet(path, index=False)
    return len(frame)


def export_tables(store: Store, out_dir: Path, fmt: str, tables: Iterable[str]) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    writers = {"csv": export_csv, "parquet": export_parquet}
    if fmt not in writers:
        raise ExportError(f"Unknown format '{fmt}'. Use csv or parquet.")
    paths = []
    for table in tables:
        path = out_dir / f"{table}.{fmt}"
        rows = writers[fmt](store, table, path)
        log.info("Wrote %s rows to %s", f"{rows:,}", path)
        paths.append(path)
    return paths
