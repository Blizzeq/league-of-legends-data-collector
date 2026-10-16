import csv
import sys
from pathlib import Path

import pandas as pd
import pytest

from lol_collector.columns import TABLES
from lol_collector.export import ExportError, export_tables, read_frame
from lol_collector.storage import Store


def _read_csv(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


async def test_csv_export_writes_headers_and_one_line_per_record(
    collected_db: Path, tmp_path: Path
) -> None:
    with Store(collected_db) as store:
        collected = store.summary()["matches_collected"]
        participants_path, teams_path = export_tables(
            store, tmp_path / "exports", "csv", ["participants", "teams"]
        )

    assert collected > 0
    assert participants_path == tmp_path / "exports" / "participants.csv"
    participants = _read_csv(participants_path)
    assert participants[0] == [c.name for c in TABLES["participants"]]
    assert len(participants) - 1 == 10 * collected

    teams = _read_csv(teams_path)
    assert teams[0] == [c.name for c in TABLES["teams"]]
    assert len(teams) - 1 == 2 * collected


@pytest.mark.parametrize(
    ("column", "dtype"),
    [("kills", "Int64"), ("kda", "Float64"), ("win", "boolean"), ("champion_name", "string")],
)
async def test_read_frame_uses_nullable_dtypes(collected_db: Path, column: str, dtype: str) -> None:
    with Store(collected_db) as store:
        frame = read_frame(store, "participants")

    assert frame[column].dtype == dtype


async def test_read_frame_parses_timestamps_as_utc_and_booleans_as_bool(
    collected_db: Path,
) -> None:
    with Store(collected_db) as store:
        frame = read_frame(store, "participants")

    start = frame["game_start_utc"].dtype
    assert isinstance(start, pd.DatetimeTZDtype)
    assert str(start.tz) == "UTC"
    assert frame["win"].isna().sum() == 0
    # Every match has five winners out of ten participants.
    assert int(frame["win"].sum()) == len(frame) // 2


async def test_unknown_format_is_rejected_without_writing_files(
    collected_db: Path, tmp_path: Path
) -> None:
    out_dir = tmp_path / "exports"
    with Store(collected_db) as store, pytest.raises(ExportError, match="Unknown format 'xlsx'"):
        export_tables(store, out_dir, "xlsx", ["matches"])

    assert list(out_dir.iterdir()) == []


async def test_parquet_without_pyarrow_raises_export_error(
    collected_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(sys.modules, "pyarrow", None)

    with Store(collected_db) as store, pytest.raises(ExportError, match="pip install"):
        export_tables(store, tmp_path, "parquet", ["matches"])


async def test_parquet_export_round_trips_row_counts(collected_db: Path, tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    with Store(collected_db) as store:
        collected = store.summary()["matches_collected"]
        participants_path, teams_path = export_tables(
            store, tmp_path, "parquet", ["participants", "teams"]
        )

    assert collected > 0
    assert len(pd.read_parquet(participants_path)) == 10 * collected
    assert len(pd.read_parquet(teams_path)) == 2 * collected
