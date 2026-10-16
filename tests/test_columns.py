from pathlib import Path
from typing import get_args

import pytest

from lol_collector.columns import SQL_TYPES, TABLES, Column, Kind, data_dictionary_markdown
from lol_collector.storage import Store

DOC_PATH = Path(__file__).resolve().parents[1] / "docs" / "data-dictionary.md"


def test_data_dictionary_doc_matches_the_generator() -> None:
    assert DOC_PATH.read_text(encoding="utf-8") == data_dictionary_markdown()


def test_data_dictionary_documents_every_table() -> None:
    markdown = data_dictionary_markdown()

    for table in TABLES:
        assert f"## {table}\n" in markdown


@pytest.mark.parametrize("table", sorted(TABLES))
def test_column_names_are_unique_within_a_table(table: str) -> None:
    names = [column.name for column in TABLES[table]]

    assert {name for name in names if names.count(name) > 1} == set()


def test_sql_types_cover_every_column_kind() -> None:
    assert set(SQL_TYPES) == set(get_args(Kind))


@pytest.mark.parametrize(
    ("kind", "sql_type"),
    [
        ("int", "INTEGER"),
        ("bool", "INTEGER"),
        ("float", "REAL"),
        ("pct", "REAL"),
        ("str", "TEXT"),
        ("datetime", "TEXT"),
    ],
)
def test_kind_maps_to_sql_type(kind: Kind, sql_type: str) -> None:
    assert Column("example", kind, "example").sql_type == sql_type


@pytest.mark.parametrize("table", sorted(TABLES))
def test_sqlite_schema_declares_each_column_with_its_sql_type(table: str, tmp_path: Path) -> None:
    with Store(tmp_path / "lol.sqlite3") as store:
        declared = {
            row[1]: row[2] for row in store.connection().execute(f"PRAGMA table_info({table})")
        }

    assert declared == {column.name: column.sql_type for column in TABLES[table]}
