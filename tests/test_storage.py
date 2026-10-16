import json
import sqlite3
import zlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from lol_collector.columns import TABLES
from lol_collector.static_data import Item
from lol_collector.storage import Store


def match_row(match_id: str, **fields: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "match_id": match_id,
        "platform_id": "EUW1",
        "queue_id": 420,
        "game_start_utc": "2025-10-09T10:00:00Z",
        "game_duration_s": 1800,
        "patch": "16.19",
        "status": "collected",
        "skip_reason": None,
        "collected_at_utc": "2025-10-12T09:00:00Z",
    }
    row.update(fields)
    return row


def participant_rows(match_id: str, count: int = 10) -> list[dict[str, Any]]:
    return [
        {"match_id": match_id, "participant_id": pid, "win": pid <= 5, "kills": pid}
        for pid in range(1, count + 1)
    ]


@pytest.fixture
def store(tmp_path: Path) -> Iterator[Store]:
    with Store(tmp_path / "lol.sqlite3") as opened:
        yield opened


def test_schema_creates_every_table(store: Store) -> None:
    rows = store.connection().execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    assert {name for (name,) in rows} == {
        "matches",
        "participants",
        "teams",
        "players",
        "rank_cache",
        "mastery_cache",
        "raw_matches",
        "champions",
        "items",
    }


@pytest.mark.parametrize(
    ("table", "primary_key"),
    [
        ("matches", ("match_id",)),
        ("participants", ("match_id", "participant_id")),
        ("teams", ("match_id", "team_id")),
    ],
)
def test_match_tables_have_declared_columns_and_primary_key(
    store: Store, table: str, primary_key: tuple[str, ...]
) -> None:
    info = store.connection().execute(f"PRAGMA table_info({table})").fetchall()
    assert [(row[1], row[2]) for row in info] == [(c.name, c.sql_type) for c in TABLES[table]]
    key_columns = sorted((row for row in info if row[5]), key=lambda row: row[5])
    assert tuple(row[1] for row in key_columns) == primary_key


def test_open_adds_participant_column_missing_from_old_database(tmp_path: Path) -> None:
    path = tmp_path / "old.sqlite3"
    kept = [c for c in TABLES["participants"] if c.name != "mastery_last_play_utc"]
    columns = ", ".join(f'"{c.name}" {c.sql_type}' for c in kept)
    old = sqlite3.connect(path)
    old.execute(f"CREATE TABLE participants ({columns}, PRIMARY KEY (match_id, participant_id))")
    old.execute(
        "INSERT INTO participants (match_id, participant_id, kills) VALUES ('EUW1_1', 1, 7)"
    )
    old.commit()
    old.close()

    with Store(path) as store:
        info = store.connection().execute("PRAGMA table_info(participants)").fetchall()
        row = (
            store.connection()
            .execute("SELECT kills, mastery_last_play_utc FROM participants")
            .fetchone()
        )

    assert {r[1] for r in info} == {c.name for c in TABLES["participants"]}
    assert {r[1]: r[2] for r in info}["mastery_last_play_utc"] == "TEXT"
    assert row == (7, None)


def test_add_seeds_requeues_a_crawled_player_at_depth_zero(store: Store) -> None:
    store.add_players(["alice", "bob"], depth=2)
    store.mark_player_done("alice")
    assert store.pending_players() == 1

    store.add_seeds(["alice"])

    assert store.pending_players() == 2
    assert store.next_player() == ("alice", 0)


def test_next_player_takes_lowest_depth_then_insertion_order(store: Store) -> None:
    store.add_players(["deep"], depth=2)
    store.add_players(["mid-b", "mid-a"], depth=1)
    store.add_seeds(["seed"])

    order: list[tuple[str, int]] = []
    while (player := store.next_player()) is not None:
        order.append(player)
        store.mark_player_done(player[0])

    assert order == [("seed", 0), ("mid-b", 1), ("mid-a", 1), ("deep", 2)]


def test_add_players_ignores_players_already_queued(store: Store) -> None:
    store.add_players(["alice"], depth=1)
    store.add_players(["alice", "bob"], depth=3)

    assert store.pending_players() == 2
    assert store.next_player() == ("alice", 1)


def test_save_match_stores_rows_and_compressed_raw_json(store: Store) -> None:
    match_json = {"metadata": {"matchId": "EUW1_1"}, "info": {"gameDuration": 1800}}

    store.save_match(match_row("EUW1_1"), participant_rows("EUW1_1"), raw=(match_json, None))

    assert store.known_match_ids(["EUW1_1"]) == {"EUW1_1"}
    assert store.connection().execute("SELECT COUNT(*) FROM participants").fetchone() == (10,)
    blob, timeline = (
        store.connection()
        .execute("SELECT match_json, timeline_json FROM raw_matches WHERE match_id = 'EUW1_1'")
        .fetchone()
    )
    assert json.loads(zlib.decompress(blob)) == match_json
    assert timeline is None


def test_save_match_is_all_or_nothing(store: Store) -> None:
    # A set cannot be serialised: the failure comes after the matches and participants inserts.
    with pytest.raises(TypeError):
        store.save_match(
            match_row("EUW1_1"), participant_rows("EUW1_1"), raw=({"bad": {1, 2}}, None)
        )

    assert store.known_match_ids(["EUW1_1"]) == set()
    assert store.connection().execute("SELECT COUNT(*) FROM participants").fetchone() == (0,)


def test_save_match_rejects_participant_without_primary_key(store: Store) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        store.save_match(match_row("EUW1_9"), [{"match_id": "EUW1_9", "kills": 3}])
    assert store.known_match_ids(["EUW1_9"]) == set()


def test_known_match_ids_includes_skipped_matches(store: Store) -> None:
    store.save_match(match_row("EUW1_1"))
    store.save_match(match_row("EUW1_2", status="skipped", skip_reason="ARAM"))

    assert store.known_match_ids(["EUW1_1", "EUW1_2", "EUW1_3"]) == {"EUW1_1", "EUW1_2"}
    assert store.known_match_ids([]) == set()


@pytest.mark.parametrize(
    ("age", "max_age", "cached"),
    [
        (timedelta(hours=1), timedelta(days=1), True),
        (timedelta(days=2), timedelta(days=1), False),
        (timedelta(days=2), timedelta(days=3), True),
    ],
)
def test_cached_rank_expires_after_max_age(
    store: Store, age: timedelta, max_age: timedelta, cached: bool
) -> None:
    fetched = (datetime.now(UTC) - age).strftime("%Y-%m-%dT%H:%M:%SZ")
    entries = [{"tier": "GOLD", "leaguePoints": 42}]
    store.save_rank("alice", entries, fetched)

    assert store.cached_rank("alice", max_age) == ((entries, fetched) if cached else None)
    assert store.cached_rank("nobody", max_age) is None


@pytest.mark.parametrize(
    ("age", "max_age", "cached"),
    [
        (timedelta(hours=1), timedelta(hours=24), True),
        (timedelta(hours=25), timedelta(hours=24), False),
        (timedelta(hours=24), timedelta(hours=24), False),
    ],
)
def test_cached_mastery_expires_after_max_age(
    store: Store, age: timedelta, max_age: timedelta, cached: bool
) -> None:
    fetched = (datetime.now(UTC) - age).strftime("%Y-%m-%dT%H:%M:%SZ")
    mastery = {"championId": 103, "championLevel": 7, "championPoints": 123456}
    store.save_mastery("alice", 103, mastery, fetched)

    assert store.cached_mastery("alice", 103, max_age) == ((mastery, fetched) if cached else None)
    assert store.cached_mastery("alice", 999, max_age) is None
    assert store.cached_mastery("nobody", 103, max_age) is None


def test_save_items_stores_completed_flag_as_int_and_replaces_by_id(store: Store) -> None:
    store.save_items(
        [
            Item(1036, "Long Sword", 350, completed=False),
            Item(3031, "Infinity Edge", 3450, completed=True),
        ]
    )
    store.save_items([Item(1036, "Long Sword", 350, completed=True)])

    rows = (
        store.connection()
        .execute("SELECT item_id, name, gold_total, completed FROM items ORDER BY item_id")
        .fetchall()
    )
    assert rows == [(1036, "Long Sword", 350, 1), (3031, "Infinity Edge", 3450, 1)]


def test_iter_rows_restores_booleans_and_keeps_nulls(store: Store) -> None:
    store.save_match(
        match_row("EUW1_1"),
        [
            {"match_id": "EUW1_1", "participant_id": 1, "win": True, "first_blood_kill": False},
            {"match_id": "EUW1_1", "participant_id": 2, "win": None, "first_blood_kill": True},
        ],
    )

    rows = list(store.iter_rows("participants"))

    assert rows[0]["win"] is True
    assert rows[0]["first_blood_kill"] is False
    assert rows[1]["win"] is None
    assert rows[1]["first_blood_kill"] is True


def test_iter_rows_for_matches_includes_skipped_matches(store: Store) -> None:
    store.save_match(match_row("EUW1_2"))
    store.save_match(match_row("EUW1_1"))
    store.save_match(match_row("EUW1_3", status="skipped", skip_reason="ARAM"))

    rows = list(store.iter_rows("matches"))
    assert [row["match_id"] for row in rows] == ["EUW1_1", "EUW1_2", "EUW1_3"]
    assert rows[2]["skip_reason"] == "ARAM"


def test_summary_counts_collected_matches_and_players(store: Store) -> None:
    store.save_match(
        match_row("EUW1_1", patch="16.19", game_start_utc="2025-10-09T10:00:00Z"),
        participant_rows("EUW1_1"),
    )
    store.save_match(
        match_row("EUW1_2", patch="16.19", game_start_utc="2025-10-10T12:00:00Z"),
        participant_rows("EUW1_2"),
    )
    store.save_match(
        match_row("EUW1_3", queue_id=440, patch="16.20", game_start_utc="2025-10-11T08:00:00Z"),
        participant_rows("EUW1_3"),
    )
    store.save_match(
        match_row(
            "EUW1_4",
            status="skipped",
            skip_reason="ARAM",
            queue_id=450,
            game_start_utc="2025-01-01T00:00:00Z",
        )
    )
    store.add_seeds(["alice"])
    store.add_players(["bob", "carol"], depth=1)
    store.mark_player_done("alice")

    assert store.summary() == {
        "matches_collected": 3,
        "matches_skipped": 1,
        "participant_rows": 30,
        "players_crawled": 1,
        "players_pending": 2,
        "first_game_utc": "2025-10-09T10:00:00Z",
        "last_game_utc": "2025-10-11T08:00:00Z",
        "patches": ["16.19", "16.20"],
        "queues": {420: 2, 440: 1},
    }


def test_summary_of_empty_store(store: Store) -> None:
    assert store.summary() == {
        "matches_collected": 0,
        "matches_skipped": 0,
        "participant_rows": 0,
        "players_crawled": 0,
        "players_pending": 0,
        "first_game_utc": None,
        "last_game_utc": None,
        "patches": [],
        "queues": {},
    }
