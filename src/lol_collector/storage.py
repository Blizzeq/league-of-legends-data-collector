"""SQLite storage: collected rows, the crawl frontier and caches.

Everything lives in one file so a collection can be stopped and resumed at any
time. Each match is written in a single transaction, so an interrupted run never
leaves half a match behind.
"""

from __future__ import annotations

import json
import sqlite3
import zlib
from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any

from .columns import TABLES, Column
from .static_data import Item

PRIMARY_KEYS = {
    "matches": ("match_id",),
    "participants": ("match_id", "participant_id"),
    "teams": ("match_id", "team_id"),
}

AUX_SCHEMA = """
CREATE TABLE IF NOT EXISTS players (
    puuid TEXT PRIMARY KEY,
    depth INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    added_at_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS players_pending ON players (status, depth);
CREATE TABLE IF NOT EXISTS rank_cache (
    puuid TEXT PRIMARY KEY,
    entries_json TEXT NOT NULL,
    fetched_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_matches (
    match_id TEXT PRIMARY KEY,
    match_json BLOB NOT NULL,
    timeline_json BLOB
);
CREATE TABLE IF NOT EXISTS items (
    item_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    gold_total INTEGER,
    completed INTEGER NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _create_table_sql(name: str, columns: Sequence[Column]) -> str:
    cols = ", ".join(f'"{c.name}" {c.sql_type}' for c in columns)
    pk = ", ".join(PRIMARY_KEYS[name])
    return f"CREATE TABLE IF NOT EXISTS {name} ({cols}, PRIMARY KEY ({pk}))"


class Store:
    """Thin wrapper over a SQLite database file."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    def __enter__(self) -> Store:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._db.close()

    def _create_schema(self) -> None:
        with self._db:
            for name, columns in TABLES.items():
                self._db.execute(_create_table_sql(name, columns))
                # Columns added in newer versions of the collector.
                existing = {row[1] for row in self._db.execute(f"PRAGMA table_info({name})")}
                for column in columns:
                    if column.name not in existing:
                        self._db.execute(
                            f'ALTER TABLE {name} ADD COLUMN "{column.name}" {column.sql_type}'
                        )
            self._db.executescript(AUX_SCHEMA)

    # ------------------------------------------------------------- frontier

    def add_seeds(self, puuids: Iterable[str]) -> None:
        """Queue seed players at depth 0, re-queueing them if they were crawled before."""
        now = utc_now()
        with self._db:
            self._db.executemany(
                "INSERT INTO players (puuid, depth, status, added_at_utc)"
                " VALUES (?, 0, 'pending', ?)"
                " ON CONFLICT (puuid) DO UPDATE SET depth = 0, status = 'pending'",
                [(p, now) for p in puuids],
            )

    def add_players(self, puuids: Iterable[str], depth: int) -> None:
        now = utc_now()
        with self._db:
            self._db.executemany(
                "INSERT OR IGNORE INTO players (puuid, depth, added_at_utc) VALUES (?, ?, ?)",
                [(p, depth, now) for p in puuids],
            )

    def next_player(self) -> tuple[str, int] | None:
        row = self._db.execute(
            "SELECT puuid, depth FROM players WHERE status = 'pending' "
            "ORDER BY depth, rowid LIMIT 1"
        ).fetchone()
        return (row[0], row[1]) if row else None

    def mark_player_done(self, puuid: str) -> None:
        with self._db:
            self._db.execute("UPDATE players SET status = 'done' WHERE puuid = ?", (puuid,))

    def pending_players(self) -> int:
        return int(
            self._db.execute("SELECT COUNT(*) FROM players WHERE status = 'pending'").fetchone()[0]
        )

    # -------------------------------------------------------------- matches

    def known_match_ids(self, match_ids: Iterable[str]) -> set[str]:
        ids = list(match_ids)
        if not ids:
            return set()
        placeholders = ",".join("?" * len(ids))
        rows = self._db.execute(
            f"SELECT match_id FROM matches WHERE match_id IN ({placeholders})", ids
        )
        return {r[0] for r in rows}

    def _insert(self, table: str, rows: Iterable[dict[str, Any]]) -> None:
        names = [c.name for c in TABLES[table]]
        quoted = ", ".join(f'"{n}"' for n in names)
        sql = f"INSERT OR REPLACE INTO {table} ({quoted}) VALUES ({', '.join('?' * len(names))})"
        self._db.executemany(sql, [tuple(row.get(n) for n in names) for row in rows])

    def save_match(
        self,
        match_row: dict[str, Any],
        participants: Sequence[dict[str, Any]] = (),
        teams: Sequence[dict[str, Any]] = (),
        raw: tuple[dict[str, Any], dict[str, Any] | None] | None = None,
    ) -> None:
        """Store a processed (or skipped) match atomically."""
        with self._db:
            self._insert("matches", [match_row])
            self._insert("participants", participants)
            self._insert("teams", teams)
            if raw is not None:
                match_json, timeline_json = raw
                self._db.execute(
                    "INSERT OR REPLACE INTO raw_matches VALUES (?, ?, ?)",
                    (
                        match_row["match_id"],
                        zlib.compress(json.dumps(match_json).encode()),
                        (
                            zlib.compress(json.dumps(timeline_json).encode())
                            if timeline_json is not None
                            else None
                        ),
                    ),
                )

    # ---------------------------------------------------------------- caches

    def cached_rank(self, puuid: str, max_age: timedelta) -> tuple[list[Any], str] | None:
        row = self._db.execute(
            "SELECT entries_json, fetched_at_utc FROM rank_cache WHERE puuid = ?", (puuid,)
        ).fetchone()
        if row is None:
            return None
        fetched = datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        if datetime.now(UTC) - fetched > max_age:
            return None
        return json.loads(row[0]), row[1]

    def save_rank(self, puuid: str, entries: list[Any], fetched_at: str) -> None:
        with self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO rank_cache VALUES (?, ?, ?)",
                (puuid, json.dumps(entries), fetched_at),
            )

    def save_items(self, items: Iterable[Item]) -> None:
        with self._db:
            self._db.executemany(
                "INSERT OR REPLACE INTO items VALUES (?, ?, ?, ?)",
                [(i.item_id, i.name, i.gold_total, int(i.completed)) for i in items],
            )

    # ---------------------------------------------------------------- reading

    def iter_rows(self, table: str) -> Iterator[dict[str, Any]]:
        columns = TABLES[table]
        names = ", ".join(f'"{c.name}"' for c in columns)
        order = ", ".join(PRIMARY_KEYS[table])
        where = " WHERE status = 'collected'" if table == "matches" else ""
        cursor = self._db.execute(f"SELECT {names} FROM {table}{where} ORDER BY {order}")
        for values in cursor:
            yield {
                c.name: bool(v) if c.kind == "bool" and v is not None else v
                for c, v in zip(columns, values, strict=True)
            }

    def connection(self) -> sqlite3.Connection:
        return self._db

    def summary(self) -> dict[str, Any]:
        q = self._db.execute
        status_counts = dict(q("SELECT status, COUNT(*) FROM matches GROUP BY status").fetchall())
        player_counts = dict(q("SELECT status, COUNT(*) FROM players GROUP BY status").fetchall())
        first, last = q(
            "SELECT MIN(game_start_utc), MAX(game_start_utc) FROM matches"
            " WHERE status = 'collected'"
        ).fetchone()
        return {
            "matches_collected": status_counts.get("collected", 0),
            "matches_skipped": status_counts.get("skipped", 0),
            "participant_rows": q("SELECT COUNT(*) FROM participants").fetchone()[0],
            "players_crawled": player_counts.get("done", 0),
            "players_pending": player_counts.get("pending", 0),
            "first_game_utc": first,
            "last_game_utc": last,
            "patches": [
                r[0]
                for r in q(
                    "SELECT patch, COUNT(*) FROM matches WHERE status = 'collected' "
                    "GROUP BY patch ORDER BY COUNT(*) DESC"
                )
            ],
            "queues": dict(
                q(
                    "SELECT queue_id, COUNT(*) FROM matches WHERE status = 'collected' "
                    "GROUP BY queue_id"
                ).fetchall()
            ),
        }
