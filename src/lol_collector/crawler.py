"""Breadth-first crawl over players and their match histories."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from .client import AuthenticationError, RiotApiError, RiotClient
from .routing import account_region, match_region, platform_from_match_id, platform_host
from .static_data import StaticData, StaticDataProvider, patch_of
from .storage import Store, utc_now
from .transform import (
    MatchFilter,
    Row,
    mastery_columns,
    match_row,
    participant_rows,
    rank_columns,
    skip_reason,
    team_rows,
)

log = logging.getLogger(__name__)

PAGE_SIZE = 100  # match-v5 returns at most 100 IDs per request
APEX_TIERS = ("CHALLENGER", "GRANDMASTER", "MASTER")
TIERS = ("DIAMOND", "EMERALD", "PLATINUM", "GOLD", "SILVER", "BRONZE", "IRON")
DIVISIONS = ("I", "II", "III", "IV")
RANKED_QUEUE_TYPES = {420: "RANKED_SOLO_5x5", 440: "RANKED_FLEX_SR"}

QueueItem = tuple[str, int, str]  # match ID, crawl depth, PUUID whose history listed it


class SeedError(ValueError):
    """A seed could not be parsed or resolved to a PUUID."""


@dataclass(frozen=True)
class CrawlConfig:
    platform: str
    max_matches: int
    queues: frozenset[int] | None = frozenset({420})
    start_time_s: int | None = None
    end_time_s: int | None = None
    matches_per_player: int = 20
    max_depth: int | None = None
    min_duration_s: int = 300
    concurrency: int = 8
    timeline: bool = True
    rank: bool = True
    mastery: bool = True
    keep_raw: bool = False
    rank_max_age: timedelta = timedelta(hours=24)

    @property
    def match_filter(self) -> MatchFilter:
        return MatchFilter(
            queues=self.queues,
            min_duration_s=self.min_duration_s,
            start_time_s=self.start_time_s,
            end_time_s=self.end_time_s,
        )


@dataclass
class CrawlStats:
    collected: int = 0
    skipped: int = 0
    failed: int = 0
    players: int = 0
    started: float = field(default_factory=time.monotonic)

    @property
    def elapsed_s(self) -> float:
        return time.monotonic() - self.started


@dataclass
class _PlayerProgress:
    """Matches of one player's history that are queued or being processed."""

    outstanding: int = 0
    complete: bool = False  # every match of the history has been queued
    failed: bool = False


def is_puuid(value: str) -> bool:
    return len(value) == 78 and "#" not in value


class Crawler:
    """Collects matches starting from seed players and expanding to everyone they played with."""

    def __init__(
        self,
        client: RiotClient,
        store: Store,
        config: CrawlConfig,
        static: StaticDataProvider | None = None,
        progress_interval_s: float = 30.0,
    ) -> None:
        self.client = client
        self.store = store
        self.config = config
        self.stats = CrawlStats()
        self._static = static
        self._static_versions_saved: set[str] = set()
        self._filter = config.match_filter
        self._in_flight: set[str] = set()
        self._processed: set[str] = set()  # stored during this run
        self._players: dict[str, _PlayerProgress] = {}
        self._busy = 0
        self._stop = asyncio.Event()
        self._fatal: BaseException | None = None
        self._rank_tasks: dict[str, asyncio.Task[Row]] = {}
        self._progress_interval_s = progress_interval_s

    # ---------------------------------------------------------------- seeds

    async def resolve_seed(self, seed: str) -> str:
        """Return the PUUID for a Riot ID (``Name#TAG``) or pass a PUUID through."""
        seed = seed.strip()
        if is_puuid(seed):
            return seed
        name, sep, tag = seed.rpartition("#")
        if not sep or not name or not tag:
            raise SeedError(f"'{seed}' is neither a Riot ID (Name#TAG) nor a PUUID.")
        account = await self.client.account_by_riot_id(
            account_region(self.config.platform), name, tag
        )
        if not account or not account.get("puuid"):
            raise SeedError(f"Riot ID '{seed}' was not found.")
        return str(account["puuid"])

    async def ladder_seeds(self, spec: str, size: int) -> list[str]:
        """Random PUUIDs from a ranked tier, e.g. ``CHALLENGER`` or ``DIAMOND:II``."""
        tier, _, division = spec.upper().partition(":")
        queue_ids = sorted(self.config.queues or {420})
        queue_type = RANKED_QUEUE_TYPES.get(queue_ids[0], "RANKED_SOLO_5x5")
        host = platform_host(self.config.platform)
        entries: list[dict[str, Any]] = []
        if tier in APEX_TIERS:
            league = await self.client.apex_league(host, tier, queue_type)
            entries = list((league or {}).get("entries", []))
        elif tier in TIERS:
            if division and division not in DIVISIONS:
                raise SeedError(f"Unknown division '{division}' in '{spec}'.")
            for div in [division] if division else DIVISIONS:
                entries += await self.client.league_entries_by_tier(host, queue_type, tier, div)
        else:
            raise SeedError(f"Unknown tier '{tier}'. Use one of {', '.join(APEX_TIERS + TIERS)}.")
        puuids = [e["puuid"] for e in entries if e.get("puuid")]
        if not puuids:
            raise SeedError(f"No ranked players found for '{spec}' on {self.config.platform}.")
        return random.sample(puuids, min(size, len(puuids)))

    # ------------------------------------------------------------------ run

    def stop(self) -> None:
        """Finish the matches in progress, then return from :meth:`run`."""
        if not self._stop.is_set():
            log.info("Stopping after the matches in progress (Ctrl+C again to quit now)...")
        self._stop.set()

    def _scheduling_done(self) -> bool:
        return self.stats.collected + len(self._in_flight) >= self.config.max_matches

    async def run(self) -> CrawlStats:
        self.store.reset_unfinished()
        queue: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=self.config.concurrency * 2)
        workers = [asyncio.create_task(self._worker(queue)) for _ in range(self.config.concurrency)]
        progress = asyncio.create_task(self._progress_loop())
        try:
            await self._produce(queue)
            await queue.join()
        finally:
            for task in (*workers, progress):
                task.cancel()
            await asyncio.gather(*workers, progress, return_exceptions=True)
        if self._fatal is not None:
            raise self._fatal
        self._log_progress()
        return self.stats

    async def _produce(self, queue: asyncio.Queue[QueueItem]) -> None:
        while not self._stop.is_set() and self.stats.collected < self.config.max_matches:
            if self._scheduling_done():
                # Enough matches are in flight; some may still be skipped and free a slot.
                await asyncio.sleep(0.1)
                continue
            nxt = self.store.next_player()
            if nxt is None:
                if queue.empty() and self._busy == 0:
                    log.info("No more players to crawl.")
                    return
                # Workers may still add players from the matches they are processing.
                await asyncio.sleep(0.2)
                continue
            puuid, depth = nxt
            try:
                match_ids = await self._match_ids(puuid)
            except AuthenticationError:
                raise
            except RiotApiError as exc:
                log.warning("Match list of %s... failed, retrying next run: %s", puuid[:12], exc)
                self.store.set_player_status(puuid, "retry")
                continue
            self.store.set_player_status(puuid, "scheduled")
            progress = self._players.setdefault(puuid, _PlayerProgress())
            known = self.store.known_match_ids(match_ids)
            for match_id in match_ids:
                if self._stop.is_set() or self._scheduling_done():
                    # Picked up again once a slot frees up, or on the next run.
                    self.store.set_player_status(puuid, "pending")
                    break
                # Matches stored while this loop waited on the queue are in _processed.
                if match_id in known or match_id in self._processed or match_id in self._in_flight:
                    continue
                self._in_flight.add(match_id)
                progress.outstanding += 1
                await queue.put((match_id, depth, puuid))
            else:
                progress.complete = True
                self._settle(puuid)

    def _settle(self, puuid: str) -> None:
        """Mark a player done once every queued match of its history has been processed."""
        progress = self._players.get(puuid)
        if progress is None or not progress.complete or progress.outstanding:
            return
        del self._players[puuid]
        self.store.set_player_status(puuid, "retry" if progress.failed else "done")
        if not progress.failed:
            self.stats.players += 1

    async def _match_ids(self, puuid: str) -> list[str]:
        """A player's match IDs, filtered by the API. Several queues mean one list per queue."""
        queues: list[int | None] = [*sorted(self.config.queues)] if self.config.queues else [None]
        ids: list[str] = []
        for queue in queues:
            ids += [m for m in await self._match_ids_for_queue(puuid, queue) if m not in ids]
        return ids

    async def _match_ids_for_queue(self, puuid: str, queue: int | None) -> list[str]:
        region = match_region(self.config.platform)
        ids: list[str] = []
        while len(ids) < self.config.matches_per_player:
            count = min(PAGE_SIZE, self.config.matches_per_player - len(ids))
            page = await self.client.match_ids(
                region,
                puuid,
                queue=queue,
                start_time=self.config.start_time_s,
                end_time=self.config.end_time_s,
                start=len(ids),
                count=count,
            )
            ids += page
            if len(page) < count:
                break
        return ids

    async def _worker(self, queue: asyncio.Queue[QueueItem]) -> None:
        while True:
            match_id, depth, puuid = await queue.get()
            self._busy += 1
            processed = failed = False
            try:
                if not self._stop.is_set():
                    await self._process(match_id, depth)
                    processed = True
            except AuthenticationError as exc:
                self._fatal = exc
                self._stop.set()
            except RiotApiError as exc:
                self.stats.failed += 1
                processed = failed = True
                log.warning("Match %s failed: %s", match_id, exc)
            except Exception:
                self.stats.failed += 1
                processed = failed = True
                log.exception("Unexpected error while processing %s", match_id)
            finally:
                self._in_flight.discard(match_id)
                progress = self._players.get(puuid)
                if processed and progress is not None:
                    progress.outstanding -= 1
                    progress.failed |= failed
                    self._settle(puuid)
                elif not processed:
                    # Dropped while stopping: crawl this player again on the next run.
                    self.store.set_player_status(puuid, "pending")
                self._busy -= 1
                queue.task_done()

    # --------------------------------------------------------------- match

    async def _process(self, match_id: str, depth: int) -> None:
        region = match_region(platform_from_match_id(match_id))
        match = await self.client.match(region, match_id)
        if match is None:
            self._save_skipped({"metadata": {"matchId": match_id}}, "not found")
            return
        reason = skip_reason(match, self._filter)
        if reason:
            self._save_skipped(match, reason)
            return

        timeline = await self.client.timeline(region, match_id) if self.config.timeline else None
        static = await self._static_for(match)
        participants = participant_rows(match, timeline, static)
        teams = team_rows(match, timeline, static)

        host = str(match["info"].get("platformId") or self.config.platform).lower()
        if self.config.rank or self.config.mastery:
            await asyncio.gather(*(self._enrich(row, host) for row in participants))

        self.store.save_match(
            match_row(match, "collected", None, utc_now()),
            participants,
            teams,
            raw=(match, timeline) if self.config.keep_raw else None,
        )
        self._processed.add(match_id)
        self.stats.collected += 1

        if self.config.max_depth is None or depth < self.config.max_depth:
            puuids = [r["puuid"] for r in participants if r["puuid"] and r["puuid"] != "BOT"]
            self.store.add_players(puuids, depth + 1)

    def _save_skipped(self, match: dict[str, Any], reason: str) -> None:
        self.store.save_match(match_row(match, "skipped", reason, utc_now()))
        self._processed.add(match["metadata"]["matchId"])
        self.stats.skipped += 1
        log.debug("Skipped %s: %s", match["metadata"]["matchId"], reason)

    async def _static_for(self, match: dict[str, Any]) -> StaticData:
        if self._static is None:
            return StaticData()
        data = await self._static.for_patch(patch_of(match["info"].get("gameVersion")))
        if data.version and data.version not in self._static_versions_saved:
            self.store.save_items(data.items.values())
            self.store.save_champions(data.champions)
            self._static_versions_saved.add(data.version)
        return data

    # ---------------------------------------------------------- enrichment

    async def _enrich(self, row: Row, host: str) -> None:
        puuid = row.get("puuid")
        if not puuid or puuid == "BOT":
            return
        try:
            if self.config.rank:
                row.update(await self._rank(puuid, host))
            if self.config.mastery and row.get("champion_id") is not None:
                mastery = await self.client.champion_mastery(host, puuid, int(row["champion_id"]))
                row.update(mastery_columns(mastery))
        except AuthenticationError:
            raise
        except RiotApiError as exc:
            log.debug("Enrichment failed for %s...: %s", puuid[:12], exc)

    async def _rank(self, puuid: str, host: str) -> Row:
        # Share one lookup between matches processed concurrently for the same player;
        # later lookups are answered by the SQLite cache.
        task = self._rank_tasks.get(puuid)
        if task is None:
            task = asyncio.ensure_future(self._fetch_rank(puuid, host))
            self._rank_tasks[puuid] = task
            task.add_done_callback(lambda _: self._rank_tasks.pop(puuid, None))
        return dict(await task)

    async def _fetch_rank(self, puuid: str, host: str) -> Row:
        cached = self.store.cached_rank(puuid, self.config.rank_max_age)
        if cached is not None:
            entries, fetched_at = cached
            return rank_columns(entries, fetched_at)
        entries = await self.client.league_entries(host, puuid)
        fetched_at = utc_now()
        self.store.save_rank(puuid, entries, fetched_at)
        return rank_columns(entries, fetched_at)

    # ------------------------------------------------------------ progress

    async def _progress_loop(self) -> None:
        while True:
            await asyncio.sleep(self._progress_interval_s)
            self._log_progress()

    def _log_progress(self) -> None:
        minutes = max(self.stats.elapsed_s / 60, 1e-9)
        requests = sum(self.client.requests.values())
        throttled = sum(self.client.rate_limited.values())
        log.info(
            "%d/%d matches collected (%.1f/min) | %d skipped | %d failed | %d players crawled, "
            "%d queued | %d requests, %d rate-limited",
            self.stats.collected,
            self.config.max_matches,
            self.stats.collected / minutes,
            self.stats.skipped,
            self.stats.failed,
            self.stats.players,
            self.store.pending_players(),
            requests,
            throttled,
        )
