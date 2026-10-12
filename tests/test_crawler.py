from __future__ import annotations

import json
import zlib
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from fakeriot import World

from lol_collector import client as client_module
from lol_collector.client import AuthenticationError
from lol_collector.crawler import CrawlConfig, Crawler, CrawlStats, SeedError
from lol_collector.storage import Store

if TYPE_CHECKING:
    from conftest import FakeApi

CrawlFn = Callable[..., Awaitable[tuple[CrawlStats, Path]]]

SEED = "Player0#EUW"
REMAKE_ID = "EUW1_7100000003"  # 120 seconds long, stored as skipped
MASTERY_LAST_PLAYED = "2025-10-09T08:53:20Z"  # lastPlayTime of the fake mastery payload


def query(db_path: Path, sql: str) -> list[tuple[Any, ...]]:
    with Store(db_path) as store:
        return store.connection().execute(sql).fetchall()


def match_statuses(db_path: Path) -> dict[str, tuple[int, str, str | None]]:
    """match_id -> (queue_id, status, skip_reason) for every row of the matches table."""
    rows = query(db_path, "SELECT match_id, queue_id, status, skip_reason FROM matches")
    return {match_id: (queue, status, reason) for match_id, queue, status, reason in rows}


def collected_ids(db_path: Path) -> set[str]:
    return {m for m, (_, status, _) in match_statuses(db_path).items() if status == "collected"}


def ranked_ids(world: World) -> set[str]:
    return {m for m, match in world.matches.items() if match["info"]["queueId"] == 420}


def collectable_ranked_ids(world: World) -> set[str]:
    """Ranked matches a crawl stores as collected: everything except the remake."""
    return ranked_ids(world) - {REMAKE_ID}


@pytest.fixture
async def full_crawl(crawl: CrawlFn) -> tuple[CrawlStats, Path]:
    """Default crawl from Player0: ranked queue, timeline, rank and mastery enabled."""
    return await crawl(seeds=[SEED])


@pytest.fixture
async def crawler(fake_api: FakeApi, tmp_path: Path) -> AsyncIterator[Crawler]:
    """A crawler with an open client and store, for calls that do not run a crawl."""
    with Store(tmp_path / "lol.sqlite3") as store:
        async with fake_api.client() as client:
            yield Crawler(client, store, CrawlConfig(platform="EUW1", max_matches=10))


async def test_full_crawl_stores_only_ranked_matches(
    full_crawl: tuple[CrawlStats, Path], world: World
) -> None:
    stats, db_path = full_crawl
    assert set(match_statuses(db_path)) == ranked_ids(world)
    assert collected_ids(db_path) == collectable_ranked_ids(world)
    assert (stats.collected, stats.skipped, stats.failed) == (35, 1, 0)

    with Store(db_path) as store:
        summary = store.summary()
    assert summary["queues"] == {420: 35}
    assert summary["participant_rows"] == 350


async def test_remake_is_stored_as_skipped(full_crawl: tuple[CrawlStats, Path]) -> None:
    _, db_path = full_crawl
    assert match_statuses(db_path)[REMAKE_ID] == (420, "skipped", "remake")


@pytest.mark.parametrize("max_matches", [1, 4, 10, 17, 30])
async def test_max_matches_caps_collected_matches(crawl: CrawlFn, max_matches: int) -> None:
    stats, db_path = await crawl(seeds=[SEED], max_matches=max_matches)
    assert stats.collected == max_matches
    assert len(collected_ids(db_path)) == max_matches


async def test_max_depth_zero_crawls_only_the_seed_matches(crawl: CrawlFn, world: World) -> None:
    stats, db_path = await crawl(seeds=[SEED], max_depth=0)
    seed_matches = set(world.player("p0").match_ids) & collectable_ranked_ids(world)
    assert collected_ids(db_path) == seed_matches
    assert stats.players == 1


async def test_second_crawl_without_seeds_collects_nothing_new(
    crawl: CrawlFn, full_crawl: tuple[CrawlStats, Path]
) -> None:
    _, db_path = full_crawl
    before = match_statuses(db_path)
    stats, _ = await crawl(db_path=db_path)
    assert (stats.collected, stats.skipped, stats.players) == (0, 0, 0)
    assert match_statuses(db_path) == before


async def test_queues_none_also_collects_aram(crawl: CrawlFn, world: World) -> None:
    _, db_path = await crawl(seeds=[SEED], queues=None)
    assert set(match_statuses(db_path)) == set(world.matches)
    assert collected_ids(db_path) == set(world.matches) - {REMAKE_ID}
    assert match_statuses(db_path)[REMAKE_ID] == (420, "skipped", "remake")


async def test_rank_and_mastery_columns_are_filled(
    full_crawl: tuple[CrawlStats, Path], world: World
) -> None:
    _, db_path = full_crawl
    rows = query(
        db_path,
        "SELECT puuid, solo_tier, solo_division, solo_lp,"
        " mastery_level, mastery_points, mastery_last_play_utc FROM participants",
    )
    assert len(rows) == 350
    for puuid, tier, division, lp, level, points, last_played in rows:
        player = world.by_puuid[puuid]
        assert (tier, division) == (player.tier, player.division)
        assert lp == (player.lp if player.tier else None)
        assert (level, points, last_played) == (12, 123456, MASTERY_LAST_PLAYED)


async def test_laning_columns_come_from_the_timeline(
    full_crawl: tuple[CrawlStats, Path], world: World
) -> None:
    _, db_path = full_crawl
    rows = query(db_path, "SELECT match_id, participant_id, gold_at_10, cs_at_10 FROM participants")
    assert len(rows) == 350
    for match_id, participant_id, gold, cs in rows:
        frames = world.timelines[match_id]["info"]["frames"]
        frame = next(f for f in frames if f["timestamp"] >= 10 * 60_000)
        pframe = frame["participantFrames"][str(participant_id)]
        assert gold == pframe["totalGold"]
        assert cs == pframe["minionsKilled"] + pframe["jungleMinionsKilled"]


@pytest.mark.parametrize(
    ("option", "endpoint", "columns"),
    [
        ("rank", "/lol/league/", ["solo_tier", "solo_lp", "rank_snapshot_utc"]),
        ("mastery", "/champion-mastery/", ["mastery_level", "mastery_points"]),
        ("timeline", "/timeline", ["gold_at_10", "cs_at_10"]),
    ],
)
async def test_disabled_option_makes_no_request_and_leaves_columns_empty(
    crawl: CrawlFn, fake_api: FakeApi, option: str, endpoint: str, columns: list[str]
) -> None:
    _, db_path = await crawl(seeds=[SEED], **{option: False})
    assert not any(endpoint in path for path in fake_api.riot.requests)
    values = query(db_path, f"SELECT {', '.join(columns)} FROM participants")
    assert values
    assert all(value is None for row in values for value in row)


async def test_keep_raw_stores_compressed_payloads_of_collected_matches(
    crawl: CrawlFn, world: World
) -> None:
    _, db_path = await crawl(seeds=[SEED], keep_raw=True)
    rows = query(db_path, "SELECT match_id, match_json, timeline_json FROM raw_matches")
    assert {match_id for match_id, _, _ in rows} == collected_ids(db_path)
    for match_id, match_blob, timeline_blob in rows:
        assert json.loads(zlib.decompress(match_blob)) == world.matches[match_id]
        assert json.loads(zlib.decompress(timeline_blob)) == world.timelines[match_id]


async def test_raw_payloads_are_not_kept_by_default(full_crawl: tuple[CrawlStats, Path]) -> None:
    _, db_path = full_crawl
    assert query(db_path, "SELECT COUNT(*) FROM raw_matches") == [(0,)]


async def test_puuid_seed_is_returned_unchanged_without_a_request(
    crawler: Crawler, fake_api: FakeApi, world: World
) -> None:
    puuid = world.player("p0").puuid
    assert len(puuid) == 78
    assert await crawler.resolve_seed(puuid) == puuid
    assert fake_api.riot.requests == []


async def test_riot_id_seed_resolves_to_its_puuid(crawler: Crawler, world: World) -> None:
    assert await crawler.resolve_seed(SEED) == world.player("p0").puuid


@pytest.mark.parametrize(
    ("seed", "message"),
    [
        ("nonsense", "neither a Riot ID"),
        ("#EUW", "neither a Riot ID"),
        ("Player0#", "neither a Riot ID"),
        ("Nobody#EUW", "was not found"),
    ],
)
async def test_invalid_seed_raises_seed_error(crawler: Crawler, seed: str, message: str) -> None:
    with pytest.raises(SeedError, match=message):
        await crawler.resolve_seed(seed)


@pytest.mark.parametrize(
    ("spec", "size", "tier", "division", "expected_count"),
    [
        ("MASTER", 5, "MASTER", None, 5),
        # Only one GOLD II player exists in the fake world; a GOLD-only pool would fill 3 slots.
        ("GOLD:II", 3, "GOLD", "II", 1),
    ],
)
async def test_ladder_seeds_draw_from_the_requested_tier(
    crawler: Crawler,
    world: World,
    spec: str,
    size: int,
    tier: str,
    division: str | None,
    expected_count: int,
) -> None:
    seeds = await crawler.ladder_seeds(spec, size)
    pool = {p.puuid for p in world.players.values() if (p.tier, p.division) == (tier, division)}
    assert len(seeds) == len(set(seeds)) == expected_count
    assert set(seeds) <= pool


async def test_rejected_key_propagates_out_of_run(
    crawl: CrawlFn, fake_api: FakeApi, world: World
) -> None:
    fake_api.riot.reject_key = True
    with pytest.raises(AuthenticationError, match="rejected the key"):
        await crawl(seeds=[world.player("p0").puuid])
    assert len(fake_api.riot.requests) == 1  # a rejected key is not retried


async def test_stopped_crawl_resumes_without_losing_queued_matches(
    crawl: CrawlFn, world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Matches queued when the crawl stops are fetched by the next run."""
    seed_matches = set(world.player("p0").match_ids) & collectable_ranked_ids(world)
    original_save = Store.save_match
    crawlers: list[Crawler] = []
    original_init = Crawler.__init__

    def remember(self: Crawler, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        crawlers.append(self)

    def save_then_stop(self: Store, *args: Any, **kwargs: Any) -> None:
        original_save(self, *args, **kwargs)
        crawlers[-1].stop()

    monkeypatch.setattr(Crawler, "__init__", remember)
    monkeypatch.setattr(Store, "save_match", save_then_stop)
    _, db_path = await crawl(seeds=[SEED], max_depth=0, concurrency=1)
    assert 0 < len(collected_ids(db_path)) < len(seed_matches)

    monkeypatch.setattr(Store, "save_match", original_save)
    await crawl(db_path=db_path, max_depth=0, concurrency=1)
    assert collected_ids(db_path) == seed_matches


async def test_failed_match_list_is_retried_on_the_next_run(
    crawl: CrawlFn, fake_api: FakeApi, world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_wait(delay: float) -> None:
        return None

    # Only the client's back-off: the crawler's own sleeps must still yield.
    monkeypatch.setattr(client_module, "asyncio", SimpleNamespace(sleep=no_wait))
    fake_api.riot.fail_500_times = 6  # the first match list request and its five retries
    stats, db_path = await crawl(seeds=[world.player("p0").puuid], max_depth=0)
    assert stats.collected == 0
    assert query(db_path, "SELECT status FROM players") == [("retry",)]

    await crawl(db_path=db_path, max_depth=0)
    seed_matches = set(world.player("p0").match_ids) & collectable_ranked_ids(world)
    assert collected_ids(db_path) == seed_matches
    assert query(db_path, "SELECT status FROM players") == [("done",)]


async def test_every_match_is_fetched_once(crawl: CrawlFn, fake_api: FakeApi) -> None:
    await crawl(seeds=[SEED], concurrency=4)
    detail_requests = [
        path
        for path in fake_api.riot.requests
        if "/lol/match/v5/matches/EUW1_" in path and not path.endswith("/timeline")
    ]
    assert len(detail_requests) == len(set(detail_requests))
