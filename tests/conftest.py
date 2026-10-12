"""Shared fixtures: a fake Riot API on localhost and a database collected from it."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path

import aiohttp
import pytest
from aiohttp.test_utils import TestServer
from fakeriot import FakeRiot, World

from lol_collector.client import RiotClient
from lol_collector.crawler import CrawlConfig, Crawler
from lol_collector.static_data import StaticDataProvider
from lol_collector.storage import Store

API_KEY = "test-key"


@dataclass
class FakeApi:
    riot: FakeRiot
    base_url: str  # e.g. http://127.0.0.1:12345

    @property
    def api_url(self) -> str:
        """Template for RiotClient(base_url=...)."""
        return self.base_url + "/{host}"

    def client(self, api_key: str = API_KEY, **kwargs: object) -> RiotClient:
        return RiotClient(api_key, base_url=self.api_url, **kwargs)  # type: ignore[arg-type]

    def static(self, session: aiohttp.ClientSession, cache_dir: Path) -> StaticDataProvider:
        return StaticDataProvider(
            session,
            cache_dir,
            ddragon_url=self.base_url + "/ddragon",
            queues_url=self.base_url + "/static/queues.json",
        )


@pytest.fixture
def world() -> World:
    return World(seed=7, players=60, matches=40)


@pytest.fixture
async def fake_api(world: World) -> AsyncIterator[FakeApi]:
    riot = FakeRiot(world, app_limit="100000:1")
    server = TestServer(riot.app())
    await server.start_server()
    try:
        yield FakeApi(riot, str(server.make_url("")).rstrip("/"))
    finally:
        await server.close()


@pytest.fixture
def crawl(fake_api: FakeApi, tmp_path: Path) -> Callable[..., object]:
    """``await crawl(db_path, seeds=[...], **config)`` runs a full crawl against the fake API."""

    async def run(db_path: Path | None = None, seeds: list[str] | None = None, **config: object):
        db_path = db_path or tmp_path / "lol.sqlite3"
        settings: dict[str, object] = {"platform": "EUW1", "max_matches": 1000}
        settings.update(config)
        with Store(db_path) as store:
            async with fake_api.client() as client, aiohttp.ClientSession() as http:
                crawler = Crawler(
                    client,
                    store,
                    CrawlConfig(**settings),  # type: ignore[arg-type]
                    fake_api.static(http, tmp_path / "cache"),
                )
                if seeds:
                    store.add_seeds([await crawler.resolve_seed(s) for s in seeds])
                stats = await crawler.run()
        return stats, db_path

    return run


@pytest.fixture
async def collected_db(crawl: Callable[..., object]) -> Path:
    """Database with every ranked match of the fake world collected."""
    _, db_path = await crawl(seeds=["Player0#EUW"])  # type: ignore[misc]
    return db_path  # type: ignore[no-any-return]
