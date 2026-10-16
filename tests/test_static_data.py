from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
import pytest
from fakeriot import ddragon_payloads

from lol_collector import static_data
from lol_collector.static_data import (
    FALLBACK_QUEUES,
    StaticData,
    StaticDataProvider,
    parse_items,
    parse_queues,
    patch_of,
)

if TYPE_CHECKING:
    from conftest import FakeApi


@pytest.mark.parametrize(
    ("version", "patch"),
    [("16.19.712.4120", "16.19"), ("16.19.1", "16.19"), ("16", None), (None, None), ("", None)],
)
def test_patch_of(version: str | None, patch: str | None) -> None:
    assert patch_of(version) == patch


def test_completed_items_are_legendaries_and_upgraded_boots() -> None:
    items = parse_items(ddragon_payloads()["item"])
    assert items[3031].completed  # Infinity Edge
    assert items[3006].completed  # Berserker's Greaves
    assert not items[1036].completed  # Long Sword builds into something
    assert not items[2055].completed  # Control Ward is a consumable
    assert not items[3340].completed  # Stealth Ward is a trinket


def test_queue_names_keep_curated_names_and_add_unknown_queues() -> None:
    queues = parse_queues(
        [
            {"queueId": 420, "description": "5v5 Ranked Solo games"},
            {"queueId": 9999, "description": "Brand new mode games"},
            {"queueId": 0, "description": None},
        ]
    )
    assert queues[420] == FALLBACK_QUEUES[420]
    assert queues[9999] == "Brand new mode"
    assert 0 not in queues


def test_lookups_fall_back_to_ids() -> None:
    data = StaticData()
    assert data.item_name(3031) == "Item 3031"
    assert data.spell_name(4) == "Spell 4"
    assert data.rune_name(None) is None
    assert data.champion_name(62, "MonkeyKing") == "MonkeyKing"
    assert data.queue_name(420) == "Ranked Solo/Duo"


async def test_provider_picks_the_version_of_the_patch(fake_api: FakeApi, tmp_path: Path) -> None:
    async with aiohttp.ClientSession() as http:
        provider = fake_api.static(http, tmp_path)
        assert await provider.resolve_version("16.19") == "16.19.1"
        assert await provider.resolve_version("99.1") == "16.20.1"  # unknown: newest
        data = await provider.for_patch("16.19")
    assert data.version == "16.19.1"
    assert data.champion_name(62) == "Wukong"
    assert (tmp_path / "ddragon" / "16.19.1" / "champion.json").is_file()


async def test_unreadable_cache_file_is_downloaded_again(fake_api: FakeApi, tmp_path: Path) -> None:
    broken = tmp_path / "ddragon" / "16.19.1" / "champion.json"
    broken.parent.mkdir(parents=True)
    broken.write_text('{"data": {"Ahri": {"key": "103"', encoding="utf-8")  # cut short

    async with aiohttp.ClientSession() as http:
        data = await fake_api.static(http, tmp_path).for_patch("16.19")

    assert data.champion_name(103) == "Ahri"
    assert json.loads(broken.read_text(encoding="utf-8"))["data"]["Ahri"]["name"] == "Ahri"


async def test_unreachable_data_dragon_degrades_to_ids(tmp_path: Path) -> None:
    async with aiohttp.ClientSession() as http:
        provider = StaticDataProvider(
            http, tmp_path, ddragon_url="http://127.0.0.1:9", queues_url="http://127.0.0.1:9/q"
        )
        data = await provider.for_patch("16.19")
    assert data.version is None
    assert data.champions == {}
    assert data.queue_name(420) == "Ranked Solo/Duo"


async def test_incomplete_download_is_retried_after_a_while(
    fake_api: FakeApi, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [1000.0]
    monkeypatch.setattr(static_data.time, "monotonic", lambda: now[0])
    async with aiohttp.ClientSession() as http:
        provider = fake_api.static(http, tmp_path)
        real_get_json = provider._get_json

        async def items_down(url: str, cache_file: Path | None) -> object:
            return None if url.endswith("item.json") else await real_get_json(url, cache_file)

        monkeypatch.setattr(provider, "_get_json", items_down)
        first = await provider.for_patch("16.19")
        assert first.items == {}

        monkeypatch.setattr(provider, "_get_json", real_get_json)
        assert (await provider.for_patch("16.19")).items == {}  # reused within the window
        now[0] += static_data.RETRY_AFTER_S + 1
        assert (await provider.for_patch("16.19")).items  # downloaded again
