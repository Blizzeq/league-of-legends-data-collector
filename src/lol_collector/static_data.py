"""Static game data (champion, item, rune, spell and queue names) from Data Dragon.

Files are cached on disk per Data Dragon version, so a dataset spanning several
patches downloads each version once. Failures are logged and names fall back to
placeholders such as ``Item 3031``: static data improves readability but is never
required for collection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

DDRAGON_URL = "https://ddragon.leagueoflegends.com"
QUEUES_URL = "https://static.developer.riotgames.com/docs/lol/queues.json"
RETRY_AFTER_S = 300.0  # how long an incomplete download is reused before trying again

# Used when queues.json cannot be fetched; covers the queues people usually collect.
FALLBACK_QUEUES: dict[int, str] = {
    400: "Normal Draft",
    420: "Ranked Solo/Duo",
    430: "Normal Blind",
    440: "Ranked Flex",
    450: "ARAM",
    480: "Swiftplay",
    490: "Quickplay",
    700: "Clash",
    720: "ARAM Clash",
    900: "ARURF",
    1700: "Arena",
}

POSITIONS: dict[str, str] = {
    "TOP": "TOP",
    "JUNGLE": "JUNGLE",
    "MIDDLE": "MID",
    "BOTTOM": "BOT",
    "UTILITY": "SUPPORT",
}


@dataclass(frozen=True)
class Item:
    item_id: int
    name: str
    gold_total: int
    completed: bool


@dataclass
class StaticData:
    """Lookup tables for one Data Dragon version. Every lookup falls back gracefully."""

    version: str | None = None
    champions: dict[int, str] = field(default_factory=dict)
    items: dict[int, Item] = field(default_factory=dict)
    summoner_spells: dict[int, str] = field(default_factory=dict)
    runes: dict[int, str] = field(default_factory=dict)
    queues: dict[int, str] = field(default_factory=lambda: dict(FALLBACK_QUEUES))

    def champion_name(self, champion_id: int | None, fallback: str | None = None) -> str | None:
        if champion_id is None:
            return fallback
        return self.champions.get(champion_id, fallback)

    def item_name(self, item_id: int) -> str:
        item = self.items.get(item_id)
        return item.name if item else f"Item {item_id}"

    def spell_name(self, spell_id: int | None) -> str | None:
        if spell_id is None:
            return None
        return self.summoner_spells.get(spell_id, f"Spell {spell_id}")

    def rune_name(self, rune_id: int | None) -> str | None:
        if rune_id is None:
            return None
        return self.runes.get(rune_id, f"Rune {rune_id}")

    def queue_name(self, queue_id: int | None) -> str | None:
        if queue_id is None:
            return None
        return self.queues.get(queue_id, f"Queue {queue_id}")


def _is_completed_item(data: dict[str, Any]) -> bool:
    """Legendary items and boots: what players build, as opposed to components."""
    tags = set(data.get("tags", []))
    if tags & {"Consumable", "Trinket"} or not data.get("gold", {}).get("purchasable", True):
        return False
    if "Boots" in tags:
        return int(data.get("depth", 1)) >= 2
    return not data.get("into") and int(data.get("gold", {}).get("total", 0)) >= 2000


def parse_champions(payload: dict[str, Any]) -> dict[int, str]:
    return {int(c["key"]): c["name"] for c in payload.get("data", {}).values()}


def parse_items(payload: dict[str, Any]) -> dict[int, Item]:
    items = {}
    for key, data in payload.get("data", {}).items():
        item_id = int(key)
        items[item_id] = Item(
            item_id=item_id,
            name=data.get("name", f"Item {item_id}"),
            gold_total=int(data.get("gold", {}).get("total", 0)),
            completed=_is_completed_item(data),
        )
    return items


def parse_summoner_spells(payload: dict[str, Any]) -> dict[int, str]:
    return {int(s["key"]): s["name"] for s in payload.get("data", {}).values()}


def parse_runes(payload: list[dict[str, Any]]) -> dict[int, str]:
    names: dict[int, str] = {}
    for style in payload:
        names[int(style["id"])] = style["name"]
        for slot in style.get("slots", []):
            for rune in slot.get("runes", []):
                names[int(rune["id"])] = rune["name"]
    return names


def parse_queues(payload: list[dict[str, Any]]) -> dict[int, str]:
    queues = dict(FALLBACK_QUEUES)
    for q in payload:
        description = (q.get("description") or "").strip()
        if q.get("queueId") is not None and description:
            # queues.json describes queues like "5v5 Ranked Solo games"; keep ours when known.
            queues.setdefault(int(q["queueId"]), description.removesuffix(" games"))
    return queues


def patch_of(game_version: str | None) -> str | None:
    """``"16.19.712.4120"`` -> ``"16.19"``."""
    if not game_version:
        return None
    parts = game_version.split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else None


class StaticDataProvider:
    """Loads and caches :class:`StaticData` per patch."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        cache_dir: Path,
        *,
        ddragon_url: str = DDRAGON_URL,
        queues_url: str = QUEUES_URL,
        language: str = "en_US",
    ) -> None:
        self._session = session
        self._cache_dir = cache_dir
        self._ddragon_url = ddragon_url
        self._queues_url = queues_url
        self._language = language
        self._versions: list[str] | None = None
        self._queues: dict[int, str] | None = None
        self._by_version: dict[str, StaticData] = {}
        self._incomplete_until: dict[str, float] = {}
        self._lock = asyncio.Lock()  # concurrent workers would download the same files

    async def _get_json(self, url: str, cache_file: Path | None) -> Any | None:
        if cache_file is not None and cache_file.exists():
            try:
                return json.loads(cache_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                log.warning("Ignoring unreadable cache file %s: %s", cache_file, exc)
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                resp.raise_for_status()
                payload = await resp.json(content_type=None)
        except (aiohttp.ClientError, TimeoutError, json.JSONDecodeError) as exc:
            log.warning("Could not load static data from %s: %s", url, exc)
            return None
        if cache_file is not None:
            # Write then rename, so an interrupted write never leaves a corrupt cache file.
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            partial = cache_file.with_suffix(".tmp")
            partial.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(partial, cache_file)
        return payload

    async def _versions_list(self) -> list[str]:
        if self._versions is None:
            payload = await self._get_json(f"{self._ddragon_url}/api/versions.json", None)
            if payload:
                self._versions = [v for v in payload if isinstance(v, str)]
            else:
                cached = self._cache_dir / "ddragon"
                self._versions = (
                    sorted((p.name for p in cached.iterdir()), key=_version_key, reverse=True)
                    if cached.is_dir()
                    else []
                )
        return self._versions

    async def _queue_names(self) -> dict[int, str]:
        if self._queues is None:
            payload = await self._get_json(self._queues_url, self._cache_dir / "queues.json")
            self._queues = (
                parse_queues(payload) if isinstance(payload, list) else dict(FALLBACK_QUEUES)
            )
        return self._queues

    async def resolve_version(self, patch: str | None) -> str | None:
        """Newest Data Dragon version for ``patch`` (``"16.19"``), else the newest overall."""
        versions = await self._versions_list()
        if not versions:
            return None
        if patch:
            for version in versions:
                if patch_of(version) == patch:
                    return version
        return versions[0]

    async def for_patch(self, patch: str | None) -> StaticData:
        async with self._lock:
            return await self._load(patch)

    async def _load(self, patch: str | None) -> StaticData:
        version = await self.resolve_version(patch)
        if version is None:
            return StaticData(queues=await self._queue_names())
        cached = self._by_version.get(version)
        retry_at = self._incomplete_until.get(version)
        if cached is not None and (retry_at is None or time.monotonic() < retry_at):
            return cached

        base = f"{self._ddragon_url}/cdn/{version}/data/{self._language}"
        cache = self._cache_dir / "ddragon" / version
        champions = await self._get_json(f"{base}/champion.json", cache / "champion.json")
        items = await self._get_json(f"{base}/item.json", cache / "item.json")
        spells = await self._get_json(f"{base}/summoner.json", cache / "summoner.json")
        runes = await self._get_json(f"{base}/runesReforged.json", cache / "runesReforged.json")

        data = StaticData(
            version=version,
            champions=parse_champions(champions) if isinstance(champions, dict) else {},
            items=parse_items(items) if isinstance(items, dict) else {},
            summoner_spells=parse_summoner_spells(spells) if isinstance(spells, dict) else {},
            runes=parse_runes(runes) if isinstance(runes, list) else {},
            queues=await self._queue_names(),
        )
        self._by_version[version] = data
        if any(p is None for p in (champions, items, spells, runes)):
            self._incomplete_until[version] = time.monotonic() + RETRY_AFTER_S
        else:
            self._incomplete_until.pop(version, None)
        return data


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(p) if p.isdigit() else -1 for p in version.split("."))
