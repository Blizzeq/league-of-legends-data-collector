"""Async Riot Games API client for the League of Legends endpoints the collector needs."""

from __future__ import annotations

import asyncio
import logging
import random
from collections import Counter
from types import TracebackType
from typing import Any
from urllib.parse import quote

import aiohttp

from .ratelimit import DEFAULT_APP_LIMITS, RateLimiter, parse_limits

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://{host}.api.riotgames.com"
RETRYABLE_STATUS = frozenset({500, 502, 503, 504})


class RiotApiError(Exception):
    """A request failed and retrying will not help (or retries ran out)."""

    def __init__(self, message: str, *, status: int | None = None, url: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.url = url


class AuthenticationError(RiotApiError):
    """The API key was rejected (401/403). Development keys expire after 24 hours."""


class RiotClient:
    """Thin async wrapper over the Riot API with rate limiting and retries.

    One client serves one API key. Rate limiters are kept per routing value
    (application limit) and per routing value + endpoint (method limit), and are
    updated from the ``X-App-Rate-Limit`` / ``X-Method-Rate-Limit`` headers.
    A 404 is returned as ``None``; other failures raise :class:`RiotApiError`.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 20.0,
        max_retries: int = 5,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("A Riot API key is required (set RIOT_API_KEY).")
        self._api_key = api_key
        self._base_url = base_url
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_retries = max_retries
        self._session = session
        self._owns_session = session is None
        self._app_limiters: dict[str, RateLimiter] = {}
        self._method_limiters: dict[tuple[str, str], RateLimiter] = {}
        self.requests: Counter[str] = Counter()
        self.rate_limited: Counter[str] = Counter()

    async def __aenter__(self) -> RiotClient:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers={"X-Riot-Token": self._api_key})
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------ core

    def _app_limiter(self, host: str) -> RateLimiter:
        if host not in self._app_limiters:
            self._app_limiters[host] = RateLimiter(DEFAULT_APP_LIMITS)
        return self._app_limiters[host]

    def _method_limiter(self, host: str, method: str) -> RateLimiter:
        key = (host, method)
        if key not in self._method_limiters:
            self._method_limiters[key] = RateLimiter()
        return self._method_limiters[key]

    async def request(
        self,
        host: str,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any | None:
        """GET ``path`` on routing value ``host`` and return the decoded JSON body.

        ``method`` names the endpoint (e.g. ``"match-v5.getMatch"``) and selects the
        method-level limiter. Returns ``None`` for 404.
        """
        if self._session is None:
            raise RuntimeError("RiotClient must be used as an async context manager.")
        url = self._base_url.format(host=host) + path
        query = {k: v for k, v in (params or {}).items() if v is not None}
        app_limiter = self._app_limiter(host)
        method_limiter = self._method_limiter(host, method)

        attempt = 0
        while True:
            await app_limiter.acquire()
            await method_limiter.acquire()
            self.requests[method] += 1
            try:
                async with self._session.get(
                    url,
                    params=query,
                    timeout=self._timeout,
                    headers={"X-Riot-Token": self._api_key},
                ) as resp:
                    self._learn_limits(resp, app_limiter, method_limiter)
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status == 404:
                        return None
                    if resp.status == 429:
                        self._on_rate_limited(resp, method, app_limiter, method_limiter, attempt)
                        attempt += 1
                        if attempt > self._max_retries:
                            raise RiotApiError("Rate limited too many times", status=429, url=url)
                        continue
                    if resp.status in (401, 403):
                        raise AuthenticationError(
                            f"Riot API rejected the key ({resp.status}). Development keys "
                            "expire 24 hours after they are generated; get a new one at "
                            "https://developer.riotgames.com/.",
                            status=resp.status,
                            url=url,
                        )
                    body = (await resp.text())[:200]
                    if resp.status not in RETRYABLE_STATUS:
                        raise RiotApiError(
                            f"HTTP {resp.status} for {url}: {body}", status=resp.status, url=url
                        )
                    reason = f"HTTP {resp.status}"
            except (TimeoutError, aiohttp.ClientConnectionError) as exc:
                reason = type(exc).__name__

            attempt += 1
            if attempt > self._max_retries:
                raise RiotApiError(f"Giving up after {attempt} attempts ({reason})", url=url)
            delay = min(60.0, 2.0**attempt) * (0.5 + random.random() / 2)
            log.warning("%s on %s, retry %d in %.1fs", reason, method, attempt, delay)
            await asyncio.sleep(delay)

    @staticmethod
    def _learn_limits(resp: aiohttp.ClientResponse, app: RateLimiter, method: RateLimiter) -> None:
        app_limits = parse_limits(resp.headers.get("X-App-Rate-Limit"))
        if app_limits:
            app.set_limits(app_limits)
        method_limits = parse_limits(resp.headers.get("X-Method-Rate-Limit"))
        if method_limits:
            method.set_limits(method_limits)

    def _on_rate_limited(
        self,
        resp: aiohttp.ClientResponse,
        method: str,
        app: RateLimiter,
        method_limiter: RateLimiter,
        attempt: int,
    ) -> None:
        self.rate_limited[method] += 1
        limit_type = resp.headers.get("X-Rate-Limit-Type", "service").lower()
        try:
            retry_after = float(resp.headers["Retry-After"])
        except (KeyError, ValueError):
            # Service-level 429s usually come without Retry-After: back off exponentially.
            retry_after = min(60.0, 2.0 ** (attempt + 1))
        target = method_limiter if limit_type == "method" else app
        target.block_for(retry_after)
        log.warning("429 (%s limit) on %s, waiting %.0fs", limit_type, method, retry_after)

    # ------------------------------------------------------------- endpoints

    async def account_by_riot_id(self, region: str, game_name: str, tag_line: str) -> Any | None:
        path = (
            "/riot/account/v1/accounts/by-riot-id/"
            f"{quote(game_name, safe='')}/{quote(tag_line, safe='')}"
        )
        return await self.request(region, "account-v1.getByRiotId", path)

    async def account_by_puuid(self, region: str, puuid: str) -> Any | None:
        path = f"/riot/account/v1/accounts/by-puuid/{puuid}"
        return await self.request(region, "account-v1.getByPuuid", path)

    async def match_ids(
        self,
        region: str,
        puuid: str,
        *,
        queue: int | None = None,
        match_type: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        start: int = 0,
        count: int = 100,
    ) -> list[str]:
        """Match IDs for a player, newest first. ``start_time``/``end_time`` are epoch seconds."""
        params = {
            "queue": queue,
            "type": match_type,
            "startTime": start_time,
            "endTime": end_time,
            "start": start,
            "count": count,
        }
        path = f"/lol/match/v5/matches/by-puuid/{puuid}/ids"
        result = await self.request(region, "match-v5.getMatchIdsByPUUID", path, params)
        return list(result or [])

    async def match(self, region: str, match_id: str) -> Any | None:
        return await self.request(region, "match-v5.getMatch", f"/lol/match/v5/matches/{match_id}")

    async def timeline(self, region: str, match_id: str) -> Any | None:
        path = f"/lol/match/v5/matches/{match_id}/timeline"
        return await self.request(region, "match-v5.getTimeline", path)

    async def league_entries(self, platform: str, puuid: str) -> list[dict[str, Any]]:
        path = f"/lol/league/v4/entries/by-puuid/{puuid}"
        result = await self.request(platform, "league-v4.getLeagueEntriesByPUUID", path)
        return list(result or [])

    async def apex_league(self, platform: str, tier: str, queue: str) -> Any | None:
        """Challenger, Grandmaster or Master league for ``queue`` (e.g. ``RANKED_SOLO_5x5``)."""
        tier = tier.lower()
        if tier not in ("challenger", "grandmaster", "master"):
            raise ValueError(f"Not an apex tier: {tier}")
        path = f"/lol/league/v4/{tier}leagues/by-queue/{queue}"
        return await self.request(platform, f"league-v4.get{tier.title()}League", path)

    async def league_entries_by_tier(
        self, platform: str, queue: str, tier: str, division: str, page: int = 1
    ) -> list[dict[str, Any]]:
        """One page of ranked entries for a tier and division (league-exp-v4)."""
        path = f"/lol/league-exp/v4/entries/{queue}/{tier.upper()}/{division.upper()}"
        result = await self.request(
            platform, "league-exp-v4.getLeagueEntries", path, {"page": page}
        )
        return list(result or [])

    async def champion_mastery(self, platform: str, puuid: str, champion_id: int) -> Any | None:
        path = (
            f"/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}"
            f"/by-champion/{champion_id}"
        )
        return await self.request(
            platform, "champion-mastery-v4.getChampionMasteryByPUUIDAndChampionId", path
        )
