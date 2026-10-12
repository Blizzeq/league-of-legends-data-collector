from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

import pytest
from fakeriot import World

from lol_collector import client as client_module
from lol_collector.client import AuthenticationError, RiotApiError, RiotClient
from lol_collector.ratelimit import DEFAULT_APP_LIMITS

if TYPE_CHECKING:
    from conftest import FakeApi

PLAYER0_RANKED = ["EUW1_7100000039", "EUW1_7100000027", "EUW1_7100000017", "EUW1_7100000014"]
PLAYER0_ALL = [*PLAYER0_RANKED, "EUW1_7100000010"]  # the last one is an ARAM game


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the back-off delays the client asks for instead of waiting them out."""
    requested: list[float] = []

    async def record(delay: float) -> None:
        requested.append(delay)

    monkeypatch.setattr(client_module, "asyncio", SimpleNamespace(sleep=record))
    return requested


async def test_account_by_riot_id_resolves_player0(fake_api: FakeApi, world: World) -> None:
    async with fake_api.client() as client:
        account = await client.account_by_riot_id("europe", "Player0", "EUW")
    assert account == {"puuid": world.player("p0").puuid, "gameName": "Player0", "tagLine": "EUW"}
    assert fake_api.riot.requests == ["/europe/riot/account/v1/accounts/by-riot-id/Player0/EUW"]


@pytest.mark.parametrize(
    ("game_name", "tag_line"),
    [("Nobody", "EUW"), ("Player0", "NA1")],
)
async def test_unknown_riot_id_returns_none(
    fake_api: FakeApi, game_name: str, tag_line: str
) -> None:
    async with fake_api.client() as client:
        assert await client.account_by_riot_id("europe", game_name, tag_line) is None


async def test_rejected_key_raises_authentication_error_without_retry(fake_api: FakeApi) -> None:
    async with fake_api.client("bad-key") as client:
        with pytest.raises(AuthenticationError, match="rejected the key") as exc_info:
            await client.account_by_riot_id("europe", "Player0", "EUW")
    assert exc_info.value.status == 403
    assert len(fake_api.riot.requests) == 1


async def test_429_with_zero_retry_after_is_retried_until_success(
    fake_api: FakeApi, world: World
) -> None:
    fake_api.riot.fail_429_times = 2
    async with fake_api.client() as client:
        account = await client.account_by_riot_id("europe", "Player0", "EUW")
    assert account["puuid"] == world.player("p0").puuid
    assert len(fake_api.riot.requests) == 3
    assert client.rate_limited["account-v1.getByRiotId"] == 2


async def test_persistent_429_raises_with_status_429(fake_api: FakeApi) -> None:
    fake_api.riot.fail_429_times = 10
    async with fake_api.client(max_retries=2) as client:
        with pytest.raises(RiotApiError, match="Rate limited too many times") as exc_info:
            await client.account_by_riot_id("europe", "Player0", "EUW")
    assert exc_info.value.status == 429
    assert client.rate_limited["account-v1.getByRiotId"] == 3


async def test_503_is_retried_with_exponential_backoff(
    fake_api: FakeApi, world: World, sleeps: list[float]
) -> None:
    fake_api.riot.fail_500_times = 2
    async with fake_api.client() as client:
        account = await client.account_by_riot_id("europe", "Player0", "EUW")
    assert account["puuid"] == world.player("p0").puuid
    assert len(fake_api.riot.requests) == 3
    assert len(sleeps) == 2
    assert 1.0 <= sleeps[0] <= 2.0
    assert 2.0 <= sleeps[1] <= 4.0


async def test_persistent_503_raises_after_max_retries(
    fake_api: FakeApi, sleeps: list[float]
) -> None:
    fake_api.riot.fail_500_times = 10
    async with fake_api.client(max_retries=2) as client:
        with pytest.raises(RiotApiError, match=r"Giving up after 3 attempts \(HTTP 503\)"):
            await client.account_by_riot_id("europe", "Player0", "EUW")
    assert len(fake_api.riot.requests) == 3  # the first try plus max_retries retries
    assert len(sleeps) == 2


async def test_exhausted_5xx_retries_keep_the_http_status(
    fake_api: FakeApi, sleeps: list[float]
) -> None:
    fake_api.riot.fail_500_times = 10
    async with fake_api.client(max_retries=2) as client:
        with pytest.raises(RiotApiError) as exc_info:
            await client.account_by_riot_id("europe", "Player0", "EUW")
    assert exc_info.value.status == 503


@pytest.mark.parametrize(
    ("kwargs", "query", "expected"),
    [
        pytest.param({}, {"start": ["0"], "count": ["100"]}, PLAYER0_ALL, id="defaults"),
        pytest.param(
            {"queue": 420, "start": 1, "count": 2},
            {"queue": ["420"], "start": ["1"], "count": ["2"]},
            ["EUW1_7100000027", "EUW1_7100000017"],
            id="queue-and-paging",
        ),
        pytest.param(
            {"match_type": "ranked", "start_time": 1_760_000_000, "end_time": 1_760_100_000},
            {
                "type": ["ranked"],
                "startTime": ["1760000000"],
                "endTime": ["1760100000"],
                "start": ["0"],
                "count": ["100"],
            },
            PLAYER0_ALL,
            id="time-window",
        ),
    ],
)
async def test_match_ids_sends_set_filters_as_query_params(
    fake_api: FakeApi,
    world: World,
    kwargs: dict[str, Any],
    query: dict[str, list[str]],
    expected: list[str],
) -> None:
    puuid = world.player("p0").puuid
    async with fake_api.client() as client:
        ids = await client.match_ids("europe", puuid, **kwargs)
    (path,) = fake_api.riot.requests
    split = urlsplit(path)
    assert split.path == f"/europe/lol/match/v5/matches/by-puuid/{puuid}/ids"
    assert parse_qs(split.query) == query
    assert ids == expected


async def test_app_rate_limit_header_replaces_default_limits(fake_api: FakeApi) -> None:
    async with fake_api.client() as client:
        assert client._app_limiter("europe").limits == DEFAULT_APP_LIMITS
        await client.account_by_riot_id("europe", "Player0", "EUW")
        assert client._app_limiters["europe"].limits == ((100000, 1.0),)
        method_limiter = client._method_limiters[("europe", "account-v1.getByRiotId")]
        assert method_limiter.limits == ((2000, 10.0),)


def test_empty_api_key_is_rejected() -> None:
    with pytest.raises(ValueError, match="RIOT_API_KEY"):
        RiotClient("")


async def test_request_outside_context_manager_raises(fake_api: FakeApi) -> None:
    client = fake_api.client()
    with pytest.raises(RuntimeError, match="async context manager"):
        await client.request(
            "europe", "account-v1.getByRiotId", "/riot/account/v1/accounts/by-riot-id/Player0/EUW"
        )
    assert fake_api.riot.requests == []


async def test_truncated_response_is_retried(
    fake_api: FakeApi, world: World, sleeps: list[float]
) -> None:
    fake_api.riot.truncate_times = 1
    async with fake_api.client() as client:
        account = await client.account_by_riot_id("europe", "Player0", "EUW")
    assert account["puuid"] == world.player("p0").puuid
    assert len(fake_api.riot.requests) == 2
    assert len(sleeps) == 1
