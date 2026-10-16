import pytest

from lol_collector import ratelimit
from lol_collector.ratelimit import RateLimiter, parse_limits


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def sleeps(clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record each delay the limiter asks for and move the fake clock forward instead."""
    requested: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        requested.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(ratelimit.asyncio, "sleep", fake_sleep)
    return requested


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("20:1,100:120", ((20, 1.0), (100, 120.0))),
        (" 20:1 ,  100:120 ", ((20, 1.0), (100, 120.0))),
        ("20:0.5", ((20, 0.5),)),
    ],
)
def test_parse_limits_reads_count_and_window_length(
    header: str, expected: tuple[tuple[int, float], ...]
) -> None:
    assert parse_limits(header) == expected


def test_parse_limits_skips_malformed_fragments_and_keeps_valid_ones() -> None:
    assert parse_limits("20:1,oops,100:x,2.5:10,20,,100:120") == ((20, 1.0), (100, 120.0))


def test_parse_limits_drops_zero_and_negative_windows() -> None:
    assert parse_limits("0:1,20:0,-5:10,30:-1,40:60") == ((40, 60.0),)


@pytest.mark.parametrize("header", [None, "", "   "])
def test_parse_limits_empty_header_yields_no_windows(header: str | None) -> None:
    assert parse_limits(header) == ()


async def test_acquire_is_immediate_while_every_window_has_room(
    clock: FakeClock, sleeps: list[float]
) -> None:
    limiter = RateLimiter(((5, 1.0), (8, 10.0)), margin=0.0, clock=clock)
    for _ in range(5):
        await limiter.acquire()
    assert sleeps == []
    assert clock.now == 0.0


async def test_acquire_waits_for_the_oldest_request_to_leave_a_full_window(
    clock: FakeClock, sleeps: list[float]
) -> None:
    limiter = RateLimiter(((2, 1.0),), margin=0.0, clock=clock)
    for _ in range(3):
        await limiter.acquire()
    assert sleeps == [1.0]
    assert clock.now == 1.0


async def test_slowest_full_window_sets_the_wait(clock: FakeClock, sleeps: list[float]) -> None:
    # The 20-per-second window still has room; the 3-per-10-seconds window is full.
    limiter = RateLimiter(((20, 1.0), (3, 10.0)), margin=0.0, clock=clock)
    starts: list[float] = []
    for _ in range(4):
        await limiter.acquire()
        starts.append(clock.now)
    assert starts == [0.0, 0.0, 0.0, 10.0]
    assert sleeps == [10.0]


async def test_sustained_rate_follows_the_sliding_window(
    clock: FakeClock, sleeps: list[float]
) -> None:
    limiter = RateLimiter(((2, 1.0),), margin=0.0, clock=clock)
    starts: list[float] = []
    for _ in range(6):
        await limiter.acquire()
        starts.append(clock.now)
    assert starts == [0.0, 0.0, 1.0, 1.0, 2.0, 2.0]
    assert sleeps == [1.0, 1.0]


async def test_sleep_adds_the_safety_margin(clock: FakeClock, sleeps: list[float]) -> None:
    limiter = RateLimiter(((1, 1.0),), margin=0.05, clock=clock)
    await limiter.acquire()
    await limiter.acquire()
    assert sleeps == pytest.approx([1.05])


async def test_block_for_delays_the_next_acquire(clock: FakeClock, sleeps: list[float]) -> None:
    limiter = RateLimiter(((20, 1.0),), margin=0.0, clock=clock)
    limiter.block_for(5.0)
    await limiter.acquire()
    assert sleeps == [5.0]
    assert clock.now == 5.0


async def test_block_for_is_absolute_and_never_shortens_a_longer_block(
    clock: FakeClock, sleeps: list[float]
) -> None:
    limiter = RateLimiter(margin=0.0, clock=clock)
    limiter.block_for(10.0)  # blocked until t=10
    clock.now = 4.0
    limiter.block_for(3.0)  # would end at t=7, earlier than the existing block
    await limiter.acquire()
    assert sleeps == [6.0]
    assert clock.now == 10.0


async def test_set_limits_carries_history_into_a_window_of_the_same_length(
    clock: FakeClock, sleeps: list[float]
) -> None:
    limiter = RateLimiter(((3, 10.0),), margin=0.0, clock=clock)
    for _ in range(3):
        await limiter.acquire()
    limiter.set_limits(((5, 10.0),))
    assert limiter.limits == ((5, 10.0),)

    await limiter.acquire()
    await limiter.acquire()
    assert sleeps == []

    await limiter.acquire()
    assert sleeps == [10.0]


async def test_set_limits_counts_each_request_once_per_window(
    clock: FakeClock, sleeps: list[float]
) -> None:
    limiter = RateLimiter(((3, 10.0),), margin=0.0, clock=clock)
    await limiter.acquire()
    limiter.set_limits(((3, 10.0), (5, 10.0)))
    clock.now = 5.0
    # Three requests within 10 seconds fit a limit of 3, so neither call should wait.
    await limiter.acquire()
    await limiter.acquire()
    assert sleeps == []


async def test_acquire_all_dates_the_request_after_every_wait(
    clock: FakeClock, sleeps: list[float]
) -> None:
    """The app slot must not be taken while the request still waits for the method limit."""
    app = RateLimiter(((2, 1.0),), margin=0.0, clock=clock)
    method = RateLimiter(((1, 1.0),), margin=0.0, clock=clock)

    await ratelimit.acquire_all(app, method)
    clock.now = 0.5
    await ratelimit.acquire_all(app, method)  # waits for the method window until t=1.0

    assert sleeps == [0.5]
    assert app.wait_time(1.0) == 0  # one app request at t=0, one at t=1.0: room for another
    assert app.wait_time(1.0 + 1e-9) == 0
