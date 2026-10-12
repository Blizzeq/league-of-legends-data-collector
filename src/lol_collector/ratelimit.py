"""Client-side rate limiting that follows the limits Riot reports in response headers.

Riot enforces limits per API key and per routing value (``euw1``, ``europe``, ...),
at two levels: an application limit shared by every endpoint on a routing value and
a method limit per endpoint. Both are advertised as ``"<count>:<seconds>"`` pairs, e.g.
``X-App-Rate-Limit: 20:1,100:120``. The limiter below keeps a sliding log for each
window, which is slightly stricter than Riot's fixed windows and therefore safe.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable, Iterable

Window = tuple[int, float]

# Limits of a Development key. Used until the first response tells us the real ones.
DEFAULT_APP_LIMITS: tuple[Window, ...] = ((20, 1.0), (100, 120.0))


def parse_limits(header: str | None) -> tuple[Window, ...]:
    """Parse a Riot rate limit header such as ``"20:1,100:120"``.

    Malformed fragments are ignored; an empty or missing header yields ``()``.
    """
    if not header:
        return ()
    windows: list[Window] = []
    for part in header.split(","):
        count, _, seconds = part.strip().partition(":")
        try:
            windows.append((int(count), float(seconds)))
        except ValueError:
            continue
    return tuple(w for w in windows if w[0] > 0 and w[1] > 0)


class _SlidingWindow:
    __slots__ = ("limit", "seconds", "stamps")

    def __init__(self, limit: int, seconds: float) -> None:
        self.limit = limit
        self.seconds = seconds
        self.stamps: deque[float] = deque()

    def wait_time(self, now: float) -> float:
        while self.stamps and self.stamps[0] <= now - self.seconds:
            self.stamps.popleft()
        if len(self.stamps) < self.limit:
            return 0.0
        return self.stamps[0] + self.seconds - now


class RateLimiter:
    """Async limiter enforcing several windows at once.

    ``acquire()`` blocks until a request fits into every window. ``set_limits()``
    replaces the windows when Riot reports different values (e.g. a Personal or
    Production key), and ``block_for()`` pauses the limiter after a 429.
    """

    def __init__(
        self,
        windows: Iterable[Window] = (),
        *,
        margin: float = 0.05,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self._margin = margin
        self._lock = asyncio.Lock()
        self._blocked_until = 0.0
        self._windows: dict[Window, _SlidingWindow] = {}
        self.set_limits(windows)

    @property
    def limits(self) -> tuple[Window, ...]:
        return tuple(self._windows)

    def set_limits(self, windows: Iterable[Window]) -> None:
        new = tuple(windows)
        if new == self.limits:
            return
        old = self._windows
        self._windows = {}
        for limit, seconds in new:
            window = _SlidingWindow(limit, seconds)
            # Carry over history from a window of the same length so a limit change
            # does not reset the budget already spent.
            for (_, old_seconds), old_window in old.items():
                if old_seconds == seconds:
                    window.stamps = deque(old_window.stamps)
                    break
            self._windows[(limit, seconds)] = window

    def block_for(self, seconds: float) -> None:
        self._blocked_until = max(self._blocked_until, self._clock() + seconds)

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = self._clock()
                wait = self._blocked_until - now
                for window in self._windows.values():
                    wait = max(wait, window.wait_time(now))
                if wait <= 0:
                    for window in self._windows.values():
                        window.stamps.append(now)
                    return
                await asyncio.sleep(wait + self._margin)
