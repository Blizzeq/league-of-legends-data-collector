"""Platform and regional routing values for League of Legends endpoints.

Platform-routed endpoints (league, champion mastery) live on hosts such as
``euw1.api.riotgames.com``; match-v5 lives on the regional cluster the platform
belongs to; account-v1 accepts any of ``americas``, ``asia`` or ``europe``.
"""

from __future__ import annotations

PLATFORM_TO_REGION: dict[str, str] = {
    "BR1": "americas",
    "LA1": "americas",
    "LA2": "americas",
    "NA1": "americas",
    "JP1": "asia",
    "KR": "asia",
    "EUN1": "europe",
    "EUW1": "europe",
    "ME1": "europe",
    "RU": "europe",
    "TR1": "europe",
    "OC1": "sea",
    "SG2": "sea",
    "TW2": "sea",
    "VN2": "sea",
    # Merged into SG2 on 2025-01-08; kept so older match IDs still route.
    "PH2": "sea",
    "TH2": "sea",
}

# account-v1 is served by americas, asia and europe; any of them can look up any account.
ACCOUNT_REGION: dict[str, str] = {
    "americas": "americas",
    "asia": "asia",
    "europe": "europe",
    "sea": "asia",
}


class UnknownPlatformError(ValueError):
    """Raised for a platform ID the collector does not know how to route."""


def normalize_platform(platform: str) -> str:
    """Return the canonical upper-case platform ID, e.g. ``"euw1"`` -> ``"EUW1"``."""
    value = platform.strip().upper()
    if value not in PLATFORM_TO_REGION:
        known = ", ".join(sorted(PLATFORM_TO_REGION))
        raise UnknownPlatformError(f"Unknown platform '{platform}'. Known platforms: {known}")
    return value


def platform_host(platform: str) -> str:
    """Routing value for platform endpoints, e.g. ``"EUW1"`` -> ``"euw1"``."""
    return normalize_platform(platform).lower()


def match_region(platform: str) -> str:
    """Regional cluster serving match-v5 for a platform."""
    return PLATFORM_TO_REGION[normalize_platform(platform)]


def account_region(platform: str) -> str:
    """Regional cluster to use for account-v1 lookups made on behalf of a platform."""
    return ACCOUNT_REGION[match_region(platform)]


def platform_from_match_id(match_id: str) -> str:
    """Extract the platform from a match ID such as ``"EUW1_7123456789"``."""
    prefix, sep, _ = match_id.partition("_")
    if not sep:
        raise ValueError(f"Malformed match ID: {match_id!r}")
    return normalize_platform(prefix)
