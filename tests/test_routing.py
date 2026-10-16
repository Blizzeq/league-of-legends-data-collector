import pytest

from lol_collector.routing import (
    PLATFORM_TO_REGION,
    UnknownPlatformError,
    account_region,
    match_region,
    normalize_platform,
    platform_from_match_id,
)

DOCUMENTED: dict[str, tuple[str, ...]] = {
    "europe": ("EUW1", "EUN1", "TR1", "RU", "ME1"),
    "americas": ("NA1", "BR1", "LA1", "LA2"),
    "asia": ("KR", "JP1"),
    "sea": ("OC1", "SG2", "TW2", "VN2", "PH2", "TH2"),
}
ALL_PLATFORMS = [platform for platforms in DOCUMENTED.values() for platform in platforms]
MATCH_REGION_CASES = [
    (platform, region) for region, platforms in DOCUMENTED.items() for platform in platforms
]


@pytest.mark.parametrize(("platform", "region"), MATCH_REGION_CASES)
def test_match_region_is_the_documented_cluster(platform: str, region: str) -> None:
    assert match_region(platform) == region


def test_known_platforms_are_exactly_the_documented_ones() -> None:
    assert set(PLATFORM_TO_REGION) == set(ALL_PLATFORMS)


@pytest.mark.parametrize("platform", ALL_PLATFORMS)
def test_account_region_is_never_sea(platform: str) -> None:
    assert account_region(platform) in {"americas", "asia", "europe"}


@pytest.mark.parametrize("platform", DOCUMENTED["sea"])
def test_sea_platforms_look_up_accounts_through_asia(platform: str) -> None:
    assert account_region(platform) == "asia"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("euw1", "EUW1"), ("Euw1", "EUW1"), ("  kr ", "KR"), ("jp1", "JP1")],
)
def test_normalize_platform_ignores_case_and_spaces(raw: str, expected: str) -> None:
    assert normalize_platform(raw) == expected


@pytest.mark.parametrize("raw", ["euw9", "europe", ""])
def test_normalize_platform_rejects_unknown_and_lists_known_platforms(raw: str) -> None:
    known = ", ".join(sorted(ALL_PLATFORMS))
    with pytest.raises(UnknownPlatformError) as excinfo:
        normalize_platform(raw)
    assert str(excinfo.value) == f"Unknown platform '{raw}'. Known platforms: {known}"


@pytest.mark.parametrize(
    ("match_id", "platform"),
    [("EUW1_7123456789", "EUW1"), ("KR_6543210", "KR"), ("TH2_123", "TH2")],
)
def test_platform_from_match_id_returns_the_prefix(match_id: str, platform: str) -> None:
    assert platform_from_match_id(match_id) == platform


@pytest.mark.parametrize("match_id", ["EUW17123456789", "", "123"])
def test_platform_from_match_id_rejects_ids_without_separator(match_id: str) -> None:
    with pytest.raises(ValueError, match="Malformed match ID"):
        platform_from_match_id(match_id)


@pytest.mark.parametrize("match_id", ["XYZ_1", "_123"])
def test_platform_from_match_id_rejects_unknown_prefix(match_id: str) -> None:
    with pytest.raises(ValueError, match="Unknown platform"):
        platform_from_match_id(match_id)
