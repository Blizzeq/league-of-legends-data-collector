"""Match, team and ranking transforms, checked against synthetic matches and hand-made payloads."""

from __future__ import annotations

import random
from typing import Any

import pytest
from fakeriot import Player, make_match

from lol_collector.columns import PARTICIPANT_COLUMNS
from lol_collector.static_data import StaticData
from lol_collector.transform import (
    MatchFilter,
    game_duration_s,
    mastery_columns,
    participant_rows,
    rank_columns,
    skip_reason,
    team_rows,
)

Json = dict[str, Any]

START_MS = 1_760_000_000_000  # 2025-10-09T08:53:20Z
START_S = START_MS // 1000
FETCHED_AT = "2026-10-12T10:00:00Z"
RANK_KEYS = [
    f"{prefix}_{field}"
    for prefix in ("solo", "flex")
    for field in ("tier", "division", "lp", "wins", "losses")
]
RANK_AND_MASTERY = {
    *RANK_KEYS,
    "rank_snapshot_utc",
    "mastery_level",
    "mastery_points",
    "mastery_last_play_utc",
}

# participant id -> (kills, deaths, assists, minions, neutral minions, gold, damage to champions)
# Blue (1-5) totals: 12 kills, 60,000 gold, 90,000 damage.
# Red (6-10) totals: 11 kills, 52,000 gold, 80,000 damage.
LOBBY = {
    1: (4, 2, 2, 150, 30, 12_000, 27_000),
    2: (2, 4, 3, 40, 0, 9_000, 12_000),
    3: (1, 3, 5, 20, 0, 11_000, 18_000),
    4: (3, 1, 4, 200, 10, 14_000, 21_000),
    5: (2, 0, 3, 10, 0, 14_000, 12_000),
    6: (5, 1, 1, 210, 0, 13_000, 25_000),
    7: (0, 6, 2, 30, 0, 8_000, 10_000),
    8: (3, 2, 4, 100, 5, 12_000, 16_000),
    9: (2, 3, 5, 15, 2, 10_000, 14_000),
    10: (1, 5, 3, 25, 0, 9_000, 15_000),
}
GOLD_AT_15 = {
    1: 4000,
    2: 3500,
    3: 4200,
    4: 4100,
    5: 3800,  # blue total 19,600
    6: 4000,
    7: 3000,
    8: 3900,
    9: 3700,
    10: 3600,  # red total 18,200
}


def _synthetic(duration_s: int = 1_200) -> tuple[Json, Json]:
    members = [Player(f"puuid-{i}", f"Player{i}", "EUW", None, None, 0) for i in range(10)]
    return make_match(
        random.Random(1), "EUW1_1", "EUW1", 420, START_MS, duration_s, "16.19.712.4120", members
    )


def _participant(pid: int, **fields: Any) -> Json:
    base: Json = {
        "participantId": pid,
        "teamId": 100 if pid <= 5 else 200,
        "puuid": f"puuid-{pid}",
        "riotIdGameName": f"Player{pid}",
        "riotIdTagline": "EUW",
        "teamPosition": "",
    }
    return {**base, **fields}


def _lobby() -> list[Json]:
    return [
        _participant(
            pid,
            kills=kills,
            deaths=deaths,
            assists=assists,
            totalMinionsKilled=minions,
            neutralMinionsKilled=neutral,
            goldEarned=gold,
            totalDamageDealtToChampions=damage,
        )
        for pid, (kills, deaths, assists, minions, neutral, gold, damage) in LOBBY.items()
    ]


def _match(
    participants: list[Json],
    *,
    duration_s: int = 1_200,
    queue_id: int = 420,
    teams: list[Json] | None = None,
) -> Json:
    return {
        "metadata": {"matchId": "EUW1_1"},
        "info": {
            "gameStartTimestamp": START_MS,
            "gameEndTimestamp": START_MS + duration_s * 1000,
            "gameDuration": duration_s,
            "gameVersion": "16.19.712.4120",
            "platformId": "EUW1",
            "queueId": queue_id,
            "participants": participants,
            "teams": teams or [],
        },
    }


def _timeline(gold_at_15: dict[int, int], events: list[Json]) -> Json:
    """Frames for minutes 0-15; only minute 15 carries gold, events sit in minute 12."""
    frames = []
    for minute in range(16):
        pframes = {
            str(pid): {
                "totalGold": gold_at_15[pid] if minute == 15 else 0,
                "minionsKilled": 0,
                "jungleMinionsKilled": 0,
                "xp": 0,
            }
            for pid in range(1, 11)
        }
        frames.append(
            {
                "timestamp": minute * 60_000,
                "participantFrames": pframes,
                "events": events if minute == 12 else [],
            }
        )
    return {"info": {"frames": frames}}


def _by_pid(match: Json, timeline: Json | None = None) -> dict[int, Json]:
    return {row["participant_id"]: row for row in participant_rows(match, timeline, StaticData())}


def test_participant_rows_have_exactly_the_collected_columns() -> None:
    match, timeline = _synthetic()
    rows = participant_rows(match, timeline, StaticData())
    assert [row["participant_id"] for row in rows] == list(range(1, 11))
    expected = {column.name for column in PARTICIPANT_COLUMNS} - RANK_AND_MASTERY
    assert all(set(row) == expected for row in rows)


def test_match_context_is_repeated_on_every_row() -> None:
    match, timeline = _synthetic()
    rows = participant_rows(match, timeline, StaticData())
    assert {row["match_id"] for row in rows} == {"EUW1_1"}
    assert {row["game_start_utc"] for row in rows} == {"2025-10-09T08:53:20Z"}
    assert {row["patch"] for row in rows} == {"16.19"}
    assert {row["queue_name"] for row in rows} == {"Ranked Solo/Duo"}


def test_combat_and_farm_math_for_one_player() -> None:
    p1 = _by_pid(_match(_lobby()))[1]
    assert p1["kda"] == 3.0  # (4 kills + 2 assists) / 2 deaths
    assert p1["kill_participation"] == 0.5  # 6 / 12 blue kills
    assert p1["cs"] == 180  # 150 lane minions + 30 neutral
    assert p1["cs_per_min"] == 9.0  # 180 over a 20 minute game
    assert p1["gold_per_min"] == 600.0
    assert p1["gold_share"] == 0.2  # 12,000 / 60,000 blue gold
    assert p1["damage_per_min"] == 1350.0
    assert p1["damage_share"] == 0.3  # 27,000 / 90,000 blue damage


@pytest.mark.parametrize(
    ("pid", "kda", "kill_participation"),
    [
        (5, 5.0, 0.4167),  # zero deaths divides by one
        (7, 0.3333, 0.1818),  # zero kills, six deaths
    ],
)
def test_kda_and_kill_participation_edge_cases(
    pid: int, kda: float, kill_participation: float
) -> None:
    row = _by_pid(_match(_lobby()))[pid]
    assert row["kda"] == kda
    assert row["kill_participation"] == kill_participation


def test_team_shares_sum_to_one() -> None:
    rows = participant_rows(_match(_lobby()), None, StaticData())
    for team_id in (100, 200):
        team = [row for row in rows if row["team_id"] == team_id]
        assert sum(row["gold_share"] for row in team) == pytest.approx(1.0, abs=1e-3)
        assert sum(row["damage_share"] for row in team) == pytest.approx(1.0, abs=1e-3)


@pytest.mark.parametrize(
    ("team_position", "position"),
    [
        ("TOP", "TOP"),
        ("JUNGLE", "JUNGLE"),
        ("MIDDLE", "MID"),
        ("BOTTOM", "BOT"),
        ("UTILITY", "SUPPORT"),
        ("", None),
        (None, None),
    ],
)
def test_position_is_mapped_from_team_position(
    team_position: str | None, position: str | None
) -> None:
    row = _by_pid(_match([_participant(1, teamPosition=team_position)]))[1]
    assert row["position"] == position


@pytest.mark.parametrize(
    ("name_fields", "riot_id"),
    [
        ({"riotIdGameName": "Faker", "riotIdTagline": "KR1", "summonerName": "Old"}, "Faker#KR1"),
        ({"riotIdGameName": None, "riotIdTagline": None, "summonerName": "Legacy"}, "Legacy"),
        ({"riotIdGameName": "", "riotIdTagline": "", "summonerName": ""}, None),
    ],
)
def test_riot_id_or_summoner_name_fallback(name_fields: Json, riot_id: str | None) -> None:
    row = _by_pid(_match([_participant(1, **name_fields)]))[1]
    assert row["riot_id"] == riot_id


def test_laning_values_come_from_the_frame_at_each_minute() -> None:
    match, timeline = _synthetic()
    rows = _by_pid(match, timeline)
    frame = timeline["info"]["frames"][10]["participantFrames"]
    own, enemy = frame["1"], frame["6"]
    assert rows[1]["gold_at_10"] == own["totalGold"]
    assert rows[1]["cs_at_10"] == own["minionsKilled"] + own["jungleMinionsKilled"]
    assert rows[1]["xp_at_10"] == own["xp"]
    assert rows[1]["gold_diff_at_10"] == own["totalGold"] - enemy["totalGold"]


@pytest.mark.parametrize(("own", "enemy"), [(1, 6), (2, 7), (3, 8), (4, 9), (5, 10)])
def test_lane_diffs_are_antisymmetric_between_opponents(own: int, enemy: int) -> None:
    rows = _by_pid(*_synthetic())
    for minute in (10, 15):
        for stat in ("gold", "cs", "xp"):
            key = f"{stat}_diff_at_{minute}"
            assert rows[own][key] == -rows[enemy][key]


@pytest.mark.parametrize(("duration_s", "minute"), [(540, 10), (540, 15), (840, 15)])
def test_laning_is_none_once_the_game_has_ended(duration_s: int, minute: int) -> None:
    match, timeline = _synthetic(duration_s)
    row = _by_pid(match, timeline)[1]
    for stat in ("gold", "cs", "xp"):
        assert row[f"{stat}_at_{minute}"] is None
        assert row[f"{stat}_diff_at_{minute}"] is None


def test_laning_takes_a_frame_stamped_exactly_on_the_minute() -> None:
    frames = [
        {"timestamp": minute * 60_000, "participantFrames": {"1": {"totalGold": minute * 100}}}
        for minute in range(16)
    ]
    row = _by_pid(_match([_participant(1)]), {"info": {"frames": frames}})[1]
    assert (row["gold_at_10"], row["gold_at_15"]) == (1_000, 1_500)


def test_lane_diffs_need_a_unique_opponent() -> None:
    match, timeline = _synthetic()
    # Participant 7 (red JUNGLE) becomes a second red TOP: blue TOP has no unique opponent,
    # and blue JUNGLE loses its red counterpart.
    match["info"]["participants"][6]["teamPosition"] = "TOP"
    rows = _by_pid(match, timeline)
    frames = timeline["info"]["frames"][10]["participantFrames"]
    assert rows[1]["gold_diff_at_10"] is None
    assert rows[1]["gold_at_10"] == frames["1"]["totalGold"]
    assert rows[2]["gold_diff_at_10"] is None
    assert rows[3]["gold_diff_at_10"] == frames["3"]["totalGold"] - frames["8"]["totalGold"]


def test_final_stats_come_from_the_last_frame() -> None:
    timeline = {
        "info": {
            "frames": [
                {"participantFrames": {"1": {"championStats": {"armor": 50, "armorPen": 5}}}},
                {
                    "participantFrames": {
                        "1": {"championStats": {"armor": 90, "armorPen": 9, "magicPen": 3}}
                    }
                },
            ]
        }
    }
    row = _by_pid(_match([_participant(1)]), timeline)[1]
    assert (row["final_armor"], row["final_armor_pen"], row["final_magic_pen"]) == (90, 9, 3)
    assert row["final_magic_resist"] is None


@pytest.mark.parametrize(
    ("info", "seconds"),
    [
        ({"gameDuration": 1_500, "gameEndTimestamp": START_MS + 1_500_000}, 1_500),
        ({"gameDuration": 1_500_999}, 1_500),  # milliseconds before patch 11.20
        ({}, 0),
    ],
)
def test_game_duration_is_seconds_or_milliseconds(info: Json, seconds: int) -> None:
    assert game_duration_s(info) == seconds


@pytest.mark.parametrize(
    ("duration_s", "queue_id", "remake", "flt", "reason"),
    [
        (1_200, 420, False, MatchFilter(), None),
        (1_200, 420, True, MatchFilter(), "remake"),
        (120, 420, True, MatchFilter(), "remake"),  # remake is checked before duration
        (299, 420, False, MatchFilter(), "too short"),
        (300, 420, False, MatchFilter(), None),
        (1_200, 450, False, MatchFilter(queues=frozenset({420, 440})), "queue 450"),
        (1_200, 440, False, MatchFilter(queues=frozenset({420, 440})), None),
        (1_200, 420, False, MatchFilter(start_time_s=START_S + 1), "before start time"),
        (1_200, 420, False, MatchFilter(start_time_s=START_S), None),
        (1_200, 420, False, MatchFilter(end_time_s=START_S - 1), "after end time"),
        (1_200, 420, False, MatchFilter(end_time_s=START_S), None),
    ],
)
def test_skip_reason(
    duration_s: int, queue_id: int, remake: bool, flt: MatchFilter, reason: str | None
) -> None:
    match = _match(_lobby(), duration_s=duration_s, queue_id=queue_id)
    if remake:
        match["info"]["participants"][0]["gameEndedInEarlySurrender"] = True
    assert skip_reason(match, flt) == reason


@pytest.fixture
def team_rows_by_side() -> dict[int, Json]:
    participants = _lobby()
    participants[8].update(championId=157, championName="Yasuo")  # pid 9, not in StaticData
    blue = {
        "teamId": 100,
        "win": True,
        "bans": [
            {"championId": 238, "pickTurn": 3},
            {"championId": -1, "pickTurn": 1},
            {"championId": 999, "pickTurn": 5},
            {"championId": 103, "pickTurn": 2},
            {"championId": 157, "pickTurn": 4},
        ],
        "objectives": {
            "champion": {"first": False, "kills": 12},
            "tower": {"first": True, "kills": 7},
            "inhibitor": {"first": False, "kills": 2},
            "dragon": {"first": True, "kills": 3},
            "riftHerald": {"first": False, "kills": 1},
            "baron": {"first": False, "kills": 0},
            "horde": {"first": True, "kills": 4},
            "atakhan": {"first": False, "kills": 1},
        },
        "feats": {"FIRST_BLOOD": {"featState": 3}, "FIRST_TURRET": {"featState": 0}},
    }
    red = {
        "teamId": 200,
        "win": False,
        "bans": [{"championId": -1, "pickTurn": turn} for turn in range(1, 6)],
        "objectives": {
            "champion": {"first": False, "kills": 11},
            "tower": {"first": False, "kills": 3},
            "dragon": {"first": False, "kills": 2},
        },
    }
    events = [
        {"type": "ELITE_MONSTER_KILL", "teamId": 100, "monsterType": "DRAGON", "name": "Cloud"},
        {"type": "DRAGON_SOUL_GIVEN", "teamId": 200, "name": "Ocean"},
    ]
    match = _match(participants, teams=[blue, red])
    timeline = _timeline(GOLD_AT_15, events)
    static = StaticData(champions={103: "Ahri", 238: "Zed"})
    return {row["team_id"]: row for row in team_rows(match, timeline, static)}


def test_team_objectives_map_onto_columns(team_rows_by_side: dict[int, Json]) -> None:
    blue = team_rows_by_side[100]
    assert (blue["kills"], blue["gold_earned"], blue["damage_to_champions"]) == (
        12,
        60_000,
        90_000,
    )
    assert (blue["towers"], blue["inhibitors"], blue["dragons"], blue["barons"]) == (7, 2, 3, 0)
    assert (blue["heralds"], blue["void_grubs"], blue["atakhan"]) == (1, 4, 1)
    assert (blue["first_blood"], blue["first_tower"], blue["first_dragon"]) == (False, True, True)
    assert (blue["first_herald"], blue["first_baron"], blue["first_inhibitor"]) == (
        False,
        False,
        False,
    )


def test_absent_objectives_are_none(team_rows_by_side: dict[int, Json]) -> None:
    red = team_rows_by_side[200]
    assert (red["towers"], red["dragons"]) == (3, 2)
    assert (red["heralds"], red["void_grubs"], red["atakhan"], red["first_herald"]) == (
        None,
        None,
        None,
        None,
    )


def test_bans_follow_pick_order_and_drop_empty_slots(team_rows_by_side: dict[int, Json]) -> None:
    assert team_rows_by_side[100]["bans"] == "Ahri, Zed, Yasuo, Champion 999"
    assert team_rows_by_side[100]["ban_ids"] == "103,238,157,999"
    assert team_rows_by_side[200]["bans"] is None
    assert team_rows_by_side[200]["ban_ids"] is None


def test_dragon_soul_comes_from_the_timeline_event(team_rows_by_side: dict[int, Json]) -> None:
    assert (team_rows_by_side[100]["dragon_soul"], team_rows_by_side[200]["dragon_soul"]) == (
        None,
        "Ocean",
    )


def test_gold_diff_at_15_is_antisymmetric(team_rows_by_side: dict[int, Json]) -> None:
    blue_diff = team_rows_by_side[100]["gold_diff_at_15"]
    red_diff = team_rows_by_side[200]["gold_diff_at_15"]
    assert (blue_diff, red_diff) == (1_400, -1_400)  # 19,600 vs 18,200 gold


def test_feats_expose_feat_state_including_zero(team_rows_by_side: dict[int, Json]) -> None:
    blue = team_rows_by_side[100]
    assert (blue["feat_first_blood"], blue["feat_first_turret"], blue["feat_epic_monsters"]) == (
        3,
        0,
        None,
    )
    red = team_rows_by_side[200]
    assert (red["feat_first_blood"], red["feat_first_turret"]) == (None, None)


def _entry(queue_type: str, tier: str, rank: str = "II", lp: int = 40) -> Json:
    return {
        "queueType": queue_type,
        "tier": tier,
        "rank": rank,
        "leaguePoints": lp,
        "wins": 10,
        "losses": 9,
    }


def test_rank_columns_pick_solo_and_flex_entries() -> None:
    entries = [
        _entry("RANKED_FLEX_SR", "GOLD"),
        _entry("RANKED_TFT_DOUBLE_UP", "DIAMOND", "I", 80),
        _entry("RANKED_SOLO_5x5", "PLATINUM", "IV", 12),
    ]
    assert rank_columns(entries, FETCHED_AT) == {
        "rank_snapshot_utc": FETCHED_AT,
        "solo_tier": "PLATINUM",
        "solo_division": "IV",
        "solo_lp": 12,
        "solo_wins": 10,
        "solo_losses": 9,
        "flex_tier": "GOLD",
        "flex_division": "II",
        "flex_lp": 40,
        "flex_wins": 10,
        "flex_losses": 9,
    }


@pytest.mark.parametrize("tier", ["MASTER", "GRANDMASTER", "CHALLENGER"])
def test_apex_tiers_have_no_division(tier: str) -> None:
    row = rank_columns([_entry("RANKED_SOLO_5x5", tier, "I", 250)], FETCHED_AT)
    assert (row["solo_tier"], row["solo_division"], row["solo_lp"]) == (tier, None, 250)


def test_rank_columns_handle_empty_input() -> None:
    assert rank_columns([], FETCHED_AT) == {
        "rank_snapshot_utc": FETCHED_AT,
        **dict.fromkeys(RANK_KEYS),
    }


def test_rank_columns_accept_a_generator() -> None:
    entries = [_entry("RANKED_SOLO_5x5", "EMERALD", "III", 5)]
    from_generator = rank_columns((entry for entry in entries), FETCHED_AT)
    assert from_generator["solo_tier"] == "EMERALD"
    assert from_generator == rank_columns(entries, FETCHED_AT)


@pytest.mark.parametrize("last_play_ms", [START_MS, START_MS + 999])
def test_mastery_last_play_is_iso_utc_to_the_second(last_play_ms: int) -> None:
    assert mastery_columns(
        {"championLevel": 7, "championPoints": 54_321, "lastPlayTime": last_play_ms}
    ) == {
        "mastery_level": 7,
        "mastery_points": 54_321,
        "mastery_last_play_utc": "2025-10-09T08:53:20Z",
    }


@pytest.mark.parametrize("mastery", [None, {}])
def test_mastery_columns_are_none_without_mastery(mastery: Json | None) -> None:
    assert mastery_columns(mastery) == {
        "mastery_level": None,
        "mastery_points": None,
        "mastery_last_play_utc": None,
    }
