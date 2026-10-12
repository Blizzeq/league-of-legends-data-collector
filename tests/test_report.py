import math
import warnings
from pathlib import Path
from typing import Any

import openpyxl
import pandas as pd
import pytest

from lol_collector.report import ReportError, ReportOptions, build_report, load_dataset
from lol_collector.report import analytics as an
from lol_collector.storage import Store

SHEETS = ["Overview", "Champions", "Matchups", "Roles", "Objectives", "Items", "Data", "Glossary"]

PARTICIPANT_DEFAULTS: dict[str, Any] = {
    "puuid": None,
    "riot_id": None,
    "position": "MID",
    "kills": 4,
    "deaths": 2,
    "assists": 6,
    "kda": 5.0,
    "cs_per_min": 6.0,
    "gold_per_min": 400.0,
    "damage_per_min": 700.0,
    "damage_share": 0.2,
    "kill_participation": 0.5,
    "vision_score_per_min": 1.0,
    "gold_diff_at_15": 0.0,
    "game_duration_s": 1800,
    "game_start_utc": pd.NaT,
    "solo_tier": None,
    "rank_snapshot_utc": pd.NaT,
}
TEAM_DEFAULTS: dict[str, Any] = {
    "team_id": 100,
    "win": False,
    "bans": None,
    "first_blood": False,
    "first_tower": False,
    "first_dragon": False,
    "first_herald": False,
    "first_baron": False,
    "first_inhibitor": False,
    "void_grubs": 0,
    "dragons": 0,
    "atakhan": None,
    "dragon_soul": None,
    "gold_diff_at_15": 0.0,
}


def _row(match: str, champion: str, win: bool, **extra: Any) -> dict[str, Any]:
    return {"match_id": match, "champion_name": champion, "win": win, **extra}


def _dataset(rows: list[dict[str, Any]], teams: list[dict[str, Any]] | None = None) -> an.Dataset:
    participants = pd.DataFrame([{**PARTICIPANT_DEFAULTS, **row} for row in rows])
    team_frame = pd.DataFrame([{**TEAM_DEFAULTS, **team} for team in teams or []])
    return an.Dataset(participants, team_frame, pd.DataFrame())


def _durations(*seconds: int) -> an.Dataset:
    """Two participants per match, so a game must still be counted once."""
    rows = [
        _row(f"m{i}", "Ahri", True, puuid=f"p{i}{k}", game_duration_s=s)
        for i, s in enumerate(seconds)
        for k in range(2)
    ]
    return _dataset(rows)


def _nonzero_players(frame: pd.DataFrame) -> dict[str, int]:
    return {tier: int(n) for tier, n in zip(frame["tier"], frame["players"], strict=True) if n}


def _build(collected_db: Path, out: Path, options: ReportOptions) -> Path:
    with Store(collected_db) as store:
        return build_report(store, out, options)


def _workbook(path: Path) -> openpyxl.Workbook:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)  # openpyxl skips XlsxWriter's extensions
        return openpyxl.load_workbook(path)


# ------------------------------------------------------------------ analytics


def test_wilson_interval_for_fifty_wins_in_a_hundred_games() -> None:
    low, high = an.wilson_interval(50, 100)

    assert low == pytest.approx(0.404, abs=1e-3)
    assert high == pytest.approx(0.596, abs=1e-3)


@pytest.mark.parametrize(
    ("wins", "games", "low", "high"),
    [(0, 10, 0.0, 0.2775), (10, 10, 0.7225, 1.0)],
)
def test_wilson_interval_at_no_wins_and_all_wins(
    wins: int, games: int, low: float, high: float
) -> None:
    assert an.wilson_interval(wins, games) == pytest.approx((low, high), abs=1e-4)


def test_wilson_interval_is_nan_without_games() -> None:
    low, high = an.wilson_interval(0, 0)

    assert math.isnan(low)
    assert math.isnan(high)


def _champion_matches() -> an.Dataset:
    """Four matches. Zed is banned by both teams in m1, which must count as one ban match."""
    return _dataset(
        [
            _row("m1", "Ahri", True),
            _row("m1", "Zed", False),
            _row("m2", "Ahri", False),
            _row("m2", "Jinx", True, position="BOT"),
            _row("m3", "Zed", True),
            _row("m3", "Jinx", False, position="BOT"),
            _row("m4", "Ahri", True),
            _row("m4", "Zed", False),
        ],
        teams=[
            {"match_id": "m1", "team_id": 100, "bans": "Zed, Lee Sin"},
            {"match_id": "m1", "team_id": 200, "bans": "Zed"},
            {"match_id": "m2", "team_id": 100, "bans": "Zed"},
            {"match_id": "m2", "team_id": 200},
            {"match_id": "m3", "team_id": 100},
            {"match_id": "m3", "team_id": 200, "bans": "Ahri"},
            {"match_id": "m4", "team_id": 100, "bans": "Jinx"},
            {"match_id": "m4", "team_id": 200, "bans": "Zed"},
        ],
    )


@pytest.mark.parametrize(
    ("champion", "games", "win_rate", "pick_rate", "ban_rate"),
    [("Ahri", 3, 2 / 3, 0.75, 0.25), ("Zed", 3, 1 / 3, 0.75, 0.75), ("Jinx", 2, 0.5, 0.5, 0.25)],
)
def test_champion_rates_count_each_match_once(
    champion: str, games: int, win_rate: float, pick_rate: float, ban_rate: float
) -> None:
    stats = an.champion_stats(_champion_matches(), min_games=1).set_index("champion")

    row = stats.loc[champion]
    assert row["games"] == games
    assert row["win_rate"] == pytest.approx(win_rate)
    assert row["pick_rate"] == pytest.approx(pick_rate)
    assert row["ban_rate"] == pytest.approx(ban_rate)


def test_champion_stats_drops_champions_below_min_games() -> None:
    # Ahri and Zed have 3 games each, Jinx 2.
    data = _champion_matches()

    assert set(an.champion_stats(data, min_games=2)["champion"]) == {"Ahri", "Zed", "Jinx"}
    assert set(an.champion_stats(data, min_games=3)["champion"]) == {"Ahri", "Zed"}
    assert an.champion_stats(data, min_games=4).empty


def test_tier_distribution_orders_unranked_then_tiers_from_iron_to_challenger() -> None:
    snapshot = pd.Timestamp("2026-01-01", tz="UTC")
    data = _dataset(
        [_row("m1", "Ahri", True, puuid="a", solo_tier="IRON", rank_snapshot_utc=snapshot)]
    )

    frame = an.tier_distribution(data)

    assert frame["tier"].tolist() == [
        "UNRANKED",
        "IRON",
        "BRONZE",
        "SILVER",
        "GOLD",
        "PLATINUM",
        "EMERALD",
        "DIAMOND",
        "MASTER",
        "GRANDMASTER",
        "CHALLENGER",
    ]


def test_tier_distribution_counts_each_player_once_at_latest_snapshot() -> None:
    early = pd.Timestamp("2026-01-01", tz="UTC")
    late = pd.Timestamp("2026-01-05", tz="UTC")
    data = _dataset(
        [
            _row("m1", "Ahri", True, puuid="a", solo_tier="IRON", rank_snapshot_utc=early),
            _row("m2", "Ahri", True, puuid="a", solo_tier="GOLD", rank_snapshot_utc=late),
            _row("m1", "Zed", False, puuid="b", rank_snapshot_utc=early),
            _row("m1", "Jinx", False, puuid="c", solo_tier="CHALLENGER", rank_snapshot_utc=early),
            _row("m1", "Lulu", True, puuid="d", solo_tier="MASTER", rank_snapshot_utc=early),
        ]
    )

    frame = an.tier_distribution(data)

    assert _nonzero_players(frame) == {"UNRANKED": 1, "GOLD": 1, "MASTER": 1, "CHALLENGER": 1}


def test_tier_distribution_prefers_a_row_that_has_a_snapshot() -> None:
    snapshot = pd.Timestamp("2026-01-01", tz="UTC")
    data = _dataset(
        [
            _row("m1", "Ahri", True, puuid="a", solo_tier="GOLD", rank_snapshot_utc=snapshot),
            _row("m2", "Ahri", True, puuid="a"),
            _row(
                "m1", "Jinx", False, puuid="b", solo_tier="CHALLENGER", rank_snapshot_utc=snapshot
            ),
        ]
    )

    assert _nonzero_players(an.tier_distribution(data)) == {"GOLD": 1, "CHALLENGER": 1}


def test_duration_histogram_bins_each_game_once_in_five_minute_bins() -> None:
    # 12:00 and 12:30 land in 10-15; 15:00 starts the 15-20 bin; 19:54 is also 15-20.
    frame = an.duration_histogram(_durations(720, 750, 900, 1194))

    assert frame["length"].tolist() == ["10-15", "15-20"]
    assert frame["games"].tolist() == [2, 2]


def test_duration_histogram_counts_longest_game_exactly_on_a_bin_edge() -> None:
    frame = an.duration_histogram(_durations(1500))

    assert int(frame["games"].sum()) == 1


@pytest.mark.parametrize(
    ("objective", "games", "occurs_in", "win_rate"),
    [("First tower", 3, 1.0, 1 / 3), ("More dragons", 2, 2 / 3, 0.5)],
)
def test_objective_impact_scores_the_team_that_secured_it(
    objective: str, games: int, occurs_in: float, win_rate: float
) -> None:
    data = _dataset(
        [],
        teams=[
            {"match_id": "m1", "team_id": 100, "win": True, "first_tower": True, "dragons": 2},
            {"match_id": "m1", "team_id": 200, "win": False, "dragons": 1},
            {"match_id": "m2", "team_id": 100, "win": False, "first_tower": True, "dragons": 1},
            {"match_id": "m2", "team_id": 200, "win": True, "dragons": 1},
            {"match_id": "m3", "team_id": 100, "win": True, "dragons": 0},
            {"match_id": "m3", "team_id": 200, "win": False, "first_tower": True, "dragons": 2},
        ],
    )

    row = an.objective_impact(data).set_index("objective").loc[objective]

    assert row["games"] == games
    assert row["occurs_in"] == pytest.approx(occurs_in)
    assert row["win_rate"] == pytest.approx(win_rate)


async def test_objective_win_rates_are_proportions(collected_db: Path) -> None:
    with Store(collected_db) as store:
        impact = an.objective_impact(load_dataset(store))

    assert impact["win_rate"].between(0, 1).all()
    assert (impact["games"] > 0).all()


def test_player_rolling_win_rate_averages_over_the_window() -> None:
    start = pd.Timestamp("2026-01-01", tz="UTC")
    rows = [
        _row(
            f"m{i}",
            "Ahri",
            win,
            puuid="me",
            riot_id="Me#EUW",
            game_start_utc=start + pd.Timedelta(days=i),
        )
        for i, win in enumerate([False, True, True, False])
    ]

    profile = an.player_profile(_dataset(rows), "Me#EUW", window=2)

    assert profile is not None
    assert profile.form["rolling_win_rate"].tolist() == pytest.approx([0.0, 0.5, 1.0, 0.5])


async def test_player_profile_is_none_for_unknown_player(collected_db: Path) -> None:
    with Store(collected_db) as store:
        data = load_dataset(store)

    assert an.player_profile(data, "Nobody#EUW") is None


async def test_player_form_starts_at_the_first_game_result(collected_db: Path) -> None:
    with Store(collected_db) as store:
        data = load_dataset(store)
    mine = data.participants[data.participants["riot_id"] == "Player0#EUW"]
    first_game = mine.sort_values("game_start_utc").iloc[0]

    profile = an.player_profile(data, "Player0#EUW")

    assert profile is not None
    assert len(mine) >= 2
    assert profile.form["game"].tolist() == list(range(1, len(mine) + 1))
    assert profile.form["rolling_win_rate"].iloc[0] == float(first_game["win"])


# --------------------------------------------------------------------- workbook


async def test_workbook_has_the_seven_sheets_in_order(collected_db: Path, tmp_path: Path) -> None:
    path = _build(collected_db, tmp_path / "report.xlsx", ReportOptions())

    assert _workbook(path).sheetnames == SHEETS


async def test_player_sheet_comes_second_for_a_known_player(
    collected_db: Path, tmp_path: Path
) -> None:
    path = _build(collected_db, tmp_path / "report.xlsx", ReportOptions(player="Player0#EUW"))

    workbook = _workbook(path)
    assert workbook.sheetnames == ["Overview", "Player", *SHEETS[1:]]
    assert workbook["Player"]["B2"].value == "Player0#EUW"


async def test_unknown_player_is_skipped_without_failing(
    collected_db: Path, tmp_path: Path
) -> None:
    path = _build(collected_db, tmp_path / "report.xlsx", ReportOptions(player="Nobody#EUW"))

    assert _workbook(path).sheetnames == SHEETS


async def test_champions_table_has_win_rate_and_kda_columns(
    collected_db: Path, tmp_path: Path
) -> None:
    path = _build(collected_db, tmp_path / "report.xlsx", ReportOptions())
    with Store(collected_db) as store:
        expected = len(an.champion_stats(load_dataset(store), 10))

    sheet = _workbook(path)["Champions"]
    (table,) = sheet.tables.values()
    header = [column.name for column in table.tableColumns]

    assert {"Win rate", "KDA"} <= set(header)
    assert len(sheet[table.ref]) - 1 == expected


async def test_data_sheet_has_one_row_per_participant(collected_db: Path, tmp_path: Path) -> None:
    path = _build(collected_db, tmp_path / "report.xlsx", ReportOptions())
    with Store(collected_db) as store:
        participants = 10 * store.summary()["matches_collected"]

    sheet = _workbook(path)["Data"]

    assert sheet.max_row - 1 == participants


async def test_data_sheet_keeps_only_the_latest_rows_when_limited(
    collected_db: Path, tmp_path: Path
) -> None:
    path = _build(collected_db, tmp_path / "report.xlsx", ReportOptions(max_data_rows=5))
    with Store(collected_db) as store:
        frame = load_dataset(store).participants
    latest = frame.sort_values(["game_start_utc", "match_id", "participant_id"]).tail(5)

    sheet = _workbook(path)["Data"]
    header = [cell.value for cell in sheet[1]]
    rows = list(sheet.iter_rows(min_row=2, values_only=True))
    kept = [(r[header.index("Match id")], r[header.index("Participant id")]) for r in rows]

    assert kept == [
        (match, int(participant))
        for match, participant in zip(latest["match_id"], latest["participant_id"], strict=True)
    ]


async def test_data_sheet_is_omitted_when_max_data_rows_is_zero(
    collected_db: Path, tmp_path: Path
) -> None:
    path = _build(collected_db, tmp_path / "report.xlsx", ReportOptions(max_data_rows=0))

    assert _workbook(path).sheetnames == [name for name in SHEETS if name != "Data"]


async def test_patch_filter_without_matches_raises_report_error(
    collected_db: Path, tmp_path: Path
) -> None:
    out = tmp_path / "report.xlsx"

    with pytest.raises(ReportError, match="No collected matches"):
        _build(collected_db, out, ReportOptions(patches=["99.99"]))

    assert not out.exists()


def test_matchups_pair_lane_opponents_from_both_sides() -> None:
    rows = []
    for match, blue_wins in (("M1", True), ("M2", True), ("M3", False)):
        for team, champion in ((100, "Darius"), (200, "Garen")):
            rows.append(
                {
                    "match_id": match,
                    "team_id": team,
                    "position": "TOP",
                    "champion_name": champion,
                    "win": blue_wins == (team == 100),
                    "gold_diff_at_15": 300 if team == 100 else -300,
                }
            )
    data = an.Dataset(pd.DataFrame(rows), pd.DataFrame(), pd.DataFrame())

    result = an.matchups(data, min_games=3).set_index(["champion", "opponent"])

    assert result.loc[("Darius", "Garen"), "games"] == 3
    assert result.loc[("Darius", "Garen"), "win_rate"] == pytest.approx(2 / 3)
    assert result.loc[("Garen", "Darius"), "win_rate"] == pytest.approx(1 / 3)
    assert result.loc[("Garen", "Darius"), "gold_diff_at_15"] == -300
    assert an.matchups(data, min_games=4).empty


def test_champion_names_and_bans_are_resolved_by_id() -> None:
    """Rows stored while Data Dragon was down carry internal names and numeric bans."""
    participants = pd.DataFrame(
        {
            "match_id": ["M1", "M2"],
            "participant_id": [1, 1],
            "champion_id": [62, 62],
            "champion_name": ["MonkeyKing", "Wukong"],
            "win": [True, False],
            "position": ["JUNGLE", "JUNGLE"],
            **{c: [1.0, 1.0] for c in ["kills", "deaths", "assists", "cs_per_min"]},
            **{
                c: [0.5, 0.5]
                for c in [
                    "gold_per_min",
                    "damage_per_min",
                    "damage_share",
                    "kill_participation",
                    "vision_score_per_min",
                    "gold_diff_at_15",
                ]
            },
        }
    )
    teams = pd.DataFrame(
        {"match_id": ["M1", "M2"], "bans": ["62", "Wukong"], "ban_ids": ["62", "62"]}
    )
    champions = pd.DataFrame({"champion_id": [62], "name": ["Wukong"]})
    data = an.with_canonical_names(an.Dataset(participants, teams, pd.DataFrame(), champions))

    stats = an.champion_stats(data, min_games=1).set_index("champion")

    assert list(stats.index) == ["Wukong"]
    assert stats.loc["Wukong", "games"] == 2
    assert stats.loc["Wukong", "ban_rate"] == 1.0


def test_gold_lead_share_counts_only_games_with_a_15_minute_frame() -> None:
    teams = pd.DataFrame(
        {
            "match_id": ["M1", "M1", "M2", "M2"],
            "team_id": [100, 200, 100, 200],
            "win": [True, False, True, False],
            "gold_diff_at_15": [5000, -5000, None, None],  # M2 ended before 15:00
            **{c: [None] * 4 for c in an.TEAM_OBJECTIVES.values()},
            **{c: [None] * 4 for c in ["void_grubs", "dragons", "atakhan", "dragon_soul"]},
        }
    )
    data = an.Dataset(pd.DataFrame({"match_id": ["M1", "M2"]}), teams, pd.DataFrame())

    impact = an.objective_impact(data).set_index("objective")

    assert impact.loc["Gold lead at 15:00", "occurs_in"] == 1.0
    assert impact.loc["Gold lead at 15:00", "win_rate"] == 1.0


def test_report_on_an_empty_database_points_to_collect(tmp_path: Path) -> None:
    with Store(tmp_path / "empty.sqlite3") as store, pytest.raises(ReportError, match="collect"):
        build_report(store, tmp_path / "r.xlsx", ReportOptions())


async def test_failed_build_keeps_the_previous_report(
    collected_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "report.xlsx"
    with Store(collected_db) as store:
        build_report(store, out, ReportOptions(max_data_rows=0))
        before = out.read_bytes()

        def interrupted(*args: Any, **kwargs: Any) -> None:
            raise KeyboardInterrupt

        monkeypatch.setattr("lol_collector.report._roles", interrupted)
        with pytest.raises(KeyboardInterrupt):
            build_report(store, out, ReportOptions(max_data_rows=0))

    assert out.read_bytes() == before
    assert list(tmp_path.glob("*.partial")) == []
