"""Excel report built from a collector database."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from .. import __version__
from ..columns import TABLES
from ..export import read_frame
from ..storage import Store
from . import analytics as an
from .excel import Report

log = logging.getLogger(__name__)

RIOT_DISCLAIMER = (
    "lol-data-collector isn't endorsed by Riot Games and doesn't reflect the views or opinions "
    "of Riot Games or anyone officially involved in producing or managing Riot Games properties. "
    "Riot Games, and all associated properties are trademarks or registered trademarks of "
    "Riot Games, Inc."
)
TOP_N = 15


class ReportError(RuntimeError):
    """The report cannot be built from the selected data."""


@dataclass(frozen=True)
class ReportOptions:
    player: str | None = None
    patches: Sequence[str] = field(default_factory=tuple)
    queues: Sequence[int] = field(default_factory=tuple)
    min_games: int = 10
    max_data_rows: int = 100_000


def load_dataset(store: Store) -> an.Dataset:
    items = pd.read_sql_query(
        "SELECT item_id, name, gold_total, completed FROM items", store.connection()
    )
    return an.Dataset(read_frame(store, "participants"), read_frame(store, "teams"), items)


def build_report(store: Store, out: Path, options: ReportOptions) -> Path:
    data = an.filter_dataset(load_dataset(store), list(options.patches), list(options.queues))
    if data.participants.empty:
        raise ReportError("No collected matches match the report filters.")
    out.parent.mkdir(parents=True, exist_ok=True)
    report = Report(str(out))
    try:
        _overview(report, data, options)
        if options.player:
            profile = an.player_profile(data, options.player)
            if profile is None:
                log.warning("Player %s not found in the data; skipping the player sheet.",
                            options.player)  # fmt: skip
            else:
                _player(report, profile)
        _champions(report, data, options)
        _roles(report, data)
        _objectives(report, data)
        _items(report, data, options)
        if options.max_data_rows:
            _data(report, data, options.max_data_rows)
        _glossary(report)
    finally:
        report.close()
    return out


# ------------------------------------------------------------------ sheets


def _overview(report: Report, data: an.Dataset, options: ReportOptions) -> None:
    ws = report.sheet("Overview")
    ws.set_column(1, 17, 11)
    ws.set_tab_color("#2A78D6")
    stats = an.overview(data)
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    filters = []
    if options.patches:
        filters.append("patches " + ", ".join(options.patches))
    if options.queues:
        filters.append("queues " + ", ".join(map(str, options.queues)))
    subtitle = f"{stats['Queues']} | {stats['Platforms']} | generated {generated}"
    if filters:
        subtitle += " | filtered to " + "; ".join(filters)
    row = report.heading(ws, "League of Legends match report", subtitle)

    row = report.tiles(
        ws,
        row,
        [
            ("Matches", stats["Matches"], "#,##0"),
            ("Unique players", stats["Unique players"], "#,##0"),
            ("Avg game length (min)", stats["Average game length (min)"], "0.0"),
            ("Blue side win rate", stats["Blue side win rate"], "0.0%"),
            ("Kills per game", stats["Kills per game"], "0.0"),
        ],
    )
    row = report.tiles(
        ws,
        row,
        [
            ("First game (UTC)", stats["First game"], "yyyy-mm-dd"),
            ("Last game (UTC)", stats["Last game"], "yyyy-mm-dd"),
            ("Champions played", stats["Champions played"], "#,##0"),
            ("Patches", stats["Patches"] or "-", None),
        ],
    )

    patches = an.games_per_patch(data)
    lengths = an.duration_histogram(data)
    tiers = an.tier_distribution(data)
    chart_row = row + 1
    table_row = chart_row + 34
    ws.write(table_row - 1, 1, "Chart data", report.section)
    table_row += 1

    first = table_row + 1
    report.table(ws, patches, table_row, 1, uniform_width=11)
    if not patches.empty:
        chart = report.chart("column", "Games per patch", sheet=ws.name,
                             categories=(1, first, first + len(patches) - 1),
                             values=(2, first, first + len(patches) - 1))  # fmt: skip
        ws.insert_chart(chart_row, 1, chart)
    report.table(ws, lengths, table_row, 5, formats={"length": "@"}, uniform_width=11)
    if not lengths.empty:
        chart = report.chart("column", "Game length (minutes)", sheet=ws.name,
                             categories=(5, first, first + len(lengths) - 1),
                             values=(6, first, first + len(lengths) - 1))  # fmt: skip
        ws.insert_chart(chart_row, 9, chart)
    report.table(ws, tiers, table_row, 9, uniform_width=11)
    if not tiers.empty:
        chart = report.chart("column", "Players by Solo/Duo tier (at collection time)",
                             sheet=ws.name, categories=(9, first, first + len(tiers) - 1),
                             values=(10, first, first + len(tiers) - 1),
                             size=(1150, 300))  # fmt: skip
        ws.insert_chart(chart_row + 16, 1, chart)

    note_row = table_row + max(len(patches), len(lengths), len(tiers), 1) + 3
    ws.write(note_row, 1, "About this report", report.section)
    notes = [
        f"Built by lol-data-collector {__version__} from Riot Games API data.",
        "Rank and mastery columns are snapshots taken when the data was collected, "
        "not at game time.",
        "Win rate intervals are 95% Wilson score intervals: small samples get wide intervals.",
        RIOT_DISCLAIMER,
    ]
    for i, text in enumerate(notes, start=1):
        ws.merge_range(note_row + i, 1, note_row + i, 15, text, report.wrap)
        ws.set_row(note_row + i, 28 if len(text) > 120 else 15)


def _champions(report: Report, data: an.Dataset, options: ReportOptions) -> None:
    ws = report.sheet("Champions")
    champs = an.champion_stats(data, options.min_games)
    row = report.heading(
        ws, "Champions",
        f"Champions with at least {options.min_games} games, sorted by games played. "
        "Win rate is colour-scaled from 40% (red) through 50% to 60% (blue).",
    )  # fmt: skip
    table_row = row + 17
    first = table_row + 1
    top = min(TOP_N, len(champs))
    if top:
        games_chart = report.chart("bar", f"Most played (top {top})", sheet=ws.name,
                                   categories=(1, first, first + top - 1),
                                   values=(3, first, first + top - 1),
                                   size=(520, 320))  # fmt: skip
        ws.insert_chart(row, 1, games_chart)
        helper_col = 30
        report.write_column(ws, table_row, helper_col, "win rate - 50%",
                            (champs["win_rate"].head(top) - 0.5).tolist())  # fmt: skip
        wr_chart = report.chart("bar", "Win rate vs 50% (same champions)", sheet=ws.name,
                                categories=(1, first, first + top - 1),
                                values=(helper_col, first, first + top - 1),
                                number_format="+0%;-0%;0%", diverging=True,
                                size=(520, 320))  # fmt: skip
        ws.insert_chart(row, 8, wr_chart)
    report.table(
        ws, champs, table_row,
        win_rate_columns=["win_rate"], bar_columns=["pick_rate", "ban_rate"],
        signed_columns=["gold_diff_at_15"],
        empty_message=f"No champion has {options.min_games} games yet. Lower --min-games.",
    )  # fmt: skip
    ws.freeze_panes(table_row + 1, 2)


def _roles(report: Report, data: an.Dataset) -> None:
    ws = report.sheet("Roles")
    row = report.heading(ws, "Roles", "Average per player and game, by assigned role.")
    report.table(ws, an.position_stats(data), row)


def _objectives(report: Report, data: an.Dataset) -> None:
    ws = report.sheet("Objectives")
    impact = an.objective_impact(data)
    row = report.heading(
        ws, "Objectives",
        "How often the team that secured an objective went on to win. "
        "Correlation, not causation: winning teams take more objectives.",
    )  # fmt: skip
    table_row = row + 17
    first = table_row + 1
    if not impact.empty:
        helper_col = 30
        report.write_column(ws, table_row, helper_col, "win rate - 50%",
                            (impact["win_rate"] - 0.5).tolist())  # fmt: skip
        chart = report.chart("bar", "Win rate when secured, vs 50%", sheet=ws.name,
                             categories=(1, first, first + len(impact) - 1),
                             values=(helper_col, first, first + len(impact) - 1),
                             number_format="+0%;-0%;0%", diverging=True,
                             size=(620, 320))  # fmt: skip
        ws.insert_chart(row, 1, chart)
    next_row = report.table(ws, impact, table_row, win_rate_columns=["win_rate"],
                            bar_columns=["occurs_in"])  # fmt: skip
    report.table(ws, an.dragon_souls(data), next_row, title="Dragon souls",
                 win_rate_columns=["win_rate"],
                 empty_message="No dragon souls recorded (timelines disabled?).")  # fmt: skip


def _items(report: Report, data: an.Dataset, options: ReportOptions) -> None:
    ws = report.sheet("Items")
    items = an.item_stats(data, options.min_games)
    row = report.heading(
        ws, "Items",
        "Completed items in final inventories. Pick rate is per player-game.",
    )  # fmt: skip
    table_row = row + 17
    first = table_row + 1
    top = min(TOP_N, len(items))
    if top:
        chart = report.chart("bar", f"Most built (top {top})", sheet=ws.name,
                             categories=(1, first, first + top - 1),
                             values=(4, first, first + top - 1),
                             number_format="0%", size=(620, 320))  # fmt: skip
        ws.insert_chart(row, 1, chart)
    report.table(ws, items, table_row, win_rate_columns=["win_rate"],
                 bar_columns=["pick_rate"])  # fmt: skip
    ws.freeze_panes(table_row + 1, 2)


def _player(report: Report, profile: an.PlayerProfile) -> None:
    ws = report.sheet("Player")
    ws.set_column(1, 17, 11)
    s = profile.summary
    row = report.heading(ws, profile.riot_id, "Games of this player found in the dataset.")
    row = report.tiles(
        ws,
        row,
        [
            ("Games", s["Games"], "#,##0"),
            ("Win rate", s["Win rate"], "0.0%"),
            ("KDA", s["KDA"], "0.00"),
            ("CS per minute", s["CS per minute"], "0.0"),
            ("Damage share", s["Damage share"], "0.0%"),
        ],
    )
    row = report.tiles(
        ws,
        row,
        [
            ("Main role", s["Main position"] or "-", None),
            ("Vision per minute", s["Vision score per minute"], "0.00"),
            ("Gold diff at 15:00", s["Gold diff at 15:00"], "+#,##0;-#,##0;0"),
        ],
    )
    chart_row = row + 1
    helper_col = 30
    first, last = report.write_column(ws, chart_row, helper_col, "game",
                                      profile.form["game"].tolist())  # fmt: skip
    report.write_column(ws, chart_row, helper_col + 1, "rolling win rate",
                        profile.form["rolling_win_rate"].tolist())  # fmt: skip
    chart = report.chart("line", "Win rate, rolling 10 games", sheet=ws.name,
                         categories=(helper_col, first, last),
                         values=(helper_col + 1, first, last),
                         number_format="0%", axis_min=0, axis_max=1,
                         size=(1150, 280))  # fmt: skip
    ws.insert_chart(chart_row, 1, chart)
    row = chart_row + 16
    row = report.table(ws, profile.champions, row, title="Champion pool",
                       win_rate_columns=["win_rate", "dataset_win_rate"],
                       signed_columns=["gold_diff_at_15"], uniform_width=12)  # fmt: skip
    report.table(ws, profile.recent, row, title="Last 20 games",
                 formats={"date": "yyyy-mm-dd", "kills": "0", "deaths": "0", "assists": "0"},
                 signed_columns=["gold_diff_at_15"], uniform_width=12)  # fmt: skip


def _data(report: Report, data: an.Dataset, max_rows: int) -> None:
    ws = report.sheet("Data", dashboard=False)
    frame = data.participants.sort_values(["game_start_utc", "match_id", "participant_id"])
    if len(frame) > max_rows:
        log.info("Data sheet limited to the latest %s of %s rows (use export for all).",
                 f"{max_rows:,}", f"{len(frame):,}")  # fmt: skip
        frame = frame.tail(max_rows)
    patterns = {"pct": "0.0%", "float": "0.00", "datetime": "yyyy-mm-dd hh:mm"}
    formats = {c.name: patterns[c.kind] for c in TABLES["participants"] if c.kind in patterns}
    report.table(ws, frame.reset_index(drop=True), 0, 0, formats=formats)
    ws.freeze_panes(1, 0)


def _glossary(report: Report) -> None:
    ws = report.sheet("Glossary")
    row = report.heading(ws, "Glossary", "Columns of the Data sheet and of the CSV exports.")
    for table in ("participants", "teams"):
        frame = pd.DataFrame(
            [(c.name, c.kind, c.description) for c in TABLES[table]],
            columns=["column", "type", "description"],
        )
        row = report.table(ws, frame, row, title=f"{table} table",
                           widths={"column": 30, "type": 10, "description": 90})  # fmt: skip
