"""Aggregations behind the Excel report. Pure pandas, no formatting."""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

TIER_ORDER = [
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
POSITION_ORDER = ["TOP", "JUNGLE", "MID", "BOT", "SUPPORT"]

TEAM_OBJECTIVES = {
    "First blood": "first_blood",
    "First tower": "first_tower",
    "First dragon": "first_dragon",
    "First Rift Herald": "first_herald",
    "First Baron": "first_baron",
    "First inhibitor": "first_inhibitor",
}


@dataclass
class Dataset:
    participants: pd.DataFrame
    teams: pd.DataFrame
    items: pd.DataFrame
    champions: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def matches(self) -> int:
        return int(self.participants["match_id"].nunique())


def filter_dataset(data: Dataset, patches: list[str], queues: list[int]) -> Dataset:
    def keep(frame: pd.DataFrame) -> pd.DataFrame:
        mask = pd.Series(True, index=frame.index)
        if patches:
            mask &= frame["patch"].isin(patches)
        if queues:
            mask &= frame["queue_id"].isin(queues)
        return frame[mask.fillna(False)].reset_index(drop=True)

    return Dataset(keep(data.participants), keep(data.teams), data.items, data.champions)


def champion_names(data: Dataset) -> dict[int, str]:
    """champion_id -> display name: Data Dragon names first, else the most common stored name.

    Matches collected while Data Dragon was unreachable carry Riot's internal names
    (``MonkeyKing``); mapping by ID keeps one name per champion.
    """
    names: dict[int, str] = {}
    p = data.participants
    if {"champion_id", "champion_name"} <= set(p.columns):
        p = p.dropna(subset=["champion_id", "champion_name"])
    else:
        p = p.iloc[0:0]
    if not p.empty:
        common = p.groupby("champion_id")["champion_name"].agg(_most_common)
        names.update({int(str(k)): str(v) for k, v in common.items() if v is not None})
    if not data.champions.empty:
        names.update(
            zip(data.champions["champion_id"].astype(int), data.champions["name"], strict=True)
        )
    return names


def with_canonical_names(data: Dataset) -> Dataset:
    names = champion_names(data)
    p = data.participants.copy()
    if names and not p.empty:
        mapped = p["champion_id"].map(names).astype("string")
        p["champion_name"] = mapped.fillna(p["champion_name"])
    return Dataset(p, data.teams, data.items, data.champions)


def wilson_interval(wins: float, games: float, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson score interval for a win rate; robust for small samples."""
    if games <= 0:
        return (math.nan, math.nan)
    p = wins / games
    denom = 1 + z**2 / games
    centre = (p + z**2 / (2 * games)) / denom
    half = z * math.sqrt(p * (1 - p) / games + z**2 / (4 * games**2)) / denom
    return (centre - half, centre + half)


def _mean(series: pd.Series) -> float:
    value = series.astype("Float64").mean()
    return float(value) if pd.notna(value) else math.nan


# ---------------------------------------------------------------- overview


def overview(data: Dataset) -> dict[str, Any]:
    p, t = data.participants, data.teams
    blue = t[t["side"] == "blue"]
    start = p["game_start_utc"]
    return {
        "Matches": data.matches,
        "Player rows": len(p),
        "Unique players": int(p["puuid"].nunique()),
        "Champions played": int(p["champion_name"].nunique()),
        "Patches": ", ".join(sorted(p["patch"].dropna().unique(), key=_patch_key)),
        "Platforms": ", ".join(sorted(p["platform_id"].dropna().unique())),
        "Queues": ", ".join(sorted(p["queue_name"].dropna().unique())),
        "First game": start.min(),
        "Last game": start.max(),
        "Average game length (min)": _mean(p.drop_duplicates("match_id")["game_duration_s"]) / 60,
        "Blue side win rate": _mean(blue["win"]) if len(blue) else math.nan,
        "Kills per game": _mean(t.groupby("match_id")["kills"].sum()),
    }


def _patch_key(patch: str) -> tuple[int, ...]:
    return tuple(int(x) if x.isdigit() else 0 for x in str(patch).split("."))


def games_per_patch(data: Dataset) -> pd.DataFrame:
    matches = data.participants.drop_duplicates("match_id")
    counts = matches.groupby("patch").size().rename("games").reset_index()
    counts["order"] = counts["patch"].map(_patch_key)
    return counts.sort_values("order").drop(columns="order").reset_index(drop=True)


def duration_histogram(data: Dataset, bin_minutes: int = 5) -> pd.DataFrame:
    minutes = data.participants.drop_duplicates("match_id")["game_duration_s"].astype(float) / 60
    if minutes.empty:
        return pd.DataFrame({"length": [], "games": []})
    top = int(minutes.max() // bin_minutes + 1) * bin_minutes  # bins are [a, b)
    edges = list(range(0, top + bin_minutes, bin_minutes))
    labels = [f"{a}-{b}" for a, b in itertools.pairwise(edges)]
    bins = pd.cut(minutes, edges, labels=labels, right=False)
    counts = bins.value_counts().reindex(labels, fill_value=0)
    first = next((i for i, c in enumerate(counts) if c), 0)
    return pd.DataFrame({"length": labels[first:], "games": counts.to_numpy()[first:]})


# ----------------------------------------------------------------- champions


def _most_common(series: pd.Series) -> Any:
    modes = series.dropna().mode()
    return modes.iloc[0] if not modes.empty else None


def _ban_counts(teams: pd.DataFrame, names: dict[int, str]) -> pd.Series:
    if "ban_ids" in teams and teams["ban_ids"].notna().any():
        ids = teams["ban_ids"].dropna().astype(str).str.split(",").explode()
        ids = ids[ids.str.strip() != ""]
        bans = ids.map(lambda i: names.get(int(i), f"Champion {i}"))
    else:  # databases collected before ban_ids existed
        bans = teams["bans"].dropna().astype(str).str.split(", ").explode()
        bans = bans[bans != ""]
    # A champion can be banned by both teams in the same match: count matches, not bans.
    frame = pd.DataFrame({"match_id": teams.loc[bans.index, "match_id"], "champion": bans})
    return frame.drop_duplicates().groupby("champion").size()


def _kda(kills: pd.Series, deaths: pd.Series, assists: pd.Series) -> pd.Series:
    """KDA over many games: (kills + assists) / deaths of the totals, as stats sites show it."""
    return (kills + assists) / deaths.clip(lower=1)


def champion_stats(data: Dataset, min_games: int) -> pd.DataFrame:
    p = data.participants
    matches = max(data.matches, 1)
    grouped = p.groupby("champion_name", dropna=True)
    stats = pd.DataFrame(
        {
            "games": grouped.size(),
            "wins": grouped["win"].sum().astype(int),
            "kills": grouped["kills"].mean(),
            "deaths": grouped["deaths"].mean(),
            "assists": grouped["assists"].mean(),
            "cs_per_min": grouped["cs_per_min"].mean(),
            "gold_per_min": grouped["gold_per_min"].mean(),
            "damage_per_min": grouped["damage_per_min"].mean(),
            "damage_share": grouped["damage_share"].mean(),
            "kill_participation": grouped["kill_participation"].mean(),
            "vision_score_per_min": grouped["vision_score_per_min"].mean(),
            "gold_diff_at_15": grouped["gold_diff_at_15"].mean(),
        }
    )
    stats["main_position"] = grouped["position"].agg(_most_common)
    stats["win_rate"] = stats["wins"] / stats["games"]
    intervals = [wilson_interval(w, g) for w, g in zip(stats["wins"], stats["games"], strict=True)]
    stats["win_rate_low"] = [lo for lo, _ in intervals]
    stats["win_rate_high"] = [hi for _, hi in intervals]
    stats["pick_rate"] = stats["games"] / matches
    bans = _ban_counts(data.teams, champion_names(data))
    stats["ban_rate"] = bans.reindex(stats.index).fillna(0) / matches
    stats["kda"] = _kda(
        grouped["kills"].sum().astype(float),
        grouped["deaths"].sum().astype(float),
        grouped["assists"].sum().astype(float),
    )
    stats = stats[stats["games"] >= min_games].reset_index(names="champion")
    columns = [
        "champion", "main_position", "games", "pick_rate", "ban_rate", "win_rate",
        "win_rate_low", "win_rate_high", "kda", "kills", "deaths", "assists",
        "kill_participation", "cs_per_min", "gold_per_min", "damage_per_min",
        "damage_share", "vision_score_per_min", "gold_diff_at_15",
    ]  # fmt: skip
    return stats[columns].sort_values("games", ascending=False).reset_index(drop=True)


# ----------------------------------------------------------------- matchups


def matchups(data: Dataset, min_games: int) -> pd.DataFrame:
    """Champion against the enemy in the same role, from the first champion's side.

    Every pairing appears twice (A vs B and B vs A), so the table can be filtered by
    either champion. Games where a role was not filled exactly once per team are ignored.
    """
    p = data.participants.dropna(subset=["position", "champion_name"])
    slots = p.groupby(["match_id", "position"])["team_id"].transform("size")
    p = p[slots == 2]
    left = p[["match_id", "team_id", "position", "champion_name", "win", "gold_diff_at_15"]]
    right = p[["match_id", "team_id", "position", "champion_name"]].rename(
        columns={"champion_name": "opponent", "team_id": "opponent_team"}
    )
    pairs = left.merge(right, on=["match_id", "position"])
    # Mirror matches are 50% by construction and only add noise.
    pairs = pairs[
        (pairs["team_id"] != pairs["opponent_team"]) & (pairs["champion_name"] != pairs["opponent"])
    ]
    columns = ["position", "champion", "opponent", "games", "win_rate", "win_rate_low",
               "win_rate_high", "gold_diff_at_15"]  # fmt: skip
    if pairs.empty:
        return pd.DataFrame(columns=columns)
    grouped = pairs.groupby(["position", "champion_name", "opponent"])
    frame = pd.DataFrame(
        {
            "games": grouped.size(),
            "wins": grouped["win"].sum().astype(int),
            "gold_diff_at_15": grouped["gold_diff_at_15"].mean(),
        }
    )
    frame = frame[frame["games"] >= min_games]
    frame["win_rate"] = frame["wins"] / frame["games"]
    intervals = [wilson_interval(w, g) for w, g in zip(frame["wins"], frame["games"], strict=True)]
    frame["win_rate_low"] = [lo for lo, _ in intervals]
    frame["win_rate_high"] = [hi for _, hi in intervals]
    frame = frame.reset_index().rename(columns={"champion_name": "champion"})
    order = frame["position"].map({pos: i for i, pos in enumerate(POSITION_ORDER)})
    frame = frame.assign(_order=order).sort_values(
        ["_order", "games", "champion"], ascending=[True, False, True]
    )
    return frame[columns].reset_index(drop=True)


# ----------------------------------------------------------------- positions


def position_stats(data: Dataset) -> pd.DataFrame:
    p = data.participants.dropna(subset=["position"])
    grouped = p.groupby("position")
    stats = pd.DataFrame(
        {
            "games": grouped.size(),
            "kda": _kda(
                grouped["kills"].sum().astype(float),
                grouped["deaths"].sum().astype(float),
                grouped["assists"].sum().astype(float),
            ),
            "kill_participation": grouped["kill_participation"].mean(),
            "cs_per_min": grouped["cs_per_min"].mean(),
            "gold_per_min": grouped["gold_per_min"].mean(),
            "damage_per_min": grouped["damage_per_min"].mean(),
            "damage_share": grouped["damage_share"].mean(),
            "vision_score_per_min": grouped["vision_score_per_min"].mean(),
            "deaths": grouped["deaths"].mean(),
            "solo_kills": grouped["solo_kills"].mean(),
            "champions": grouped["champion_name"].nunique(),
        }
    )
    order = [pos for pos in POSITION_ORDER if pos in stats.index]
    return stats.reindex(order).reset_index(names="position")


# ------------------------------------------------------------------- ranks


def tier_distribution(data: Dataset) -> pd.DataFrame:
    """Unique players by Ranked Solo/Duo tier (latest snapshot per player)."""
    p = data.participants.dropna(subset=["rank_snapshot_utc"])
    if p.empty:
        return pd.DataFrame({"tier": [], "players": [], "share": []})
    p = p.sort_values("rank_snapshot_utc").drop_duplicates("puuid", keep="last")
    tiers = p["solo_tier"].fillna("UNRANKED")
    counts = tiers.value_counts().reindex(["UNRANKED", *TIER_ORDER], fill_value=0)
    frame = counts.rename("players").rename_axis("tier").reset_index()
    frame["share"] = frame["players"] / max(int(frame["players"].sum()), 1)
    return frame


# -------------------------------------------------------------- objectives


def objective_impact(data: Dataset) -> pd.DataFrame:
    t = data.teams
    rows = []
    for label, column in TEAM_OBJECTIVES.items():
        secured = t[t[column].fillna(False).astype(bool)]
        if t[column].notna().any():
            rows.append(_objective_row(label, secured, t, column))
    with_enemy = t.merge(
        t[["match_id", "team_id", "void_grubs", "dragons", "gold_diff_at_15"]].assign(
            team_id=lambda f: 300 - f["team_id"]
        ),
        on=["match_id", "team_id"],
        suffixes=("", "_enemy"),
    )
    if with_enemy["void_grubs"].notna().any():
        more = with_enemy[with_enemy["void_grubs"] > with_enemy["void_grubs_enemy"]]
        rows.append(_objective_row("More Void Grubs", more, t, "void_grubs"))
    if with_enemy["dragons"].notna().any():
        more = with_enemy[with_enemy["dragons"] > with_enemy["dragons_enemy"]]
        rows.append(_objective_row("More dragons", more, t, "dragons"))
    if t["atakhan"].notna().any():
        rows.append(_objective_row("Atakhan", t[t["atakhan"].fillna(0) > 0], t, "atakhan"))
    if t["gold_diff_at_15"].notna().any():
        # Timeline-based rows: only games with a 15:00 frame count (no soul before then).
        timeline = "gold_diff_at_15"
        if t["dragon_soul"].notna().any():
            soul = t[t["dragon_soul"].notna()]
            rows.append(_objective_row("Dragon soul", soul, t, timeline))
        lead = t[t["gold_diff_at_15"] > 0]
        rows.append(_objective_row("Gold lead at 15:00", lead, t, timeline))
        big_lead = t[t["gold_diff_at_15"] > 3000]
        rows.append(_objective_row("Gold lead > 3k at 15:00", big_lead, t, timeline))
    return pd.DataFrame(rows, columns=["objective", "games", "occurs_in", "win_rate"])


def _objective_row(
    label: str, secured: pd.DataFrame, teams: pd.DataFrame, column: str
) -> dict[str, Any]:
    """``occurs_in`` is relative to the games where ``column`` is known."""
    matches = max(int(teams.loc[teams[column].notna(), "match_id"].nunique()), 1)
    return {
        "objective": label,
        "games": len(secured),
        "occurs_in": len(secured) / matches,
        "win_rate": _mean(secured["win"]) if len(secured) else math.nan,
    }


def dragon_souls(data: Dataset) -> pd.DataFrame:
    souls = data.teams.dropna(subset=["dragon_soul"])
    if souls.empty:
        return pd.DataFrame({"soul": [], "games": [], "win_rate": []})
    grouped = souls.groupby("dragon_soul")["win"]
    frame = pd.DataFrame({"games": grouped.size(), "win_rate": grouped.mean().astype(float)})
    return frame.sort_values("games", ascending=False).reset_index(names="soul")


# -------------------------------------------------------------------- items


def item_stats(data: Dataset, min_games: int) -> pd.DataFrame:
    """Completed items (per Data Dragon) in final inventories; empty without item data."""
    columns = ["item", "gold", "games", "pick_rate", "win_rate"]
    p = data.participants
    if data.items.empty:
        return pd.DataFrame(columns=columns)
    slots = [f"item{i}" for i in range(6)]
    long = p[["match_id", "participant_id", "win", *slots]].melt(
        id_vars=["match_id", "participant_id", "win"], value_name="item_id"
    )
    long = long[long["item_id"].fillna(0) > 0].drop_duplicates(
        ["match_id", "participant_id", "item_id"]
    )
    completed = data.items[data.items["completed"].astype(bool)]
    long = long.merge(completed[["item_id", "name", "gold_total"]], on="item_id")
    if long.empty:
        return pd.DataFrame(columns=columns)
    grouped = long.groupby("name")
    frame = pd.DataFrame(
        {
            "gold": grouped["gold_total"].first(),
            "games": grouped.size(),
            "win_rate": grouped["win"].mean().astype(float),
        }
    )
    frame["pick_rate"] = frame["games"] / max(len(p), 1)
    frame = frame[frame["games"] >= min_games].reset_index(names="item")
    return frame[columns].sort_values("games", ascending=False, ignore_index=True)


# ------------------------------------------------------------------ player


@dataclass
class PlayerProfile:
    riot_id: str
    summary: dict[str, Any]
    champions: pd.DataFrame
    recent: pd.DataFrame
    form: pd.DataFrame


def player_profile(data: Dataset, player: str, window: int = 10) -> PlayerProfile | None:
    p = data.participants
    key = player.strip().lower()
    mine = p[(p["riot_id"].str.lower() == key) | (p["puuid"] == player.strip())]
    mine = mine.dropna(subset=["game_start_utc"]).sort_values("game_start_utc")
    if mine.empty:
        return None
    champion_wr = p.groupby("champion_name")["win"].mean()
    grouped = mine.groupby("champion_name")
    champs = pd.DataFrame(
        {
            "games": grouped.size(),
            "win_rate": grouped["win"].mean().astype(float),
            "kda": _kda(
                grouped["kills"].sum().astype(float),
                grouped["deaths"].sum().astype(float),
                grouped["assists"].sum().astype(float),
            ),
            "cs_per_min": grouped["cs_per_min"].mean(),
            "damage_per_min": grouped["damage_per_min"].mean(),
            "gold_diff_at_15": grouped["gold_diff_at_15"].mean(),
        }
    )
    champs["dataset_win_rate"] = champion_wr.reindex(champs.index).astype(float)
    champs = champs.sort_values("games", ascending=False).reset_index(names="champion")

    recent = mine.tail(20).iloc[::-1]
    recent = pd.DataFrame(
        {
            "date": recent["game_start_utc"],
            "champion": recent["champion_name"],
            "position": recent["position"],
            "result": recent["win"].map({True: "Win", False: "Loss"}),
            "kills": recent["kills"],
            "deaths": recent["deaths"],
            "assists": recent["assists"],
            "cs_per_min": recent["cs_per_min"],
            "damage_share": recent["damage_share"],
            "gold_diff_at_15": recent["gold_diff_at_15"],
        }
    ).reset_index(drop=True)

    form = pd.DataFrame(
        {
            "game": range(1, len(mine) + 1),
            "date": mine["game_start_utc"].to_numpy(),
            "rolling_win_rate": mine["win"]
            .astype(float)
            .rolling(window, min_periods=1)
            .mean()
            .to_numpy(),
        }
    )
    position = mine["position"].mode()
    summary = {
        "Games": len(mine),
        "Win rate": _mean(mine["win"]),
        "KDA": (mine["kills"].sum() + mine["assists"].sum()) / max(int(mine["deaths"].sum()), 1),
        "CS per minute": _mean(mine["cs_per_min"]),
        "Damage share": _mean(mine["damage_share"]),
        "Vision score per minute": _mean(mine["vision_score_per_min"]),
        "Gold diff at 15:00": _mean(mine["gold_diff_at_15"]),
        "Main position": position.iloc[0] if not position.empty else None,
    }
    riot_id = str(mine["riot_id"].dropna().iloc[-1]) if mine["riot_id"].notna().any() else player
    return PlayerProfile(riot_id, summary, champs, recent, form)
