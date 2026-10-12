"""Turn raw match-v5 payloads into flat rows. Pure functions, no I/O."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .static_data import POSITIONS, StaticData, patch_of

Row = dict[str, Any]
Json = dict[str, Any]

SIDES = {100: "blue", 200: "red"}
APEX_TIERS = {"MASTER", "GRANDMASTER", "CHALLENGER"}
LANE_MINUTES = (10, 15)
FINAL_STATS = {
    "final_ability_haste": "abilityHaste",
    "final_ability_power": "abilityPower",
    "final_attack_damage": "attackDamage",
    "final_attack_speed": "attackSpeed",
    "final_armor": "armor",
    "final_magic_resist": "magicResist",
    "final_armor_pen": "armorPen",
    "final_magic_pen": "magicPen",
    "final_health_max": "healthMax",
    "final_movement_speed": "movementSpeed",
    "final_lifesteal": "lifesteal",
    "final_omnivamp": "omnivamp",
}


@dataclass(frozen=True)
class MatchFilter:
    """Client-side checks applied after a match is downloaded.

    Queue and time are also sent to the match list endpoint, so these mostly catch
    remakes and very short games.
    """

    queues: frozenset[int] | None = None
    min_duration_s: int = 300
    start_time_s: int | None = None
    end_time_s: int | None = None


def iso_utc(epoch_ms: int | float | None) -> str | None:
    if not epoch_ms:
        return None
    moment = datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def game_duration_s(info: Json) -> int:
    """Game length in seconds.

    ``gameDuration`` is in seconds when ``gameEndTimestamp`` is present (patch 11.20+)
    and in milliseconds before that.
    """
    duration = int(info.get("gameDuration") or 0)
    return duration if "gameEndTimestamp" in info else duration // 1000


def skip_reason(match: Json, flt: MatchFilter) -> str | None:
    """Why a match should not be stored, or ``None`` if it passes the filter."""
    info = match.get("info") or {}
    participants = info.get("participants") or []
    if len(participants) < 2:
        return "no participants"
    if flt.queues is not None and info.get("queueId") not in flt.queues:
        return f"queue {info.get('queueId')}"
    start_s = (info.get("gameStartTimestamp") or 0) // 1000
    if flt.start_time_s is not None and start_s < flt.start_time_s:
        return "before start time"
    if flt.end_time_s is not None and start_s > flt.end_time_s:
        return "after end time"
    if any(p.get("gameEndedInEarlySurrender") for p in participants):
        return "remake"
    if game_duration_s(info) < flt.min_duration_s:
        return "too short"
    return None


def _ratio(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def _per_min(value: float | None, duration_s: int) -> float | None:
    if value is None or duration_s <= 0:
        return None
    return round(value / (duration_s / 60), 4)


def match_context(match: Json, static: StaticData) -> Row:
    info = match["info"]
    queue_id = info.get("queueId")
    return {
        "match_id": match["metadata"]["matchId"],
        "platform_id": info.get("platformId"),
        "queue_id": queue_id,
        "queue_name": static.queue_name(queue_id),
        "game_start_utc": iso_utc(info.get("gameStartTimestamp")),
        "game_duration_s": game_duration_s(info),
        "game_version": info.get("gameVersion"),
        "patch": patch_of(info.get("gameVersion")),
    }


def match_row(match: Json, status: str, reason: str | None, collected_at: str) -> Row:
    info = match.get("info") or {}
    return {
        "match_id": (match.get("metadata") or {}).get("matchId"),
        "platform_id": info.get("platformId"),
        "queue_id": info.get("queueId"),
        "game_start_utc": iso_utc(info.get("gameStartTimestamp")),
        "game_duration_s": game_duration_s(info) if info else None,
        "patch": patch_of(info.get("gameVersion")),
        "status": status,
        "skip_reason": reason,
        "collected_at_utc": collected_at,
    }


# --------------------------------------------------------------------- timeline


def _frame_at(timeline: Json | None, minute: int) -> Json | None:
    """First frame at or after ``minute``; ``None`` if the game ended earlier."""
    if not timeline:
        return None
    target = minute * 60_000
    frame: Json
    for frame in timeline.get("info", {}).get("frames", []):
        if frame.get("timestamp", 0) >= target:
            return frame
    return None


def _participant_frame(frame: Json | None, participant_id: int) -> Json | None:
    if frame is None:
        return None
    pframe: Json | None = frame.get("participantFrames", {}).get(str(participant_id))
    return pframe


def _frame_values(pframe: Json | None) -> tuple[int, int, int] | None:
    if pframe is None:
        return None
    cs = int(pframe.get("minionsKilled", 0)) + int(pframe.get("jungleMinionsKilled", 0))
    return int(pframe.get("totalGold", 0)), cs, int(pframe.get("xp", 0))


def _lane_opponents(participants: Iterable[Json]) -> dict[int, int]:
    """participantId -> participantId of the enemy with the same assigned role."""
    slots: dict[tuple[int, str], list[int]] = {}
    for p in participants:
        position = p.get("teamPosition")
        if position in POSITIONS:
            slots.setdefault((p["teamId"], position), []).append(p["participantId"])
    opponents = {}
    for (team_id, position), ids in slots.items():
        enemy = slots.get((300 - team_id, position), [])
        if len(ids) == 1 and len(enemy) == 1:
            opponents[ids[0]] = enemy[0]
    return opponents


def laning_columns(timeline: Json | None, participant_id: int, opponent_id: int | None) -> Row:
    row: Row = {}
    for minute in LANE_MINUTES:
        frame = _frame_at(timeline, minute)
        own = _frame_values(_participant_frame(frame, participant_id))
        enemy = (
            _frame_values(_participant_frame(frame, opponent_id))
            if opponent_id is not None
            else None
        )
        for i, stat in enumerate(("gold", "cs", "xp")):
            row[f"{stat}_at_{minute}"] = own[i] if own else None
            row[f"{stat}_diff_at_{minute}"] = own[i] - enemy[i] if own and enemy else None
    return row


def final_stat_columns(timeline: Json | None, participant_id: int) -> Row:
    frames = (timeline or {}).get("info", {}).get("frames", [])
    pframe = _participant_frame(frames[-1], participant_id) if frames else None
    stats = (pframe or {}).get("championStats", {})
    return {column: stats.get(field) for column, field in FINAL_STATS.items()}


def _dragon_souls(timeline: Json | None) -> dict[int, str]:
    souls: dict[int, str] = {}
    for frame in (timeline or {}).get("info", {}).get("frames", []):
        for event in frame.get("events", []):
            if event.get("type") == "DRAGON_SOUL_GIVEN" and event.get("teamId"):
                souls[int(event["teamId"])] = event.get("name")
    return souls


def _team_gold_at(timeline: Json | None, minute: int, participants: list[Json]) -> dict[int, int]:
    frame = _frame_at(timeline, minute)
    totals: dict[int, int] = {}
    if frame is None:
        return totals
    for p in participants:
        values = _frame_values(_participant_frame(frame, p["participantId"]))
        if values:
            totals[p["teamId"]] = totals.get(p["teamId"], 0) + values[0]
    return totals


# ----------------------------------------------------------------- participants


def _riot_id(p: Json) -> str | None:
    name = p.get("riotIdGameName")
    tag = p.get("riotIdTagline")
    if name and tag:
        return f"{name}#{tag}"
    return name or p.get("summonerName") or None


def _runes(p: Json, static: StaticData) -> Row:
    styles = (p.get("perks") or {}).get("styles") or []
    primary = next((s for s in styles if s.get("description") == "primaryStyle"), None)
    secondary = next((s for s in styles if s.get("description") == "subStyle"), None)
    selections = (primary or {}).get("selections") or [{}]
    return {
        "keystone": static.rune_name(selections[0].get("perk")),
        "primary_style": static.rune_name((primary or {}).get("style")),
        "secondary_style": static.rune_name((secondary or {}).get("style")),
    }


def participant_rows(match: Json, timeline: Json | None, static: StaticData) -> list[Row]:
    """One row per player. Rank and mastery columns are added later by the crawler."""
    info = match["info"]
    context = match_context(match, static)
    duration = context["game_duration_s"]
    participants = info["participants"]

    team_kills: dict[int, int] = {}
    team_gold: dict[int, int] = {}
    team_damage: dict[int, int] = {}
    for p in participants:
        t = p["teamId"]
        team_kills[t] = team_kills.get(t, 0) + int(p.get("kills", 0))
        team_gold[t] = team_gold.get(t, 0) + int(p.get("goldEarned", 0))
        team_damage[t] = team_damage.get(t, 0) + int(p.get("totalDamageDealtToChampions", 0))
    opponents = _lane_opponents(participants)

    rows = []
    for p in participants:
        pid = p["participantId"]
        team_id = p["teamId"]
        challenges = p.get("challenges") or {}
        kills, deaths, assists = (int(p.get(k, 0)) for k in ("kills", "deaths", "assists"))
        cs = int(p.get("totalMinionsKilled", 0)) + int(p.get("neutralMinionsKilled", 0))
        gold = int(p.get("goldEarned", 0))
        damage = int(p.get("totalDamageDealtToChampions", 0))
        item_ids = [int(p.get(f"item{i}", 0) or 0) for i in range(7)]

        row: Row = dict(context)
        row.update(
            {
                "participant_id": pid,
                "puuid": p.get("puuid"),
                "riot_id": _riot_id(p),
                "summoner_level": p.get("summonerLevel"),
                "team_id": team_id,
                "side": SIDES.get(team_id),
                "win": bool(p.get("win")),
                "position": POSITIONS.get(p.get("teamPosition") or ""),
                "champion_id": p.get("championId"),
                "champion_name": static.champion_name(p.get("championId"), p.get("championName")),
                "champion_level": p.get("champLevel"),
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "kda": round((kills + assists) / max(1, deaths), 4),
                "kill_participation": _ratio(kills + assists, team_kills[team_id]),
                "solo_kills": challenges.get("soloKills"),
                "largest_multikill": p.get("largestMultiKill"),
                "penta_kills": p.get("pentaKills"),
                "first_blood_kill": bool(p.get("firstBloodKill")),
                "time_dead_s": p.get("totalTimeSpentDead"),
                "cs": cs,
                "cs_per_min": _per_min(cs, duration),
                "gold_earned": gold,
                "gold_per_min": _per_min(gold, duration),
                "gold_share": _ratio(gold, team_gold[team_id]),
                "damage_to_champions": damage,
                "damage_per_min": _per_min(damage, duration),
                "damage_share": _ratio(damage, team_damage[team_id]),
                "physical_damage_to_champions": p.get("physicalDamageDealtToChampions"),
                "magic_damage_to_champions": p.get("magicDamageDealtToChampions"),
                "true_damage_to_champions": p.get("trueDamageDealtToChampions"),
                "damage_taken": p.get("totalDamageTaken"),
                "damage_mitigated": p.get("damageSelfMitigated"),
                "damage_to_buildings": p.get("damageDealtToBuildings"),
                "damage_to_objectives": p.get("damageDealtToObjectives"),
                "heals_on_teammates": p.get("totalHealsOnTeammates"),
                "shields_on_teammates": p.get("totalDamageShieldedOnTeammates"),
                "time_ccing_others_s": p.get("timeCCingOthers"),
                "turret_takedowns": p.get("turretTakedowns"),
                "turret_plates_taken": challenges.get("turretPlatesTaken"),
                "dragon_kills": p.get("dragonKills"),
                "baron_kills": p.get("baronKills"),
                "vision_score": p.get("visionScore"),
                "vision_score_per_min": _per_min(p.get("visionScore"), duration),
                "wards_placed": p.get("wardsPlaced"),
                "wards_killed": p.get("wardsKilled"),
                "control_wards_bought": p.get("visionWardsBoughtInGame"),
                "summoner_spells": " | ".join(
                    filter(
                        None,
                        (
                            static.spell_name(p.get("summoner1Id")),
                            static.spell_name(p.get("summoner2Id")),
                        ),
                    )
                )
                or None,
                **_runes(p, static),
                **{f"item{i}": item_ids[i] for i in range(7)},
                "items": " | ".join(static.item_name(i) for i in item_ids[:6] if i) or None,
                "role_bound_item": p.get("roleBoundItem") or None,
            }
        )
        row.update(laning_columns(timeline, pid, opponents.get(pid)))
        row.update(final_stat_columns(timeline, pid))
        rows.append(row)
    return rows


# ------------------------------------------------------------------------ teams


def team_rows(match: Json, timeline: Json | None, static: StaticData) -> list[Row]:
    info = match["info"]
    context = match_context(match, static)
    participants = info["participants"]
    names_in_game = {p.get("championId"): p.get("championName") for p in participants}
    souls = _dragon_souls(timeline)
    gold_at_15 = _team_gold_at(timeline, 15, participants)

    rows = []
    for team in info.get("teams", []):
        team_id = team["teamId"]
        members = [p for p in participants if p["teamId"] == team_id]
        objectives = team.get("objectives") or {}

        def kills(key: str, objectives: Json = objectives) -> int | None:
            obj = objectives.get(key)
            return obj.get("kills") if obj else None

        def first(key: str, objectives: Json = objectives) -> bool | None:
            obj = objectives.get(key)
            return bool(obj.get("first")) if obj else None

        bans = sorted(team.get("bans") or [], key=lambda b: b.get("pickTurn", 0))
        ban_names = [
            static.champion_name(b["championId"], names_in_game.get(b["championId"]))
            or str(b["championId"])
            for b in bans
            if b.get("championId", -1) > 0
        ]
        enemy_gold = gold_at_15.get(300 - team_id)
        own_gold = gold_at_15.get(team_id)

        row: Row = dict(context)
        row.update(
            {
                "team_id": team_id,
                "side": SIDES.get(team_id),
                "win": bool(team.get("win")),
                "kills": sum(int(p.get("kills", 0)) for p in members),
                "gold_earned": sum(int(p.get("goldEarned", 0)) for p in members),
                "damage_to_champions": sum(
                    int(p.get("totalDamageDealtToChampions", 0)) for p in members
                ),
                "towers": kills("tower"),
                "inhibitors": kills("inhibitor"),
                "dragons": kills("dragon"),
                "barons": kills("baron"),
                "heralds": kills("riftHerald"),
                "void_grubs": kills("horde"),
                "atakhan": kills("atakhan"),
                "first_blood": first("champion"),
                "first_tower": first("tower"),
                "first_dragon": first("dragon"),
                "first_herald": first("riftHerald"),
                "first_baron": first("baron"),
                "first_inhibitor": first("inhibitor"),
                "dragon_soul": souls.get(team_id),
                "feat_first_blood": _feat(team, "FIRST_BLOOD"),
                "feat_first_turret": _feat(team, "FIRST_TURRET"),
                "feat_epic_monsters": _feat(team, "EPIC_MONSTER_KILL"),
                "gold_diff_at_15": (
                    own_gold - enemy_gold
                    if own_gold is not None and enemy_gold is not None
                    else None
                ),
                "bans": ", ".join(ban_names) or None,
            }
        )
        rows.append(row)
    return rows


def _feat(team: Json, key: str) -> int | None:
    feat = (team.get("feats") or {}).get(key)
    return feat.get("featState") if feat else None


# --------------------------------------------------------------- enrichment


def rank_columns(entries: Iterable[Json], fetched_at: str) -> Row:
    entries = list(entries)
    row: Row = {"rank_snapshot_utc": fetched_at}
    for prefix, queue_type in (("solo", "RANKED_SOLO_5x5"), ("flex", "RANKED_FLEX_SR")):
        entry = next((e for e in entries if e.get("queueType") == queue_type), None)
        tier = entry.get("tier") if entry else None
        row[f"{prefix}_tier"] = tier
        row[f"{prefix}_division"] = entry.get("rank") if entry and tier not in APEX_TIERS else None
        row[f"{prefix}_lp"] = entry.get("leaguePoints") if entry else None
        row[f"{prefix}_wins"] = entry.get("wins") if entry else None
        row[f"{prefix}_losses"] = entry.get("losses") if entry else None
    return row


def mastery_columns(mastery: Json | None) -> Row:
    mastery = mastery or {}
    return {
        "mastery_level": mastery.get("championLevel"),
        "mastery_points": mastery.get("championPoints"),
        "mastery_last_play_utc": iso_utc(mastery.get("lastPlayTime")),
    }
