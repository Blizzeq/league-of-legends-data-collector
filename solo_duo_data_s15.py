import asyncio
import datetime
import csv
import os
import time

import aiohttp
from aiohttp import ClientSession
from aiolimiter import AsyncLimiter

###############################################################################
# 1. MULTIPLE API KEYS & INITIAL PUUIDS
###############################################################################
RIOT_API_KEYS = [
    'RGAPI-6d5ea83a-e046-43a4-b832-0ed1bb26d0a0',
    'RGAPI-826316c2-e5a5-490c-a098-8f5717a1486a',
    'RGAPI-52426db0-99a7-434b-a475-9c6be0e77e7b',
    'RGAPI-6e682541-6c1a-4e71-99a8-cc0eb9725783',
    'RGAPI-be26f079-e148-4b63-81a4-7b774b68aa36'
]

INITIAL_PUUIDS = [
    'gs4SYlmSJ2yP8QXfOhRmzzKnz5B9xRXYe-LZjDcbAzwxAPsr7a9X1MIkvGx6rZS918hX84rWw1S8VA',
    'cqDe59xS5WE0zUcXW-llmRZA9mbSKa33yU1jOhKeIOJjTq06TBYksILqatmaJzfjg-_fXZR8zkPrGQ',
    'oPcjKNOXxwYNmPJR4WHeZsO3Bva_yIf_v_WLDbs3MRZLueO_4jJbiVizfDDOh4pt1ekvBWTpf9UwKQ',
    'X6hryZuSfJpK0ghxNCht7LAgjO-gZsK4WPjhSVIhHGwkXcoqPZe0jIZXD7kOw5MMtbE9lTVwv0Y7GQ',
    'ILv7VQBHQlJLJwmNUZyFvskUqOtWFkhlS2Rop1-5utEAN-UimtgJNlHqhB4oAEnEoqZCzjv_w_xWHw'
]

if len(RIOT_API_KEYS) != len(INITIAL_PUUIDS):
    raise ValueError("RIOT_API_KEYS and INITIAL_PUUIDS must have the same length.")

###############################################################################
# 2. GLOBAL CONFIG
###############################################################################
MATCH_REGION_BASE_URL = "https://europe.api.riotgames.com"
BASE_DOMAIN = "eun1.api.riotgames.com"

# These global limiters nie są używane – teraz będziemy tworzyć lokalne w każdej parze.
# RATE_LIMIT_1S = AsyncLimiter(20, 1)
# RATE_LIMIT_2M = AsyncLimiter(100, 120)

SEASON15_START_TIMESTAMP = int(datetime.datetime(2025, 1, 9, 0, 0, tzinfo=datetime.timezone.utc).timestamp() * 1000)
MIN_DURATION_SECONDS = 300

CHUNK_SIZE = 50
MAX_ROWS = 200000
MATCH_HISTORY_COUNT = 20

PLATFORM_MAP = {
    "EUW1": "euw1.api.riotgames.com",
    "EUN1": "eun1.api.riotgames.com",
    "NA1": "na1.api.riotgames.com",
    "KR": "kr.api.riotgames.com",
    "TR1": "tr1.api.riotgames.com",
    "RU": "ru.api.riotgames.com",
    "BR1": "br1.api.riotgames.com",
    "LA1": "la1.api.riotgames.com",
    "LA2": "la2.api.riotgames.com",
    "OC1": "oc1.api.riotgames.com",
}


###############################################################################
# 3. DATA DRAGON (ITEM MAPPING)
###############################################################################
async def get_latest_dd_version(session: ClientSession):
    url = "https://ddragon.leagueoflegends.com/api/versions.json"
    async with session.get(url) as resp:
        if resp.status == 200:
            versions = await resp.json()
            return versions[0] if versions else None
        return None


async def load_item_mapping(session: ClientSession, version: str):
    url = f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/item.json"
    async with session.get(url) as resp:
        if resp.status == 200:
            data = await resp.json()
            mapping = {}
            for item_id, details in data.get("data", {}).items():
                mapping[int(item_id)] = details.get("name", f"Item_{item_id}")
            return mapping
        return {}


###############################################################################
# 4. HELPER FUNCTIONS
###############################################################################
def default_numeric(value):
    try:
        return float(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def default_text(value):
    return value if value not in [None, ""] else "N/A"


def get_item_name(item_id, item_map):
    try:
        iid = int(item_id)
    except (TypeError, ValueError):
        return "N/A"
    return item_map.get(iid, f"Item_{iid}")


def map_queue_id(qid):
    return "Ranked Solo/Duo" if qid == 420 else f"Queue_{qid}"


###############################################################################
# 5. RATE LIMIT & TIMEOUT HANDLING WITH LOCAL LIMITERS
###############################################################################
REQUEST_TIMEOUT = 15
MAX_RETRIES = 6


async def do_request(session: ClientSession, url: str, params=None, headers=None, retries=0,
                     local_rl_1s=None, local_rl_2m=None):
    if headers is None:
        headers = {}
    # Use local limiters if provided, else fall back to global ones
    if local_rl_1s is None:
        local_rl_1s = AsyncLimiter(20, 1)
    if local_rl_2m is None:
        local_rl_2m = AsyncLimiter(100, 120)
    while True:
        async with local_rl_1s, local_rl_2m:
            try:
                async with session.get(url, params=params, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 1))
                        print(f"[429] Rate limit hit. Waiting {retry_after}s... URL: {url}")
                        await asyncio.sleep(retry_after)
                        continue
                    elif resp.status in [500, 502, 503, 504]:
                        print(f"[{resp.status}] Server error. 5s backoff (Attempt {retries + 1})... URL: {url}")
                        await asyncio.sleep(5)
                        retries += 1
                        if retries >= MAX_RETRIES:
                            print(f"[ERROR] Max retries for {url}. Skipping.")
                            return None
                        continue
                    else:
                        txt = await resp.text()
                        print(f"[{resp.status}] Unexpected error for {url}: {txt}")
                        return None
            except asyncio.TimeoutError:
                print(f"[TIMEOUT] {url}. Retrying (Attempt {retries + 1}/{MAX_RETRIES})...")
                retries += 1
                if retries < MAX_RETRIES:
                    await asyncio.sleep(5 * (retries + 1))
                    continue
                else:
                    print(f"[ERROR] Max retries (timeout) for {url}. Skipping.")
                    return None


###############################################################################
# 6. SPECIFIC ENDPOINT FUNCTIONS (with local limiters)
###############################################################################
async def get_match_history(session, puuid, api_key, count=MATCH_HISTORY_COUNT,
                            local_rl_1s=None, local_rl_2m=None):
    url = f"{MATCH_REGION_BASE_URL}/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"count": count}
    headers = {"X-Riot-Token": api_key}
    return await do_request(session, url, params=params, headers=headers,
                            local_rl_1s=local_rl_1s, local_rl_2m=local_rl_2m)


async def get_match_details(session, match_id, api_key, local_rl_1s=None, local_rl_2m=None):
    url = f"{MATCH_REGION_BASE_URL}/lol/match/v5/matches/{match_id}"
    headers = {"X-Riot-Token": api_key}
    return await do_request(session, url, headers=headers,
                            local_rl_1s=local_rl_1s, local_rl_2m=local_rl_2m)


async def get_match_timeline(session, match_id, api_key, local_rl_1s=None, local_rl_2m=None):
    url = f"{MATCH_REGION_BASE_URL}/lol/match/v5/matches/{match_id}/timeline"
    headers = {"X-Riot-Token": api_key}
    return await do_request(session, url, headers=headers,
                            local_rl_1s=local_rl_1s, local_rl_2m=local_rl_2m)


async def get_summoner_rank(session, summoner_id, platform, api_key, local_rl_1s=None, local_rl_2m=None):
    base_domain = PLATFORM_MAP.get(platform.upper(), BASE_DOMAIN)
    url = f"https://{base_domain}/lol/league/v4/entries/by-summoner/{summoner_id}"
    headers = {"X-Riot-Token": api_key}
    data = await do_request(session, url, headers=headers,
                            local_rl_1s=local_rl_1s, local_rl_2m=local_rl_2m)
    rank_info = {
        "solo_tier": "N/A", "solo_rank": "N/A", "solo_lp": 0,
        "solo_wins": 0, "solo_losses": 0,
        "flex_tier": "N/A", "flex_rank": "N/A", "flex_lp": 0,
        "flex_wins": 0, "flex_losses": 0
    }
    if isinstance(data, list):
        for entry in data:
            q_type = entry.get("queueType", "")
            tier = entry.get("tier", "N/A")
            rank_ = entry.get("rank", "N/A")
            lp = default_numeric(entry.get("leaguePoints"))
            wins_ = default_numeric(entry.get("wins"))
            losses_ = default_numeric(entry.get("losses"))
            if q_type == "RANKED_SOLO_5x5":
                rank_info["solo_tier"] = tier
                rank_info["solo_rank"] = rank_
                rank_info["solo_lp"] = lp
                rank_info["solo_wins"] = wins_
                rank_info["solo_losses"] = losses_
            elif q_type == "RANKED_FLEX_SR":
                rank_info["flex_tier"] = tier
                rank_info["flex_rank"] = rank_
                rank_info["flex_lp"] = lp
                rank_info["flex_wins"] = wins_
                rank_info["flex_losses"] = losses_
    return rank_info


async def get_champion_mastery_list(session, puuid, api_key, local_rl_1s=None, local_rl_2m=None):
    url = f"https://{BASE_DOMAIN}/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}"
    headers = {"X-Riot-Token": api_key}
    return await do_request(session, url, headers=headers,
                            local_rl_1s=local_rl_1s, local_rl_2m=local_rl_2m)


def extract_champion_mastery(mastery_list, champion_id):
    if not isinstance(mastery_list, list):
        print(f"[WARN] mastery_list is not a list: {mastery_list}")
        return {
            "mastery_level": 0,
            "mastery_points": 0,
            "mastery_lastPlayTime": 0,
            "mastery_pointsSinceLastLevel": 0,
            "mastery_pointsUntilNextLevel": 0,
            "mastery_tokens": 0,
        }
    for item in mastery_list:
        if item.get("championId") == champion_id:
            return {
                "mastery_level": max(0, item.get("championLevel", 0)),
                "mastery_points": max(0, item.get("championPoints", 0)),
                "mastery_lastPlayTime": max(0, item.get("lastPlayTime", 0)),
                "mastery_pointsSinceLastLevel": max(0, item.get("championPointsSinceLastLevel", 0)),
                "mastery_pointsUntilNextLevel": max(0, item.get("championPointsUntilNextLevel", 0)),
                "mastery_tokens": max(0, item.get("tokensEarned", 0)),
            }
    return {
        "mastery_level": 0,
        "mastery_points": 0,
        "mastery_lastPlayTime": 0,
        "mastery_pointsSinceLastLevel": 0,
        "mastery_pointsUntilNextLevel": 0,
        "mastery_tokens": 0,
    }


###############################################################################
# 7. TIMELINE FINAL STATS
###############################################################################
def get_final_champion_stats(timeline_data, participant_id):
    result = {}
    if not timeline_data:
        return result
    frames = timeline_data.get("info", {}).get("frames", [])
    if not frames:
        return result
    last_frame = frames[-1]
    participant_frames = last_frame.get("participantFrames", {})
    fdata = participant_frames.get(str(int(participant_id)), {})
    cstats = fdata.get("championStats", {})
    fields = [
        "abilityHaste", "abilityPower", "armor", "armorPen", "armorPenPercent",
        "attackDamage", "attackSpeed", "bonusArmorPenPercent", "bonusMagicPenPercent",
        "ccReduction", "cooldownReduction", "health", "healthMax", "healthRegen",
        "lifesteal", "magicPen", "magicPenPercent", "magicResist", "movementSpeed",
        "omnivamp", "physicalVamp", "power", "powerMax", "powerRegen", "spellVamp"
    ]
    for field in fields:
        result[field] = default_numeric(cstats.get(field))
    return result


###############################################################################
# 8. PROCESS MATCH DATA
###############################################################################
async def process_match_data(session, match_data, timeline_data, mastery_cache, puuid_pool,
                             api_key, item_map):
    if not match_data:
        return []
    info = match_data.get("info", {})
    queue_id = info.get("queueId")
    if queue_id != 420:
        return []
    timestamp_ms = info.get("gameStartTimestamp")
    if not timestamp_ms or timestamp_ms < SEASON15_START_TIMESTAMP:
        return []
    duration = default_numeric(info.get("gameDuration"))
    if duration < MIN_DURATION_SECONDS:
        return []  # Skip very short matches
    game_id = info.get("gameId")
    dt_utc = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc)
    start_utc = dt_utc.isoformat()
    platform_id = default_text(info.get("platformId"))
    map_id = default_numeric(info.get("mapId"))
    game_mode = default_text(info.get("gameMode"))
    game_version = default_text(info.get("gameVersion"))
    # Gather team-level stats
    team_stats = {}
    for t in info.get("teams", []):
        t_id = t.get("teamId")
        if not t_id:
            continue
        obj = t.get("objectives", {})
        team_stats[t_id] = {
            "team_baronKills": default_numeric(obj.get("baron", {}).get("kills")),
            "team_dragonKills": default_numeric(obj.get("dragon", {}).get("kills")),
            "team_towerKills": default_numeric(obj.get("tower", {}).get("kills")),
            "team_champKills": default_numeric(obj.get("champion", {}).get("kills")),
            "team_riftHeraldKills": default_numeric(obj.get("riftHerald", {}).get("kills")),
            "team_inhibitorKills": default_numeric(obj.get("inhibitor", {}).get("kills")),
        }
    rows = []
    for part in info.get("participants", []):
        puuid = part.get("puuid")
        if puuid:
            puuid_pool.add(puuid)
        participant_id = default_numeric(part.get("participantId"))
        champion_id = default_numeric(part.get("championId"))
        summoner_name = default_text(part.get("summonerName"))
        position = default_text(part.get("individualPosition") or part.get("teamPosition"))
        if position.upper() == "UTILITY":
            position = "SUPPORT"
        is_win = part.get("win", False)
        kills = default_numeric(part.get("kills"))
        deaths = default_numeric(part.get("deaths"))
        assists = default_numeric(part.get("assists"))
        gold_earned = default_numeric(part.get("goldEarned"))
        gold_spent = default_numeric(part.get("goldSpent"))
        dmg_dealt = default_numeric(part.get("totalDamageDealt"))
        dmg_to_champ = default_numeric(part.get("totalDamageDealtToChampions"))
        dmg_taken = default_numeric(part.get("totalDamageTaken"))
        vision_score = default_numeric(part.get("visionScore"))
        # Flatten items
        items = {}
        for i in range(7):
            key = f"item{i}"
            items[key] = get_item_name(part.get(key), item_map)
        # Rank info
        platform = platform_id.upper()
        summoner_id = default_text(part.get("summonerId"))
        rank_data = await get_summoner_rank(session, summoner_id, platform, api_key)
        solo_tier = default_text(rank_data["solo_tier"])
        solo_rank = default_text(rank_data["solo_rank"])
        solo_lp = default_numeric(rank_data["solo_lp"])
        solo_wins = default_numeric(rank_data["solo_wins"])
        solo_losses = default_numeric(rank_data["solo_losses"])
        flex_tier = default_text(rank_data["flex_tier"])
        flex_rank = default_text(rank_data["flex_rank"])
        flex_lp = default_numeric(rank_data["flex_lp"])
        flex_wins = default_numeric(rank_data["flex_wins"])
        flex_losses = default_numeric(rank_data["flex_losses"])
        # Champion mastery
        if puuid not in mastery_cache:
            mastery_cache[puuid] = await get_champion_mastery_list(session, puuid, api_key)
        champ_mastery = extract_champion_mastery(mastery_cache[puuid], champion_id)
        mastery_level = default_numeric(champ_mastery["mastery_level"])
        mastery_points = default_numeric(champ_mastery["mastery_points"])
        mastery_last_play = default_numeric(champ_mastery["mastery_lastPlayTime"])
        mastery_points_since = default_numeric(champ_mastery["mastery_pointsSinceLastLevel"])
        mastery_points_until = default_numeric(champ_mastery["mastery_pointsUntilNextLevel"])
        mastery_tokens = default_numeric(champ_mastery["mastery_tokens"])
        # Final stats from timeline
        final_stats = get_final_champion_stats(timeline_data, participant_id)
        final_abilityHaste = default_numeric(final_stats.get("abilityHaste"))
        final_abilityPower = default_numeric(final_stats.get("abilityPower"))
        final_armor = default_numeric(final_stats.get("armor"))
        final_attackDamage = default_numeric(final_stats.get("attackDamage"))
        final_attackSpeed = default_numeric(final_stats.get("attackSpeed"))
        final_movementSpeed = default_numeric(final_stats.get("movementSpeed"))
        final_health = default_numeric(final_stats.get("health"))
        final_healthMax = default_numeric(final_stats.get("healthMax"))
        final_lifesteal = default_numeric(final_stats.get("lifesteal"))
        final_omnivamp = default_numeric(final_stats.get("omnivamp"))
        final_power = default_numeric(final_stats.get("power"))
        final_powerMax = default_numeric(final_stats.get("powerMax"))
        final_spellVamp = default_numeric(final_stats.get("spellVamp"))
        # Team-level stats
        team_id = part.get("teamId", 0)
        tstats = team_stats.get(team_id, {
            "team_baronKills": 0,
            "team_dragonKills": 0,
            "team_towerKills": 0,
            "team_champKills": 0,
            "team_riftHeraldKills": 0,
            "team_inhibitorKills": 0,
        })
        team_champ_kills = tstats["team_champKills"] if tstats["team_champKills"] > 0 else 1
        # Computed columns
        kda_ratio = (kills + assists) / max(1, deaths)
        kill_participation = (kills + assists) / team_champ_kills
        gold_per_min = gold_earned / (duration / 60.0)
        dmg_per_min = dmg_dealt / (duration / 60.0)
        dmg_champ_per_min = dmg_to_champ / (duration / 60.0)
        row = {
            "game_id": game_id,
            "start_utc": start_utc,
            "duration": duration,
            "queue": map_queue_id(queue_id),
            "platform_id": platform_id,
            "map_id": map_id,
            "game_mode": game_mode,
            "game_version": game_version,
            "participant_id": participant_id,
            "summoner_name": summoner_name,
            "champion_id": champion_id,
            "champion_name": default_text(part.get("championName")),
            "position": position,
            "win": is_win,
            "kills": kills,
            "deaths": deaths,
            "assists": assists,
            "kda_ratio": kda_ratio,
            "kill_participation": kill_participation,
            "gold_earned": gold_earned,
            "gold_spent": gold_spent,
            "gold_per_min": gold_per_min,
            "damage_dealt": dmg_dealt,
            "damage_per_min": dmg_per_min,
            "damage_to_champ": dmg_to_champ,
            "damage_champ_per_min": dmg_champ_per_min,
            "damage_taken": dmg_taken,
            "vision_score": vision_score,
            "item0": items["item0"],
            "item1": items["item1"],
            "item2": items["item2"],
            "item3": items["item3"],
            "item4": items["item4"],
            "item5": items["item5"],
            "item6": items["item6"],
            "solo_tier": solo_tier,
            "solo_rank": solo_rank,
            "solo_lp": solo_lp,
            "solo_wins": solo_wins,
            "solo_losses": solo_losses,
            "flex_tier": flex_tier,
            "flex_rank": flex_rank,
            "flex_lp": flex_lp,
            "flex_wins": flex_wins,
            "flex_losses": flex_losses,
            "mastery_level": mastery_level,
            "mastery_points": mastery_points,
            "mastery_lastPlayTime": mastery_last_play,
            "mastery_pointsSinceLastLevel": mastery_points_since,
            "mastery_pointsUntilNextLevel": mastery_points_until,
            "mastery_tokens": mastery_tokens,
            "final_abilityHaste": final_abilityHaste,
            "final_abilityPower": final_abilityPower,
            "final_armor": final_armor,
            "final_attackDamage": final_attackDamage,
            "final_attackSpeed": final_attackSpeed,
            "final_movementSpeed": final_movementSpeed,
            "final_health": final_health,
            "final_healthMax": final_healthMax,
            "final_lifesteal": final_lifesteal,
            "final_omnivamp": final_omnivamp,
            "final_power": final_power,
            "final_powerMax": final_powerMax,
            "final_spellVamp": final_spellVamp,
            "team_baronKills": tstats["team_baronKills"],
            "team_dragonKills": tstats["team_dragonKills"],
            "team_towerKills": tstats["team_towerKills"],
            "team_champKills": tstats["team_champKills"],
            "team_riftHeraldKills": tstats["team_riftHeraldKills"],
            "team_inhibitorKills": tstats["team_inhibitorKills"],
        }
        rows.append(row)
    return rows


###############################################################################
# 9. SAVE TO CSV
###############################################################################
def save_chunk_to_csv(all_data, total_rows, index):
    """
    Save data to a file named league_data_flat_{index}_{total_rows}.csv.
    Each index is associated with a distinct API key / PUUID pair.
    """
    if not all_data:
        return
    filename = f"league_data_flat_{index}_{total_rows}.csv"
    keys = list(all_data[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_data)
    print(f"[SAVE] Wrote {len(all_data)} rows (cumulative {total_rows}) to file: {filename}")
    prev_count = total_rows - CHUNK_SIZE
    if prev_count > 0:
        prev_filename = f"league_data_flat_{index}_{prev_count}.csv"
        if os.path.exists(prev_filename):
            try:
                os.remove(prev_filename)
                print(f"Removed previous file: {prev_filename}")
            except PermissionError:
                print(f"[WARN] Could not remove locked file: {prev_filename}. Skipping removal.")
        else:
            print(f"[INFO] Previous file {prev_filename} not found. Skipping removal.")


###############################################################################
# 10. GATHER DATA FOR ONE (API_KEY, PUUID) PAIR
###############################################################################
async def gather_data_for_key(session, api_key, initial_puuid, index):
    """
    Gathers data starting from a specific initial PUUID using a specific API key.
    Saves data in separate CSV files named league_data_flat_{index}_{total_rows}.csv.
    Each task uses its own local rate limiters.
    """
    # Create local rate limiters for this task
    local_rl_1s = AsyncLimiter(20, 1)
    local_rl_2m = AsyncLimiter(100, 120)

    dd_version = await get_latest_dd_version(session)
    if dd_version:
        item_map = await load_item_mapping(session, dd_version)
        print(f"[INFO] [Key#{index}] Loaded item map for version {dd_version}, total items: {len(item_map)}")
    else:
        print(f"[WARN] [Key#{index}] Could not load Data Dragon version; item map is empty.")
        item_map = {}
    all_data = []
    processed_matches = set()
    mastery_cache = {}
    puuid_pool = {initial_puuid}
    total_rows = 0
    rows_since_last_save = 0
    while total_rows < MAX_ROWS and puuid_pool:
        current_puuid = puuid_pool.pop()
        print(f"[INFO] [Key#{index}] Fetching match history for PUUID: {current_puuid}")
        match_ids = await get_match_history(session, current_puuid, api_key, MATCH_HISTORY_COUNT,
                                            local_rl_1s, local_rl_2m)
        if not match_ids:
            print(f"[WARN] [Key#{index}] No match_ids for {current_puuid} or error while fetching.")
            continue
        for match_id in match_ids:
            if match_id in processed_matches:
                continue
            processed_matches.add(match_id)
            print(f"[INFO] [Key#{index}] Processing match {match_id}")
            match_details = await get_match_details(session, match_id, api_key, local_rl_1s, local_rl_2m)
            if match_details:
                timeline = await get_match_timeline(session, match_id, api_key, local_rl_1s, local_rl_2m)
                new_rows = await process_match_data(session, match_details, timeline,
                                                    mastery_cache, puuid_pool, api_key, item_map)
                for row in new_rows:
                    all_data.append(row)
                    total_rows += 1
                    rows_since_last_save += 1
                    if rows_since_last_save >= CHUNK_SIZE:
                        save_chunk_to_csv(all_data, total_rows, index)
                        rows_since_last_save = 0
                    if total_rows >= MAX_ROWS:
                        break
            if total_rows >= MAX_ROWS:
                break
    if all_data and (total_rows % CHUNK_SIZE != 0):
        save_chunk_to_csv(all_data, total_rows, index)
    print(f"[DONE] [Key#{index}] Data collection complete.")
    print(f"[Key#{index}] Collected a total of {total_rows} rows.")


###############################################################################
# 11. MAIN - RUN ALL (API_KEY, PUUID) PAIRS
###############################################################################
async def main():
    tasks = []
    async with ClientSession() as session:
        for i, (api_key, initial_puuid) in enumerate(zip(RIOT_API_KEYS, INITIAL_PUUIDS)):
            tasks.append(gather_data_for_key(session, api_key, initial_puuid, i))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
