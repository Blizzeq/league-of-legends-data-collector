import asyncio
import datetime
import csv
import os
import json
from aiohttp import ClientSession
from aiolimiter import AsyncLimiter

###############################################################################
# 1. CONFIGURATION
###############################################################################
RIOT_API_KEY = "YOUR_RIOT_API_KEY_HEREa"
MATCH_REGION_BASE_URL = "https://europe.api.riotgames.com"
BASE_DOMAIN = "eun1.api.riotgames.com"

# Only matches that start on or after 9 January 2025 (Season 15)
SEASON15_START_TIMESTAMP = int(datetime.datetime(2025, 1, 9, 0, 0, tzinfo=datetime.timezone.utc).timestamp() * 1000)

# Skip matches under 300 seconds (likely remakes)
MIN_DURATION_SECONDS = 300

CHUNK_SIZE = 50
MAX_ROWS = 200000
MATCH_HISTORY_COUNT = 20
INITIAL_PUUID = "EXAMPLE_PUUID_HERE"

RATE_LIMIT = AsyncLimiter(15, 1.0)
HEADERS = {"X-Riot-Token": RIOT_API_KEY}

PLATFORM_MAP = {
    "EUW1": "euw1.api.riotgames.com",
    "EUN1": "eun1.api.riotgames.com",
    "NA1":  "na1.api.riotgames.com",
    "KR":   "kr.api.riotgames.com",
    "TR1":  "tr1.api.riotgames.com",
    "RU":   "ru.api.riotgames.com",
    "BR1":  "br1.api.riotgames.com",
    "LA1":  "la1.api.riotgames.com",
    "LA2":  "la2.api.riotgames.com",
    "OC1":  "oc1.api.riotgames.com",
}

###############################################################################
# 2. DATA DRAGON FOR ITEM MAPPING
###############################################################################
async def get_latest_dd_version(session: ClientSession):
    url = "https://ddragon.leagueoflegends.com/api/versions.json"
    async with session.get(url) as resp:
        versions = await resp.json()
        return versions[0] if versions else None

async def load_item_mapping(session: ClientSession, version: str):
    url = f"https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/item.json"
    async with session.get(url) as resp:
        data = await resp.json()
        mapping = {}
        for item_id, details in data.get("data", {}).items():
            mapping[int(item_id)] = details.get("name", f"Item_{item_id}")
        return mapping

ITEM_MAP = {}  # global item map

###############################################################################
# 3. HELPER FUNCTIONS
###############################################################################
def default_numeric(value):
    try:
        return float(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0

def default_text(value):
    return value if value not in [None, ""] else "N/A"

def clamp_value(val, min_val=0, max_val=3000000):
    if val < min_val:
        return min_val
    if val > max_val:
        return max_val
    return val

def get_item_name(item_id):
    try:
        item_id = int(item_id)
    except (TypeError, ValueError):
        return "N/A"
    return ITEM_MAP.get(item_id, f"Item_{item_id}")

def map_queue_id(qid):
    return "Ranked Solo/Duo" if qid == 420 else f"Queue_{qid}"

###############################################################################
# 4. ASYNC REQUEST
###############################################################################
async def do_request(session: ClientSession, url: str, method="GET", params=None, headers=None, retries=0, max_retries=5):
    if headers is None:
        headers = {}
    if retries > max_retries:
        print(f"[ERROR] Exceeded max retries for URL: {url}")
        return None
    async with RATE_LIMIT:
        try:
            resp = await session.get(url, params=params, headers=headers)
        except Exception as e:
            print(f"[WARN] Exception {e} for URL: {url} - retrying in 2s...")
            await asyncio.sleep(2)
            return await do_request(session, url, method, params, headers, retries+1)
    if resp.status == 200:
        return resp
    elif resp.status == 429:
        retry_after = int(resp.headers.get("Retry-After", 1))
        print(f"[429] Rate limit reached. Waiting {retry_after}s for URL: {url}")
        await asyncio.sleep(retry_after)
        return await do_request(session, url, method, params, headers, retries+1)
    elif resp.status in [500,502,503,504]:
        print(f"[{resp.status}] Server error for URL: {url}. Waiting 5s...")
        await asyncio.sleep(5)
        return await do_request(session, url, method, params, headers, retries+1)
    else:
        text = await resp.text()
        print(f"[{resp.status}] {text} for URL: {url}")
        return None

###############################################################################
# 5. SPECIFIC ENDPOINT FUNCTIONS
###############################################################################
async def get_match_history(session: ClientSession, puuid: str, count=MATCH_HISTORY_COUNT):
    url = f"{MATCH_REGION_BASE_URL}/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = {"count": count}
    resp = await do_request(session, url, headers=HEADERS, params=params)
    return await resp.json() if resp else []

async def get_match_details(session: ClientSession, match_id: str):
    url = f"{MATCH_REGION_BASE_URL}/lol/match/v5/matches/{match_id}"
    resp = await do_request(session, url, headers=HEADERS)
    return await resp.json() if resp else None

async def get_match_timeline(session: ClientSession, match_id: str):
    url = f"{MATCH_REGION_BASE_URL}/lol/match/v5/matches/{match_id}/timeline"
    resp = await do_request(session, url, headers=HEADERS)
    return await resp.json() if resp else None

async def get_summoner_rank(session: ClientSession, summoner_id: str, platform: str):
    base = PLATFORM_MAP.get(platform.upper(), BASE_DOMAIN)
    url = f"https://{base}/lol/league/v4/entries/by-summoner/{summoner_id}"
    resp = await do_request(session, url, headers=HEADERS)
    rank_info = {
        "solo_tier": "N/A", "solo_rank": "N/A", "solo_lp": 0,
        "solo_wins": 0, "solo_losses": 0,
        "flex_tier": "N/A", "flex_rank": "N/A", "flex_lp": 0,
        "flex_wins": 0, "flex_losses": 0
    }
    if resp:
        data = await resp.json()
        for entry in data:
            q_type = entry.get("queueType")
            if q_type == "RANKED_SOLO_5x5":
                rank_info["solo_tier"] = default_text(entry.get("tier"))
                rank_info["solo_rank"] = default_text(entry.get("rank"))
                rank_info["solo_lp"]   = default_numeric(entry.get("leaguePoints"))
                rank_info["solo_wins"] = default_numeric(entry.get("wins"))
                rank_info["solo_losses"] = default_numeric(entry.get("losses"))
            elif q_type == "RANKED_FLEX_SR":
                rank_info["flex_tier"] = default_text(entry.get("tier"))
                rank_info["flex_rank"] = default_text(entry.get("rank"))
                rank_info["flex_lp"]   = default_numeric(entry.get("leaguePoints"))
                rank_info["flex_wins"] = default_numeric(entry.get("wins"))
                rank_info["flex_losses"] = default_numeric(entry.get("losses"))
    return rank_info

async def get_champion_mastery_list(session: ClientSession, puuid: str):
    url = f"https://{BASE_DOMAIN}/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}"
    resp = await do_request(session, url, headers=HEADERS)
    if not resp:
        return []
    return await resp.json()

def extract_champion_mastery(mastery_list, champion_id):
    for item in mastery_list:
        if item.get("championId") == champion_id:
            lvl = clamp_value(default_numeric(item.get("championLevel")), 0, 20_000)
            pts = clamp_value(default_numeric(item.get("championPoints")), 0, 3_000_000)
            lastp = clamp_value(default_numeric(item.get("lastPlayTime")), 0, 2_000_000_000_0000)
            pSince = clamp_value(default_numeric(item.get("championPointsSinceLastLevel")), 0, 9999999)
            pUntil = clamp_value(default_numeric(item.get("championPointsUntilNextLevel")), 0, 9999999)
            tokens = clamp_value(default_numeric(item.get("tokensEarned")), 0, 50)
            return {
                "mastery_level": lvl,
                "mastery_points": pts,
                "mastery_lastPlayTime": lastp,
                "mastery_pointsSinceLastLevel": pSince,
                "mastery_pointsUntilNextLevel": pUntil,
                "mastery_tokens": tokens,
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
# 6. FINAL CHAMPION STATS FROM TIMELINE
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
    frame_data = participant_frames.get(str(int(participant_id)), {})
    champ_stats = frame_data.get("championStats", {})

    fields = [
        "abilityHaste","abilityPower","armor","armorPen","armorPenPercent",
        "attackDamage","attackSpeed","bonusArmorPenPercent","bonusMagicPenPercent",
        "ccReduction","cooldownReduction","health","healthMax","healthRegen",
        "lifesteal","magicPen","magicPenPercent","magicResist","movementSpeed",
        "omnivamp","physicalVamp","power","powerMax","powerRegen","spellVamp"
    ]
    for field in fields:
        result[field] = default_numeric(champ_stats.get(field))
    return result

###############################################################################
# 7. PROCESS MATCH DATA – INCLUDES TEAM-LEVEL STATS + NEW COLUMNS
###############################################################################
async def process_match_data(session, match_data, timeline_data, mastery_cache):
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
        # skip extremely short matches (likely remake)
        return []

    game_id = info.get("gameId")
    dt_utc = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000.0)
    start_utc = dt_utc.isoformat() + "Z"
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
        baron_kills = default_numeric(obj.get("baron", {}).get("kills"))
        dragon_kills = default_numeric(obj.get("dragon", {}).get("kills"))
        tower_kills = default_numeric(obj.get("tower", {}).get("kills"))
        champ_kills = default_numeric(obj.get("champion", {}).get("kills"))
        rift_kills = default_numeric(obj.get("riftHerald", {}).get("kills"))
        inhib_kills = default_numeric(obj.get("inhibitor", {}).get("kills"))
        team_stats[t_id] = {
            "team_baronKills": baron_kills,
            "team_dragonKills": dragon_kills,
            "team_towerKills": tower_kills,
            "team_champKills": champ_kills,
            "team_riftHeraldKills": rift_kills,
            "team_inhibitorKills": inhib_kills,
        }

    participants = info.get("participants", [])
    rows = []
    for part in participants:
        participant_id = default_numeric(part.get("participantId"))
        champion_id = default_numeric(part.get("championId"))
        summoner_name = default_text(part.get("summonerName"))
        position = default_text(part.get("individualPosition") or part.get("teamPosition"))
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
            item_val = get_item_name(part.get(key))
            items[key] = item_val

        # Flatten rank
        platform = platform_id.upper()
        summoner_id = default_text(part.get("summonerId"))
        rank_data = await get_summoner_rank(session, summoner_id, platform)
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
        puuid = part.get("puuid")
        if puuid not in mastery_cache:
            mastery_cache[puuid] = await get_champion_mastery_list(session, puuid)
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

        # Additional computed columns
        # KDA ratio
        kda_ratio = (kills + assists) / max(1, deaths)
        # Kill Participation
        kill_participation = (kills + assists) / team_champ_kills
        # Gold per minute
        gold_per_min = gold_earned / (duration / 60.0)
        # Damage per minute
        dmg_per_min = dmg_dealt / (duration / 60.0)
        # Damage to champions per minute
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

            # Basic stats
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

            # Items
            "item0": items["item0"],
            "item1": items["item1"],
            "item2": items["item2"],
            "item3": items["item3"],
            "item4": items["item4"],
            "item5": items["item5"],
            "item6": items["item6"],

            # Rank
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

            # Mastery
            "mastery_level": mastery_level,
            "mastery_points": mastery_points,
            "mastery_lastPlayTime": mastery_last_play,
            "mastery_pointsSinceLastLevel": mastery_points_since,
            "mastery_pointsUntilNextLevel": mastery_points_until,
            "mastery_tokens": mastery_tokens,

            # Final champion stats from timeline
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

            # Team-level stats
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
# 8. SAVE TO CSV
###############################################################################
def save_chunk_to_csv(all_data, total_rows):
    if not all_data:
        return
    filename = f"league_data_flat_{total_rows}.csv"
    keys = list(all_data[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_data)
    print(f"[SAVE] Wrote {len(all_data)} rows (cumulative {total_rows}) to file: {filename}")

    prev_count = total_rows - CHUNK_SIZE
    if prev_count > 0:
        prev_filename = f"league_data_flat_{prev_count}.csv"
        if os.path.exists(prev_filename):
            os.remove(prev_filename)
            print(f"Removed previous file: {prev_filename}")

###############################################################################
# 9. MAIN FUNCTION
###############################################################################
async def main():
    global ITEM_MAP
    puuid_pool = {INITIAL_PUUID}
    processed_matches = set()
    all_data = []
    total_rows = 0
    rows_since_last_save = 0

    # We'll keep a champion mastery cache to avoid multiple calls
    mastery_cache = {}

    async with ClientSession() as session:
        # Load item mapping from Data Dragon
        latest_version = await get_latest_dd_version(session)
        if latest_version:
            ITEM_MAP = await load_item_mapping(session, latest_version)
            print(f"[INFO] Loaded item map for version {latest_version}, total items: {len(ITEM_MAP)}")
        else:
            print("[WARN] Could not load Data Dragon version; item map will be empty.")

        while total_rows < MAX_ROWS and puuid_pool:
            current_puuid = puuid_pool.pop()
            print(f"[INFO] Fetching match history for PUUID: {current_puuid}")
            match_ids = await get_match_history(session, current_puuid, count=MATCH_HISTORY_COUNT)
            if not match_ids:
                print(f"[WARN] No match_ids for {current_puuid} or error while fetching.")
                continue

            for match_id in match_ids:
                if match_id in processed_matches:
                    continue
                processed_matches.add(match_id)
                print(f"[INFO] Processing match {match_id}")
                match_details = await get_match_details(session, match_id)
                if match_details:
                    timeline = await get_match_timeline(session, match_id)
                    new_rows = await process_match_data(session, match_details, timeline, mastery_cache)
                    for row in new_rows:
                        all_data.append(row)
                        total_rows += 1
                        rows_since_last_save += 1

                        if rows_since_last_save >= CHUNK_SIZE:
                            save_chunk_to_csv(all_data, total_rows)
                            rows_since_last_save = 0

                        if total_rows >= MAX_ROWS:
                            break
                if total_rows >= MAX_ROWS:
                    break

    # Save remaining rows if chunk not empty
    if all_data and (total_rows % CHUNK_SIZE != 0):
        save_chunk_to_csv(all_data, total_rows)

    print("[DONE] Data collection complete.")
    print(f"Collected a total of {total_rows} rows.")

###############################################################################
# 10. LAUNCH
###############################################################################
if __name__ == "__main__":
    asyncio.run(main())
