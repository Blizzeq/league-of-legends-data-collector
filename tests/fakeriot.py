"""Synthetic Riot API for tests: a deterministic world of players and matches served over HTTP.

Payload shapes follow match-v5, league-v4, champion-mastery-v4,
account-v1 and Data Dragon. Values are random but internally consistent.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web

POSITIONS = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
CHAMPIONS = {
    "TOP": [(266, "Aatrox"), (24, "Jax"), (86, "Garen"), (122, "Darius"), (54, "Malphite")],
    "JUNGLE": [(64, "LeeSin"), (121, "Khazix"), (62, "MonkeyKing"), (876, "Lillia"), (154, "Zac")],
    "MIDDLE": [(103, "Ahri"), (238, "Zed"), (61, "Orianna"), (134, "Syndra"), (7, "Leblanc")],
    "BOTTOM": [(222, "Jinx"), (51, "Caitlyn"), (81, "Ezreal"), (145, "Kaisa"), (21, "MissFortune")],
    "UTILITY": [(412, "Thresh"), (117, "Lulu"), (89, "Leona"), (25, "Morgana"), (526, "Rell")],
}
DISPLAY_NAMES = {"LeeSin": "Lee Sin", "Khazix": "Kha'Zix", "MonkeyKing": "Wukong",
                 "Leblanc": "LeBlanc", "Kaisa": "Kai'Sa", "MissFortune": "Miss Fortune"}  # fmt: skip
ITEMS = {
    3031: ("Infinity Edge", 3450, ["Damage", "CriticalStrike"], [], 3),
    3071: ("Black Cleaver", 3000, ["Damage", "Health"], [], 3),
    3089: ("Rabadon's Deathcap", 3600, ["SpellDamage"], [], 3),
    3157: ("Zhonya's Hourglass", 3250, ["SpellDamage", "Armor"], [], 3),
    6672: ("Kraken Slayer", 3100, ["Damage", "AttackSpeed"], [], 3),
    3190: ("Locket of the Iron Solari", 2200, ["Health", "Aura"], [], 3),
    3068: ("Sunfire Aegis", 2700, ["Health", "Armor"], [], 3),
    3006: ("Berserker's Greaves", 1100, ["Boots", "AttackSpeed"], [], 2),
    3020: ("Sorcerer's Shoes", 1100, ["Boots", "MagicPenetration"], [], 2),
    3047: ("Plated Steelcaps", 1100, ["Boots", "Armor"], [], 2),
    1036: ("Long Sword", 350, ["Damage"], ["3071"], 1),
    2055: ("Control Ward", 75, ["Consumable", "Vision"], [], 1),
    3340: ("Stealth Ward", 0, ["Trinket", "Vision"], [], 1),
}
SPELLS = {4: "Flash", 14: "Ignite", 11: "Smite", 7: "Heal", 12: "Teleport", 3: "Exhaust"}
RUNES = [
    (8000, "Precision", [8005, 8008, 8021], ["Press the Attack", "Lethal Tempo", "Fleet Footwork"]),
    (8100, "Domination", [8112, 8128], ["Electrocute", "Dark Harvest"]),
    (8200, "Sorcery", [8214, 8229], ["Summon Aery", "Arcane Comet"]),
    (8400, "Resolve", [8437, 8439], ["Grasp of the Undying", "Aftershock"]),
]
TIERS = ["IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM", "EMERALD", "DIAMOND", "MASTER"]
DDRAGON_VERSIONS = ["16.20.1", "16.19.1", "16.18.1"]


def _puuid(rng: random.Random) -> str:
    return "".join(rng.choice(string.ascii_letters + string.digits + "-_") for _ in range(78))


@dataclass
class Player:
    puuid: str
    game_name: str
    tag_line: str
    tier: str | None
    division: str | None
    lp: int
    match_ids: list[str] = field(default_factory=list)


class World:
    """Players and matches on one platform. Every player appears in several matches."""

    def __init__(self, seed: int = 7, players: int = 60, matches: int = 40,
                 platform: str = "EUW1", queue_id: int = 420) -> None:  # fmt: skip
        rng = random.Random(seed)
        self.rng = rng
        self.platform = platform
        self.players: dict[str, Player] = {}
        for i in range(players):
            tier = rng.choice([*TIERS, None])
            self.players[f"p{i}"] = Player(
                puuid=_puuid(rng),
                game_name=f"Player{i}",
                tag_line="EUW",
                tier=tier,
                division=None if tier in (None, "MASTER") else rng.choice(["I", "II", "III", "IV"]),
                lp=rng.randint(0, 99),
            )
        self.by_puuid = {p.puuid: p for p in self.players.values()}
        self.matches: dict[str, dict[str, Any]] = {}
        self.timelines: dict[str, dict[str, Any]] = {}
        start = 1_760_000_000_000  # 2025-10-09
        pool = list(self.players.values())
        for m in range(matches):
            match_id = f"{platform}_{7_100_000_000 + m}"
            members = rng.sample(pool, 10)
            version = rng.choice(["16.19.712.4120", "16.20.715.1234"])
            queue = queue_id if m % 10 else 450  # every tenth match is ARAM, to test filtering
            duration = 120 if m == 3 else rng.randint(1_200, 2_300)  # one remake
            match, timeline = make_match(rng, match_id, platform, queue, start + m * 3_600_000,
                                         duration, version, members)  # fmt: skip
            self.matches[match_id] = match
            self.timelines[match_id] = timeline
            for p in members:
                p.match_ids.insert(0, match_id)  # newest first, like the API

    def player(self, key: str) -> Player:
        return self.players[key]


def make_match(rng: random.Random, match_id: str, platform: str, queue_id: int, start_ms: int,
               duration_s: int, version: str, members: list[Player]) -> tuple[dict, dict]:  # fmt: skip
    remake = duration_s < 300
    blue_wins = rng.random() < 0.5
    participants = []
    frames_count = duration_s // 60 + 1
    for idx, player in enumerate(members):
        pid = idx + 1
        team_id = 100 if pid <= 5 else 200
        position = POSITIONS[idx % 5]
        champ_id, champ_name = rng.choice(CHAMPIONS[position])
        win = blue_wins if team_id == 100 else not blue_wins
        kills = rng.randint(0, 12)
        deaths = rng.randint(0, 10)
        jungle = position == "JUNGLE"
        minions = (
            0 if position in ("JUNGLE", "UTILITY") else int(duration_s / 60 * rng.uniform(5, 8.5))
        )
        neutral = int(duration_s / 60 * rng.uniform(4, 6)) if jungle else rng.randint(0, 12)
        items = [*rng.sample([i for i in ITEMS if ITEMS[i][1] >= 1100], 4), 1036, 0, 3340]
        style, _, perks, _ = rng.choice(RUNES)
        sub_style = rng.choice([r for r in RUNES if r[0] != style])[0]
        participants.append(
            {
                "participantId": pid,
                "puuid": player.puuid,
                "riotIdGameName": player.game_name,
                "riotIdTagline": player.tag_line,
                "summonerName": "",
                "summonerLevel": rng.randint(30, 600),
                "teamId": team_id,
                "win": win,
                "teamPosition": "" if remake else position,
                "individualPosition": position,
                "championId": champ_id,
                "championName": champ_name,
                "champLevel": rng.randint(11, 18),
                "kills": kills,
                "deaths": deaths,
                "assists": rng.randint(0, 20),
                "largestMultiKill": min(kills, rng.randint(1, 3)),
                "pentaKills": 0,
                "firstBloodKill": False,
                "totalTimeSpentDead": deaths * 25,
                "totalMinionsKilled": minions,
                "neutralMinionsKilled": neutral,
                "goldEarned": int(duration_s / 60 * rng.uniform(320, 480)),
                "totalDamageDealtToChampions": rng.randint(5_000, 45_000),
                "physicalDamageDealtToChampions": rng.randint(1_000, 20_000),
                "magicDamageDealtToChampions": rng.randint(1_000, 20_000),
                "trueDamageDealtToChampions": rng.randint(0, 3_000),
                "totalDamageTaken": rng.randint(8_000, 40_000),
                "damageSelfMitigated": rng.randint(3_000, 30_000),
                "damageDealtToBuildings": rng.randint(0, 9_000),
                "damageDealtToObjectives": rng.randint(0, 20_000),
                "totalHealsOnTeammates": rng.randint(0, 4_000),
                "totalDamageShieldedOnTeammates": rng.randint(0, 4_000),
                "timeCCingOthers": rng.randint(0, 60),
                "turretTakedowns": rng.randint(0, 4),
                "dragonKills": rng.randint(0, 2) if jungle else 0,
                "baronKills": rng.randint(0, 1) if jungle else 0,
                "visionScore": rng.randint(5, 90),
                "wardsPlaced": rng.randint(3, 40),
                "wardsKilled": rng.randint(0, 12),
                "visionWardsBoughtInGame": rng.randint(0, 8),
                "summoner1Id": 4,
                "summoner2Id": 11 if jungle else rng.choice([14, 7, 12, 3]),
                "perks": {
                    "styles": [
                        {
                            "description": "primaryStyle",
                            "style": style,
                            "selections": [{"perk": perks[0]}],
                        },
                        {"description": "subStyle", "style": sub_style, "selections": []},
                    ]
                },
                **{f"item{i}": items[i] for i in range(7)},
                "roleBoundItem": 0,
                "gameEndedInEarlySurrender": remake,
                "gameEndedInSurrender": False,
                "challenges": {
                    "soloKills": rng.randint(0, 4),
                    "turretPlatesTaken": rng.randint(0, 5),
                    "kda": 0.0,
                },
            }
        )
    participants[0]["firstBloodKill"] = True
    teams = []
    first_dragon = rng.choice((100, 200))
    first_herald = rng.choice((100, 200))
    for team_id in (100, 200):
        win = blue_wins if team_id == 100 else not blue_wins
        teams.append(
            {
                "teamId": team_id,
                "win": win,
                "bans": [
                    {"championId": rng.choice([157, 238, 64, -1]), "pickTurn": t}
                    for t in range(1, 6)
                ],
                "objectives": {
                    "champion": {"first": team_id == 100, "kills": rng.randint(10, 40)},
                    "tower": {"first": win, "kills": rng.randint(2, 11)},
                    "inhibitor": {"first": win, "kills": rng.randint(0, 3)},
                    "dragon": {"first": team_id == first_dragon, "kills": rng.randint(0, 4)},
                    "baron": {"first": win, "kills": rng.randint(0, 2)},
                    "riftHerald": {"first": team_id == first_herald, "kills": rng.randint(0, 1)},
                    "horde": {"first": team_id == first_herald, "kills": rng.randint(0, 6)},
                    "atakhan": {"first": win, "kills": int(win and rng.random() < 0.6)},
                },
            }
        )
    match = {
        "metadata": {"dataVersion": "2", "matchId": match_id,
                     "participants": [p["puuid"] for p in participants]},
        "info": {
            "gameCreation": start_ms - 60_000, "gameStartTimestamp": start_ms,
            "gameEndTimestamp": start_ms + duration_s * 1000, "gameDuration": duration_s,
            "gameId": int(match_id.split("_")[1]), "gameMode": "CLASSIC", "gameType": "MATCHED_GAME",
            "gameVersion": version, "mapId": 11, "platformId": platform, "queueId": queue_id,
            "endOfGameResult": "GameComplete", "participants": participants, "teams": teams,
        },
    }  # fmt: skip

    frames = []
    for minute in range(frames_count):
        pframes = {}
        for p in participants:
            share = minute / max(frames_count - 1, 1)
            pframes[str(p["participantId"])] = {
                "participantId": p["participantId"],
                "totalGold": 500 + int((p["goldEarned"] - 500) * share),
                "currentGold": rng.randint(0, 1500), "level": 1 + int(17 * share),
                "xp": int(18_000 * share * rng.uniform(0.9, 1.1)),
                "minionsKilled": int(p["totalMinionsKilled"] * share),
                "jungleMinionsKilled": int(p["neutralMinionsKilled"] * share),
                "championStats": {
                    "abilityHaste": rng.randint(0, 60), "abilityPower": rng.randint(0, 600),
                    "attackDamage": rng.randint(60, 400), "attackSpeed": rng.randint(100, 250),
                    "armor": rng.randint(30, 200), "magicResist": rng.randint(30, 120),
                    "armorPen": rng.randint(0, 30), "magicPen": rng.randint(0, 30),
                    "healthMax": rng.randint(600, 4000), "movementSpeed": rng.randint(330, 480),
                    "lifesteal": rng.randint(0, 20), "omnivamp": rng.randint(0, 10),
                },
            }  # fmt: skip
        events: list[dict[str, Any]] = []
        if minute == frames_count - 1 and frames_count > 25:
            winner = 100 if blue_wins else 200
            events.append({"type": "DRAGON_SOUL_GIVEN", "teamId": winner, "name": "Infernal",
                           "timestamp": minute * 60_000})  # fmt: skip
        frames.append({"timestamp": minute * 60_000 + (12 if minute else 0),
                       "participantFrames": pframes, "events": events})  # fmt: skip
    timeline = {"metadata": {"matchId": match_id},
                "info": {"frameInterval": 60_000, "frames": frames}}  # fmt: skip
    return match, timeline


def ddragon_payloads() -> dict[str, Any]:
    champions = {
        name: {"key": str(cid), "id": name, "name": DISPLAY_NAMES.get(name, name)}
        for pool in CHAMPIONS.values()
        for cid, name in pool
    }
    for cid, name in ((157, "Yasuo"),):
        champions[name] = {"key": str(cid), "id": name, "name": name}
    items = {
        str(i): {"name": n, "gold": {"total": g, "purchasable": True}, "tags": t, "into": into,
                 "depth": d}
        for i, (n, g, t, into, d) in ITEMS.items()
    }  # fmt: skip
    spells = {f"S{k}": {"key": str(k), "name": v} for k, v in SPELLS.items()}
    runes = [
        {"id": sid, "name": sname,
         "slots": [{"runes": [{"id": i, "name": n} for i, n in zip(ids, names, strict=True)]}]}
        for sid, sname, ids, names in RUNES
    ]  # fmt: skip
    queues = [{"queueId": 420, "map": "Summoner's Rift", "description": "5v5 Ranked Solo games"},
              {"queueId": 450, "map": "Howling Abyss", "description": "5v5 ARAM games"}]  # fmt: skip
    return {"champion": {"data": champions}, "item": {"data": items},
            "summoner": {"data": spells}, "runesReforged": runes, "queues": queues}  # fmt: skip


@dataclass
class FakeRiot:
    """aiohttp application serving a :class:`World`. Records every request path."""

    world: World
    app_limit: str = "20:1,100:120"
    fail_429_times: int = 0
    fail_500_times: int = 0
    truncate_times: int = 0
    reject_key: bool = False
    requests: list[str] = field(default_factory=list)

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/ddragon/api/versions.json", self._versions)
        app.router.add_get("/ddragon/cdn/{version}/data/{lang}/{file}", self._ddragon)
        app.router.add_get("/static/queues.json", self._queues)
        app.router.add_get("/{host}/{path:.*}", self._api)
        return app

    async def _versions(self, request: web.Request) -> web.Response:
        return web.json_response(DDRAGON_VERSIONS)

    async def _ddragon(self, request: web.Request) -> web.Response:
        name = request.match_info["file"].removesuffix(".json")
        return web.json_response(ddragon_payloads()[name])

    async def _queues(self, request: web.Request) -> web.Response:
        return web.json_response(ddragon_payloads()["queues"])

    def _json(self, payload: Any, status: int = 200) -> web.Response:
        headers = {"X-App-Rate-Limit": self.app_limit, "X-Method-Rate-Limit": "2000:10"}
        return web.json_response(payload, status=status, headers=headers)

    async def _api(self, request: web.Request) -> web.Response:
        self.requests.append(request.path_qs)
        if self.reject_key or request.headers.get("X-Riot-Token") != "test-key":
            return self._json({"status": {"status_code": 403, "message": "Forbidden"}}, 403)
        if self.truncate_times > 0:
            # Announce a longer body than is sent, then drop the connection.
            self.truncate_times -= 1
            headers = {"Content-Type": "application/json", "Content-Length": "400"}
            truncated = web.StreamResponse(headers=headers)
            await truncated.prepare(request)
            await truncated.write(b'{"truncated":')
            assert request.transport is not None
            request.transport.close()
            return truncated
        if self.fail_500_times > 0:
            self.fail_500_times -= 1
            return web.json_response({}, status=503)
        if self.fail_429_times > 0:
            self.fail_429_times -= 1
            return web.json_response({}, status=429, headers={
                "Retry-After": "0", "X-Rate-Limit-Type": "application"})  # fmt: skip
        parts = request.match_info["path"].split("/")
        w = self.world
        try:
            if parts[:4] == ["riot", "account", "v1", "accounts"] and parts[4] == "by-riot-id":
                for p in w.players.values():
                    if p.game_name.lower() == parts[5].lower() and p.tag_line == parts[6]:
                        return self._json({"puuid": p.puuid, "gameName": p.game_name,
                                           "tagLine": p.tag_line})  # fmt: skip
                return self._json({}, 404)
            if parts[:3] == ["lol", "match", "v5"]:
                if parts[3] == "matches" and parts[4] == "by-puuid":
                    return self._match_ids(request, parts[5])
                match_id = parts[4]
                if match_id not in w.matches:
                    return self._json({}, 404)
                if len(parts) > 5 and parts[5] == "timeline":
                    return self._json(w.timelines[match_id])
                return self._json(w.matches[match_id])
            if parts[:5] == ["lol", "league", "v4", "entries", "by-puuid"]:
                return self._json(self._entries(w.by_puuid[parts[5]]))
            if parts[:3] == ["lol", "league", "v4"] and parts[3].endswith("leagues"):
                tier = parts[3].removesuffix("leagues").upper()
                entries = [
                    {"puuid": p.puuid, "leaguePoints": p.lp, "rank": "I", "wins": 50, "losses": 40}
                    for p in w.players.values()
                    if p.tier == tier
                ]
                return self._json({"tier": tier, "queue": parts[5], "entries": entries})
            if parts[:4] == ["lol", "league", "v4", "entries"]:
                tier, division = parts[5], parts[6]
                page = int(request.query.get("page", "1"))
                entries = (
                    []
                    if page > 1
                    else [
                        self._entries(p)[0]
                        for p in w.players.values()
                        if p.tier == tier and p.division == division
                    ]
                )
                return self._json(entries)
            if parts[:3] == ["lol", "champion-mastery", "v4"]:
                return self._json(
                    {
                        "championId": int(parts[7]),
                        "championLevel": 12,
                        "championPoints": 123_456,
                        "lastPlayTime": 1_760_000_000_000,
                    }
                )
        except (IndexError, KeyError):
            return self._json({}, 404)
        return self._json({}, 404)  # fmt: skip

    def _match_ids(self, request: web.Request, puuid: str) -> web.Response:
        player = self.world.by_puuid.get(puuid)
        if player is None:
            return self._json([])
        ids = player.match_ids
        queue = request.query.get("queue")
        if queue:
            ids = [m for m in ids if self.world.matches[m]["info"]["queueId"] == int(queue)]
        start = int(request.query.get("start", 0))
        count = int(request.query.get("count", 20))
        return self._json(ids[start : start + count])

    @staticmethod
    def _entries(player: Player) -> list[dict[str, Any]]:
        if player.tier is None:
            return []
        return [{"queueType": "RANKED_SOLO_5x5", "tier": player.tier,
                 "rank": player.division or "I", "leaguePoints": player.lp, "wins": 30,
                 "losses": 25, "puuid": player.puuid}]  # fmt: skip
