# Data dictionary

Generated from `src/lol_collector/columns.py`. Do not edit by hand; run
`python -m lol_collector.columns > docs/data-dictionary.md`.

Empty values mean the information was not available (feature disabled, field
missing for that queue, or the game ended before the timestamp).

## participants

| Column | Type | Description |
|---|---|---|
| `match_id` | str | Match ID including the platform prefix, e.g. EUW1_7123456789. |
| `platform_id` | str | Platform (server) the game was played on. |
| `queue_id` | int | Queue ID (420 = Ranked Solo/Duo, 440 = Ranked Flex, ...). |
| `queue_name` | str | Human-readable queue name. |
| `game_start_utc` | datetime | Game start time, UTC. |
| `game_duration_s` | int | Game length in seconds. |
| `game_version` | str | Full client version reported by the API. |
| `patch` | str | Patch as major.minor: the first two parts of game_version. |
| `participant_id` | int | Participant slot 1-10 (1-5 blue side, 6-10 red side). |
| `puuid` | str | Player's PUUID: stable, but encrypted per Riot application. |
| `riot_id` | str | Riot ID at the time of the game, GameName#TAG. |
| `summoner_level` | int | Account level. |
| `team_id` | int | 100 = blue side, 200 = red side. |
| `side` | str | blue or red. |
| `win` | bool | Whether the player's team won. |
| `position` | str | Assigned role: TOP, JUNGLE, MID, BOT or SUPPORT (from teamPosition). |
| `champion_id` | int | Champion ID. |
| `champion_name` | str | Champion display name. |
| `champion_level` | int | Champion level at the end of the game. |
| `kills` | int | Champion kills. |
| `deaths` | int | Deaths. |
| `assists` | int | Assists. |
| `kda` | float | (kills + assists) / max(1, deaths). |
| `kill_participation` | pct | (kills + assists) / team kills. |
| `solo_kills` | int | Kills without assistance from allies (challenges). |
| `largest_multikill` | int | Largest multikill (2 = double ... 5 = penta). |
| `penta_kills` | int | Pentakills. |
| `first_blood_kill` | bool | Player scored first blood. |
| `time_dead_s` | int | Total time spent dead, seconds. |
| `cs` | int | Creep score: lane minions + neutral monsters. |
| `cs_per_min` | float | Creep score per minute. |
| `gold_earned` | int | Gold earned. |
| `gold_per_min` | float | Gold earned per minute. |
| `gold_share` | pct | Share of the team's gold. |
| `damage_to_champions` | int | Total damage dealt to champions. |
| `damage_per_min` | float | Damage to champions per minute. |
| `damage_share` | pct | Share of the team's damage to champions. |
| `physical_damage_to_champions` | int | Physical damage dealt to champions. |
| `magic_damage_to_champions` | int | Magic damage dealt to champions. |
| `true_damage_to_champions` | int | True damage dealt to champions. |
| `damage_taken` | int | Total damage taken. |
| `damage_mitigated` | int | Damage mitigated by the player's own resistances and shields. |
| `damage_to_buildings` | int | Damage dealt to turrets and inhibitors. |
| `damage_to_objectives` | int | Damage dealt to epic monsters and buildings. |
| `heals_on_teammates` | int | Healing done on teammates. |
| `shields_on_teammates` | int | Damage shielded on teammates. |
| `time_ccing_others_s` | int | Crowd-control score (seconds of CC applied, weighted by Riot). |
| `turret_takedowns` | int | Turrets destroyed or assisted. |
| `turret_plates_taken` | int | Turret plates taken (challenges). |
| `dragon_kills` | int | Dragons last-hit by the player. |
| `baron_kills` | int | Barons last-hit by the player. |
| `vision_score` | int | Vision score. |
| `vision_score_per_min` | float | Vision score per minute. |
| `wards_placed` | int | Wards placed. |
| `wards_killed` | int | Enemy wards destroyed. |
| `control_wards_bought` | int | Control wards bought. |
| `summoner_spells` | str | Summoner spells, e.g. Flash | Ignite. |
| `keystone` | str | Keystone rune. |
| `primary_style` | str | Primary rune path. |
| `secondary_style` | str | Secondary rune path. |
| `item0` | int | Item ID in slot 0 (0 = empty). |
| `item1` | int | Item ID in slot 1. |
| `item2` | int | Item ID in slot 2. |
| `item3` | int | Item ID in slot 3. |
| `item4` | int | Item ID in slot 4. |
| `item5` | int | Item ID in slot 5. |
| `item6` | int | Item ID in slot 6 (trinket). |
| `items` | str | Names of the final items in slots 0-5. |
| `role_bound_item` | int | Role-bound item ID, when the game mode has one. |
| `gold_at_10` | int | Total gold at 10:00. |
| `cs_at_10` | int | Creep score at 10:00. |
| `xp_at_10` | int | Experience at 10:00. |
| `gold_diff_at_10` | int | Gold difference to the lane opponent at 10:00. |
| `cs_diff_at_10` | int | Creep score difference to the lane opponent at 10:00. |
| `xp_diff_at_10` | int | Experience difference to the lane opponent at 10:00. |
| `gold_at_15` | int | Total gold at 15:00. |
| `cs_at_15` | int | Creep score at 15:00. |
| `xp_at_15` | int | Experience at 15:00. |
| `gold_diff_at_15` | int | Gold difference to the lane opponent at 15:00. |
| `cs_diff_at_15` | int | Creep score difference to the lane opponent at 15:00. |
| `xp_diff_at_15` | int | Experience difference to the lane opponent at 15:00. |
| `final_ability_haste` | float | Ability haste in the last timeline frame. |
| `final_ability_power` | float | Ability power in the last timeline frame. |
| `final_attack_damage` | float | Attack damage in the last timeline frame. |
| `final_attack_speed` | float | Attack speed (Riot units, 100 = 1.0) in the last frame. |
| `final_armor` | float | Armor in the last timeline frame. |
| `final_magic_resist` | float | Magic resist in the last timeline frame. |
| `final_armor_pen` | float | Flat armor penetration in the last timeline frame. |
| `final_magic_pen` | float | Flat magic penetration in the last timeline frame. |
| `final_health_max` | float | Maximum health in the last timeline frame. |
| `final_movement_speed` | float | Movement speed in the last timeline frame. |
| `final_lifesteal` | float | Life steal in the last timeline frame. |
| `final_omnivamp` | float | Omnivamp in the last timeline frame. |
| `solo_tier` | str | Ranked Solo/Duo tier when the data was collected (not at game time). |
| `solo_division` | str | Ranked Solo/Duo division (I-IV; empty for apex tiers). |
| `solo_lp` | int | Ranked Solo/Duo league points. |
| `solo_wins` | int | Ranked Solo/Duo wins this split. |
| `solo_losses` | int | Ranked Solo/Duo losses this split. |
| `flex_tier` | str | Ranked Flex tier when the data was collected. |
| `flex_division` | str | Ranked Flex division. |
| `flex_lp` | int | Ranked Flex league points. |
| `flex_wins` | int | Ranked Flex wins this split. |
| `flex_losses` | int | Ranked Flex losses this split. |
| `rank_snapshot_utc` | datetime | When the rank above was fetched. |
| `mastery_level` | int | Mastery level on the played champion when the data was collected. |
| `mastery_points` | int | Mastery points on the played champion. |
| `mastery_last_play_utc` | datetime | Last time the player played this champion. |

## teams

| Column | Type | Description |
|---|---|---|
| `match_id` | str | Match ID including the platform prefix, e.g. EUW1_7123456789. |
| `platform_id` | str | Platform (server) the game was played on. |
| `queue_id` | int | Queue ID (420 = Ranked Solo/Duo, 440 = Ranked Flex, ...). |
| `queue_name` | str | Human-readable queue name. |
| `game_start_utc` | datetime | Game start time, UTC. |
| `game_duration_s` | int | Game length in seconds. |
| `game_version` | str | Full client version reported by the API. |
| `patch` | str | Patch as major.minor: the first two parts of game_version. |
| `team_id` | int | 100 = blue side, 200 = red side. |
| `side` | str | blue or red. |
| `win` | bool | Whether the team won. |
| `kills` | int | Champion kills. |
| `gold_earned` | int | Gold earned by the whole team. |
| `damage_to_champions` | int | Damage to champions dealt by the whole team. |
| `towers` | int | Turrets destroyed. |
| `inhibitors` | int | Inhibitors destroyed. |
| `dragons` | int | Dragons killed. |
| `barons` | int | Barons killed. |
| `heralds` | int | Rift Heralds killed. |
| `void_grubs` | int | Void Grubs killed. |
| `atakhan` | int | Atakhan kills (0 or 1). |
| `first_blood` | bool | Team drew first blood. |
| `first_tower` | bool | Team destroyed the first turret. |
| `first_dragon` | bool | Team killed the first dragon. |
| `first_herald` | bool | Team killed the first Rift Herald. |
| `first_baron` | bool | Team killed the first Baron. |
| `first_inhibitor` | bool | Team destroyed the first inhibitor. |
| `dragon_soul` | str | Dragon soul the team claimed, if any (timeline). |
| `feat_first_blood` | int | Feats of Warfare: featState of FIRST_BLOOD as reported (undocumented scale). |
| `feat_first_turret` | int | Feats of Warfare: featState of FIRST_TURRET as reported. |
| `feat_epic_monsters` | int | Feats of Warfare: featState of EPIC_MONSTER_KILL as reported. |
| `gold_diff_at_15` | int | Team gold minus enemy team gold at 15:00 (timeline). |
| `bans` | str | Banned champions, in pick-turn order. |
| `ban_ids` | str | Banned champion IDs, comma-separated, in pick-turn order. |

## matches

| Column | Type | Description |
|---|---|---|
| `match_id` | str | Match ID. |
| `platform_id` | str | Platform. |
| `queue_id` | int | Queue ID. |
| `game_start_utc` | datetime | Game start time, UTC. |
| `game_duration_s` | int | Game length in seconds. |
| `patch` | str | Patch, major.minor. |
| `status` | str | collected, or skipped (not found, failed validation or filtered out). |
| `skip_reason` | str | Why the match was skipped. |
| `collected_at_utc` | datetime | When the collector processed the match. |
