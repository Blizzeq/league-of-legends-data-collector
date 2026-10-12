# Changelog

## 2.0.0 - 2026-10-12

Rewrite of the 2025 scripts as an installable package with a command-line interface.

### Added

- `lol-collector` command with `collect`, `export`, `report` and `info` subcommands.
- Seeding by Riot ID (`Name#TAG`) through account-v1, and random seeding from a
  ranked tier (`--seed-ladder`).
- Queue presets and date filters passed to the match list endpoint.
- SQLite storage with a persistent crawl frontier: runs can be stopped and resumed.
- Team table with objectives (including Void Grubs and Atakhan), first-objective
  flags, dragon soul, gold difference at 15:00 and bans.
- Laning-phase columns from the timeline: gold, CS and XP at 10:00 and 15:00 and the
  differences to the lane opponent.
- Damage and gold shares, kill participation, per-minute rates, runes, summoner
  spells and item names from Data Dragon, cached per patch.
- Excel report with an overview dashboard, champion, role, objective and item
  sheets, an optional player profile, the raw data and a glossary.
- CSV and Parquet export, a generated data dictionary, a test suite against a local
  fake Riot API, and CI.

### Changed

- Rate limits are read from `X-App-Rate-Limit` and `X-Method-Rate-Limit` and enforced
  per routing value, instead of fixed local limits.
- Ranked data comes from `league-v4/entries/by-puuid` and is cached for 24 hours;
  champion mastery is fetched on the player's own platform.
- Riot ID replaces the retired summoner name. Missing values are stored as empty
  instead of `0` or `N/A`.
- Data is committed per match instead of rewriting a growing CSV every 50 rows.

### Fixed

- `riot_api_client.py` referenced an undefined `puuid` variable when fetching ranks.
- `solo_duo_data_s15.py` created a new rate limiter on every rank and mastery call,
  so those requests were not rate limited.
- Champion mastery was always requested from the EUN1 host regardless of the
  player's platform.
- An expired or invalid key (401/403) no longer leaves the crawler running; it stops
  with an explanation.

### Removed

- `riot_api_client.py` and `solo_duo_data_s15.py`, including the API keys that were
  hard-coded in the latter.
