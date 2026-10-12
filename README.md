# League of Legends Data Collector

Collects League of Legends match data from the Riot Games API into a local SQLite
database, exports it to CSV or Parquet, and builds an Excel report with champion,
role, objective and item statistics.

The collector crawls breadth-first: it starts from one or more players, downloads
their recent matches, then continues with everyone they played with. The result is
a flat table with one row per player per game and another with one row per team
per game.

- **Seeds:** Riot IDs (`Name#TAG`), PUUIDs, or a random sample of a ranked tier
  (`--seed-ladder EMERALD:II`).
- **Filters applied by the API:** queue and date range are sent to the match list
  endpoint (one list per queue when several are selected), so games from other
  queues are never downloaded.
- **107 columns per player**, including laning-phase gold, CS and
  XP differences at 10 and 15 minutes, damage and gold shares, runes, items, final
  champion stats, a ranked snapshot and champion mastery.
- **Rate limits read from Riot's response headers:** development, personal and
  production keys work without configuration; `429` responses are handled.
- **Resumable:** every match is committed on its own and a player counts as crawled
  only once all of its queued matches are stored, so after `Ctrl+C`, an expired key
  or a crash the next run continues where the last one stopped.
- **Excel report:** an overview dashboard, a champion table with win-rate
  confidence intervals, lane matchups, roles, objectives, items, an optional player
  profile, the raw data and a glossary. Charts and tables are native Excel objects, so they
  stay sortable and filterable.

## Requirements

- Python 3.11 or newer
- A Riot API key from the [Riot Developer Portal](https://developer.riotgames.com/).
  Development keys expire 24 hours after they are generated.

## Installation

```bash
git clone https://github.com/Blizzeq/league-of-legends-data-collector.git
cd league-of-legends-data-collector
python3 -m venv .venv
source .venv/bin/activate
pip install -e .              # or: pip install -e ".[parquet]" for Parquet export
cp .env.example .env          # then put your key in .env
```

The key is read from the `RIOT_API_KEY` environment variable or from a `.env` file
in the working directory. It is never passed on the command line.

## Quick start

```bash
# Collect 500 Ranked Solo/Duo games on EUW, starting from one player
lol-collector collect --platform EUW1 --seed "YourName#EUW" --max-matches 500

# Build the Excel report, with a sheet for that player
lol-collector report --player "YourName#EUW"

# Export the tables for pandas, R, Power BI, ...
lol-collector export --format csv
```

`lol-collector` is also available as `python -m lol_collector`.

## Commands

### `collect`

```text
lol-collector collect -p PLATFORM [seeds] [filters] [options]
```

| Option | Default | Description |
|---|---|---|
| `-p, --platform` | required | Platform to crawl: `EUW1`, `EUN1`, `NA1`, `KR`, `BR1`, ... |
| `-s, --seed` | | Starting player as `Name#TAG` or PUUID. Repeatable. |
| `--seed-ladder` | | Random players from a ranked tier: `CHALLENGER`, `GRANDMASTER`, `MASTER`, or `TIER[:DIVISION]` such as `DIAMOND:II`. Repeatable. |
| `--ladder-size` | 25 | Players taken from each `--seed-ladder` tier. |
| `-q, --queue` | `ranked-solo` | `ranked-solo`, `ranked-flex`, `draft`, `swiftplay`, `aram`, `arena`, `clash`, a numeric queue ID, or `any`. Repeatable. |
| `--since`, `--until` | | Only games started in this UTC date range (`YYYY-MM-DD`). |
| `-n, --max-matches` | 1000 | Stop after this many new matches. |
| `--matches-per-player` | 20 | Match history depth requested per player (per queue when several are selected). |
| `--max-depth` | unlimited | `0` collects only the seeds' own games. |
| `--min-duration` | 300 | Skip games shorter than this many seconds. Remakes are always skipped. |
| `--concurrency` | 8 | Matches processed in parallel. |
| `--no-timeline` | | Skip timelines: one request less per match, but no laning-phase, final-stat, `dragon_soul` and team `gold_diff_at_15` columns. |
| `--no-rank`, `--no-mastery` | | Skip ranked and mastery lookups (the most request-hungry part). |
| `--no-static` | | Do not download Data Dragon files. Item, rune and spell names become placeholders such as `Item 3031`, and the Items sheet is left empty; champion and queue names still come from the match data. |
| `--keep-raw` | | Also store compressed raw match and timeline JSON. |
| `--db` | `data/lol.sqlite3` | Database file. Accepted by every command. |

Examples:

```bash
# Your own last 100 games in every queue, nobody else's
lol-collector collect -p EUN1 -s "YourName#EUNE" -q any --max-depth 0 --matches-per-player 100

# A high-elo dataset for one month, seeded from the ladder
lol-collector collect -p EUW1 --seed-ladder CHALLENGER --seed-ladder GRANDMASTER \
    --since 2026-09-01 --until 2026-10-01 -n 5000

# No rank or mastery lookups: about ten times fewer requests per match
lol-collector collect -p NA1 --seed-ladder EMERALD:I --no-rank --no-mastery -n 2000

# Continue the crawl stored in the database
lol-collector collect -p EUW1
```

Rerunning `collect` with seeds re-queues those players and fetches their newest
games, which makes it suitable for tracking one account over time.

### `report`

```bash
lol-collector report [--player "Name#TAG"] [--patch X.Y] [--queue ranked-solo] [--min-games 10] [-o report.xlsx]
```

| Sheet | Contents |
|---|---|
| Overview | Matches, players, average game length, blue-side win rate, games per patch, game length distribution, players by tier. |
| Player | With `--player`: key averages, rolling win rate, champion pool against the dataset, last 20 games. |
| Champions | Games, pick, ban and win rate with a 95% Wilson interval, KDA, CS, gold, damage, vision, gold difference at 15:00. |
| Matchups | Champion against the enemy in the same role: games, win rate with interval, gold difference at 15:00. |
| Roles | Averages per assigned role. |
| Objectives | Win rate of the team that took each first objective (blood, tower, dragon, Rift Herald, Baron, inhibitor), more Void Grubs or dragons, Atakhan, a dragon soul, or a gold lead at 15:00. |
| Items | Completed items in final inventories: games, pick rate, win rate. Needs Data Dragon item data. |
| Data | The latest participant rows as an Excel table (`--max-data-rows`, default 20,000; `0` leaves it out). |
| Glossary | Every column of the three tables with its type and meaning. |

The report is written to `reports/lol-report-<date>.xlsx` unless `-o` is given.
`--patch` takes the values listed by `lol-collector info`. Win rates are coloured
on a red–grey–blue scale centred on 50%. Champions and matchups with fewer than
`--min-games` games are left out, since a 70% win rate over five games says very
little.

Excel keeps every cell in memory while the file is written, so the Data sheet is
capped. On a synthetic database of 10,000 matches (100,000 rows) the report took
about 25 seconds and 1 GB of memory with the default cap, and about 90 seconds and
2.9 GB with all rows. Use `export` for the complete data.

### `export`

```bash
lol-collector export --format csv|parquet [--tables participants teams matches] [-o data/export]
```

Exports `participants` and `teams` unless `--tables` says otherwise.

### `info`

Prints a summary of the database: collected and skipped matches, players crawled
and queued, date range, patches and queues.

Every command also accepts `--log-level DEBUG|INFO|WARNING|ERROR` before the command
name, and `lol-collector --version` prints the version.

## Data

| Table | Grain |
|---|---|
| `participants` | One row per player per game. |
| `teams` | One row per team per game. |
| `matches` | One row per processed match, including skipped ones and the reason. |

[docs/data-dictionary.md](docs/data-dictionary.md) lists every column. Notes on
interpreting the data:

- **Rank and mastery are snapshots taken at collection time**, not at game time.
  `rank_snapshot_utc` records when.
- **Laning-phase differences** compare a player with the enemy who has the same
  `teamPosition`. They are empty when the game ended before that minute or when
  roles could not be paired.
- **PUUIDs are encrypted per Riot application.** A regenerated development key
  keeps working with the same database; a key from a different application
  (for example a new production key) needs a new database or fresh seeds.
- **Crawl bias:** a breadth-first crawl stays close to the seeds' rank. Seed from
  several tiers with `--seed-ladder` for a broader sample.
- **Missing values** are empty (NULL). Empty item slots are `0`, as in the API.

## Throughput

Riot enforces limits per API key and per routing value, so match data
(`europe`, `americas`, ...) and player lookups (`euw1`, `na1`, ...) have separate
budgets. A development key allows 20 requests per second and 100 per two minutes
on each routing value.

Each match costs one match and one timeline request on the regional routing value,
plus up to ten rank and ten mastery lookups on the platform. Each crawled player
costs one more regional request for the match list (one per queue).

| Mode | Bottleneck | Approximate speed with a development key |
|---|---|---|
| Default | up to 20 platform requests per match | 2–3 matches per minute |
| `--no-rank --no-mastery` | 2 regional requests per match plus match lists | 15–20 matches per minute |
| `--no-rank --no-mastery --no-timeline` | 1 regional request per match plus match lists | 25–35 matches per minute |

The figures are estimates from these request counts and the 50 requests per minute
a development key allows on each routing value; match-list overhead depends on how
many of a player's games are already stored. Ranked lookups are cached for 24 hours, so throughput improves as the
crawl revisits players. Larger datasets call for a personal or production key,
which the collector picks up from the response headers.

## Riot API changes covered

The 2025 version of this project predates several API changes. This version
accounts for them:

- Summoner names were replaced by Riot IDs. Players are identified by PUUID and
  shown by Riot ID (`riotIdGameName#riotIdTagline`); `summonerName` is only a
  fallback for old matches. Rank and mastery are looked up by PUUID; `summonerId`,
  deprecated in league-v4, is not used.
- Match-v5 data is served from four regional clusters: `americas`, `asia`,
  `europe` and `sea`. OC1 routes to `sea`; PH2 and TH2 were merged into SG2 on
  2025-01-08.
- Team objectives include `horde` (Void Grubs) and `atakhan`, and teams carry
  Feats of Warfare state.
- The match list endpoint filters by `queue`, `startTime` and `endTime` and returns
  at most 100 IDs per request.

## Project layout

```text
src/lol_collector/
    cli.py            command-line interface
    client.py         async Riot API client with retries
    ratelimit.py      limiter driven by X-App-Rate-Limit / X-Method-Rate-Limit
    routing.py        platform and regional routing values
    crawler.py        breadth-first crawl, filtering and enrichment
    transform.py      match and timeline payloads -> flat rows
    columns.py        column definitions (schema, exports, glossary)
    storage.py        SQLite storage, crawl frontier, caches
    static_data.py    Data Dragon names, cached per patch
    export.py         CSV and Parquet export
    report/           analytics and the Excel workbook
tests/                pytest suite with a local fake Riot API
```

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check src tests
black --check src tests
mypy src
```

The tests run against a fake Riot API served on localhost (`tests/fakeriot.py`), so
they need neither an API key nor network access.

## Migrating from the 2025 scripts

`riot_api_client.py` and `solo_duo_data_s15.py` were replaced by the
`lol-collector` command. The Season 15 Solo/Duo script corresponds to:

```bash
lol-collector collect -p EUN1 -s "Name#TAG" -q ranked-solo --since 2025-01-09 -n 20000
lol-collector export --format csv
```

Notable column changes: `summoner_name` is now `riot_id`; `start_utc` is
`game_start_utc`; `duration` is `game_duration_s`; `solo_rank` and `flex_rank` are
`solo_division` and `flex_division`; `kda_ratio` is `kda`; `damage_to_champ` is
`damage_to_champions`; item columns hold item IDs, with names in `items`; missing
values are empty instead of `0` or `N/A` (empty item slots stay `0`).

## Legal

Released under the [MIT License](LICENSE).

League of Legends Data Collector isn't endorsed by Riot Games and doesn't reflect the
views or opinions of Riot Games or anyone officially involved in producing or
managing Riot Games properties. Riot Games, and all associated properties are
trademarks or registered trademarks of Riot Games, Inc.
