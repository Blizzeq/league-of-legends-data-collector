"""Command-line interface: ``lol-collector collect | export | report | info``."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sqlite3
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

import aiohttp

from . import __version__
from .client import RiotApiError, RiotClient
from .crawler import CrawlConfig, Crawler, SeedError
from .export import ExportError, export_tables
from .routing import UnknownPlatformError, normalize_platform
from .static_data import StaticDataProvider
from .storage import Store

log = logging.getLogger("lol_collector")

DEFAULT_DB = Path("data/lol.sqlite3")
QUEUE_PRESETS = {
    "ranked-solo": 420,
    "ranked-flex": 440,
    "draft": 400,
    "swiftplay": 480,
    "aram": 450,
    "arena": 1700,
    "clash": 700,
}


class CliError(Exception):
    """A user-facing error: printed without a traceback, exit code 1."""


# ------------------------------------------------------------------ helpers


def load_dotenv(path: Path = Path(".env")) -> None:
    """Minimal ``.env`` support: ``KEY=VALUE`` lines; non-empty environment variables win.

    Accepts ``export KEY=VALUE``, quotes, trailing ``# comments`` and files saved by
    Windows editors (UTF-8 with BOM, UTF-16).
    """
    if not path.is_file():
        return
    raw = path.read_bytes()
    encoding = "utf-16" if raw.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    try:
        text = raw.decode(encoding)
    except UnicodeDecodeError:
        log.warning("Ignoring %s: save it as UTF-8.", path)
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        value = value.strip()
        if value[:1] in ("'", '"') and value[-1:] == value[:1] and len(value) > 1:
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].strip()
        if not os.environ.get(key.strip()):
            os.environ[key.strip()] = value


def parse_queue(value: str) -> int | None:
    value = value.strip().lower()
    if value == "any":
        return None
    if value in QUEUE_PRESETS:
        return QUEUE_PRESETS[value]
    if value.isdigit():
        return int(value)
    presets = ", ".join(QUEUE_PRESETS)
    raise argparse.ArgumentTypeError(f"'{value}' is not a queue ID or one of: any, {presets}")


def parse_date(value: str) -> int:
    """``YYYY-MM-DD`` (UTC midnight) -> epoch seconds."""
    try:
        day = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"'{value}' is not a date (YYYY-MM-DD)") from exc
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())


def parse_platform(value: str) -> str:
    try:
        return normalize_platform(value)
    except UnknownPlatformError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def non_negative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be 0 or more")
    return number


# ------------------------------------------------------------------ parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lol-collector",
        description="Collect League of Legends match data from the Riot API and build reports.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    sub = parser.add_subparsers(dest="command", required=True)

    db_arg = argparse.ArgumentParser(add_help=False)
    db_arg.add_argument(
        "--db", type=Path, default=DEFAULT_DB, help=f"SQLite database (default: {DEFAULT_DB})"
    )

    collect = sub.add_parser(
        "collect",
        parents=[db_arg],
        help="crawl matches into the database",
        description="Crawl matches breadth-first from seed players. Safe to stop with Ctrl+C "
        "and resume later: rerun without seeds to continue the existing crawl.",
    )
    collect.add_argument(
        "-p",
        "--platform",
        type=parse_platform,
        required=True,
        help="platform to crawl, e.g. EUW1, EUN1, NA1, KR",
    )
    collect.add_argument(
        "-s",
        "--seed",
        action="append",
        default=[],
        metavar="RIOT_ID|PUUID",
        help="starting player, e.g. 'Name#TAG' (repeatable)",
    )
    collect.add_argument(
        "--seed-ladder",
        action="append",
        default=[],
        metavar="TIER[:DIVISION]",
        help="seed with random ranked players, e.g. CHALLENGER or EMERALD:II (repeatable)",
    )
    collect.add_argument(
        "--ladder-size",
        type=positive_int,
        default=25,
        help="players taken from each --seed-ladder tier (default: 25)",
    )
    collect.add_argument(
        "-q",
        "--queue",
        action="append",
        type=parse_queue,
        metavar="QUEUE",
        help="queue preset or ID; 'any' for all queues (default: ranked-solo, repeatable)",
    )
    collect.add_argument(
        "--since",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="only games started on or after this date (UTC)",
    )
    collect.add_argument(
        "--until",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="only games started before this date (UTC)",
    )
    collect.add_argument(
        "-n",
        "--max-matches",
        type=positive_int,
        default=1000,
        help="stop after collecting this many new matches (default: 1000)",
    )
    collect.add_argument(
        "--matches-per-player",
        type=positive_int,
        default=20,
        help="match history depth per player (default: 20)",
    )
    collect.add_argument(
        "--max-depth",
        type=non_negative_int,
        help="crawl depth limit; 0 = seed players only (default: unlimited)",
    )
    collect.add_argument(
        "--min-duration",
        type=non_negative_int,
        default=300,
        metavar="SECONDS",
        help="skip games shorter than this (default: 300)",
    )
    collect.add_argument(
        "--concurrency",
        type=positive_int,
        default=8,
        help="matches processed in parallel (default: 8)",
    )
    collect.add_argument(
        "--no-timeline",
        action="store_true",
        help="skip timelines (no laning-phase or final-stat columns)",
    )
    collect.add_argument("--no-rank", action="store_true", help="skip ranked lookups")
    collect.add_argument("--no-mastery", action="store_true", help="skip champion mastery lookups")
    collect.add_argument(
        "--no-static", action="store_true", help="do not download Data Dragon names (IDs only)"
    )
    collect.add_argument(
        "--keep-raw", action="store_true", help="also store compressed raw match and timeline JSON"
    )

    export = sub.add_parser("export", parents=[db_arg], help="export tables to CSV or Parquet")
    export.add_argument("-f", "--format", choices=["csv", "parquet"], default="csv")
    export.add_argument(
        "-o",
        "--out",
        type=Path,
        default=Path("data/export"),
        help="output directory (default: data/export)",
    )
    export.add_argument(
        "--tables",
        nargs="+",
        choices=["participants", "teams", "matches"],
        default=["participants", "teams"],
    )

    report = sub.add_parser("report", parents=[db_arg], help="build the Excel report")
    report.add_argument(
        "-o", "--out", type=Path, help="output .xlsx (default: reports/lol-report-<date>.xlsx)"
    )
    report.add_argument(
        "--player", metavar="RIOT_ID", help="add a sheet focused on one player, e.g. 'Name#TAG'"
    )
    report.add_argument(
        "--patch", action="append", metavar="X.Y", help="only include these patches (repeatable)"
    )
    report.add_argument(
        "-q",
        "--queue",
        action="append",
        type=parse_queue,
        help="only include these queues (repeatable)",
    )
    report.add_argument(
        "--min-games",
        type=positive_int,
        default=10,
        help="minimum games for a champion to be ranked (default: 10)",
    )
    report.add_argument(
        "--max-data-rows",
        type=non_negative_int,
        default=20_000,
        help="rows in the Data sheet; 0 leaves it out (default: 20000)",
    )

    sub.add_parser("info", parents=[db_arg], help="summarise the database")
    return parser


# ----------------------------------------------------------------- commands


async def _collect(args: argparse.Namespace) -> int:
    api_key = os.environ.get("RIOT_API_KEY", "").strip()
    if not api_key:
        raise CliError("RIOT_API_KEY is not set. Put it in the environment or in a .env file.")
    queues = args.queue or [420]
    config = CrawlConfig(
        platform=args.platform,
        max_matches=args.max_matches,
        queues=None if None in queues else frozenset(q for q in queues if q is not None),
        start_time_s=args.since,
        end_time_s=args.until,
        matches_per_player=args.matches_per_player,
        max_depth=args.max_depth,
        min_duration_s=args.min_duration,
        concurrency=args.concurrency,
        timeline=not args.no_timeline,
        rank=not args.no_rank,
        mastery=not args.no_mastery,
        keep_raw=args.keep_raw,
    )

    with Store(args.db) as store:
        async with RiotClient(api_key) as client, aiohttp.ClientSession() as http:
            static = None if args.no_static else StaticDataProvider(http, args.db.parent / "cache")
            crawler = Crawler(client, store, config, static)

            seeds = [await crawler.resolve_seed(seed) for seed in args.seed]
            for spec in args.seed_ladder:
                seeds += await crawler.ladder_seeds(spec, args.ladder_size)
            if seeds:
                store.add_seeds(seeds)
                log.info("Queued %d seed players", len(seeds))
            elif store.pending_players() == 0:
                raise CliError("Nothing to crawl: pass --seed or --seed-ladder.")
            else:
                log.info("Resuming: %d players queued in %s", store.pending_players(), args.db)

            loop = asyncio.get_running_loop()

            def stop_gracefully() -> None:
                crawler.stop()
                # A second Ctrl+C falls through to KeyboardInterrupt and quits at once.
                loop.remove_signal_handler(signal.SIGINT)

            with contextlib.suppress(NotImplementedError):  # not available on Windows
                loop.add_signal_handler(signal.SIGINT, stop_gracefully)
                loop.add_signal_handler(signal.SIGTERM, crawler.stop)
            stats = await crawler.run()

    log.info(
        "Done: %d matches collected, %d skipped, %d failed in %.0f min. Database: %s",
        stats.collected,
        stats.skipped,
        stats.failed,
        stats.elapsed_s / 60,
        args.db,
    )
    return 0


def _require_db(path: Path) -> None:
    if not path.is_file():
        raise CliError(f"Database {path} does not exist. Run 'lol-collector collect' first.")


def _export(args: argparse.Namespace) -> int:
    _require_db(args.db)
    with Store(args.db) as store:
        export_tables(store, args.out, args.format, args.tables)
    return 0


def _report(args: argparse.Namespace) -> int:
    from .report import ReportError, ReportOptions, build_report  # slow import, load lazily

    _require_db(args.db)
    out = args.out or Path("reports") / f"lol-report-{date.today():%Y%m%d}.xlsx"
    queues = [q for q in (args.queue or []) if q is not None]
    options = ReportOptions(
        player=args.player,
        patches=args.patch or [],
        queues=queues,
        min_games=args.min_games,
        max_data_rows=args.max_data_rows,
    )
    with Store(args.db) as store:
        try:
            build_report(store, out, options)
        except ReportError as exc:
            raise CliError(str(exc)) from exc
    log.info("Report written to %s", out)
    return 0


def _info(args: argparse.Namespace) -> int:
    _require_db(args.db)
    with Store(args.db) as store:
        summary = store.summary()
    width = max(len(k) for k in summary)
    for key, value in summary.items():
        print(f"{key.replace('_', ' '):<{width}}  {value}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        if args.command == "collect":
            return asyncio.run(_collect(args))
        if args.command == "export":
            return _export(args)
        if args.command == "report":
            return _report(args)
        return _info(args)
    except (CliError, SeedError, ExportError, RiotApiError) as exc:
        log.error("%s", exc)
        return 1
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc):
            log.error("%s is in use by another lol-collector process.", args.db)
        else:
            log.error("Database error in %s: %s", args.db, exc)
        return 1
    except (OSError, sqlite3.Error) as exc:
        log.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        log.warning("Interrupted.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
