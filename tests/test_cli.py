import csv
import logging
import os
import re
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from lol_collector.cli import build_parser, load_dotenv, main, parse_queue
from lol_collector.columns import TABLES
from lol_collector.storage import Store

DOTENV_NAMES = (
    "RIOT_TEST_PLAIN",
    "RIOT_TEST_DOUBLE",
    "RIOT_TEST_SINGLE",
    "RIOT_TEST_SPACED",
    "RIOT_TEST_EMPTY",
    "RIOT_TEST_COMMENTED",
)


@pytest.fixture(autouse=True)
def _run_in_tmp_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads .env from the working directory and writes reports under it."""
    monkeypatch.chdir(tmp_path)


def _unset_env(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
    """Remove variables so that anything the test sets on them is undone at teardown."""
    for name in names:
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)


def _csv_rows(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


def _error_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


def test_collect_arguments_are_parsed_into_typed_values() -> None:
    args = build_parser().parse_args(
        ["collect", "-p", "euw1", "-s", "Name#TAG", "-q", "ranked-flex", "-q", "450",
         "--since", "2026-01-08"]
    )  # fmt: skip

    assert args.platform == "EUW1"
    assert args.seed == ["Name#TAG"]
    assert args.queue == [440, 450]
    assert args.since == 1_767_830_400  # 2026-01-08T00:00:00Z
    assert args.until is None


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["collect", "-p", "MARS1"], "Unknown platform 'MARS1'"),
        (["collect", "-p", "EUW1", "-q", "bogus"], "'bogus' is not a queue ID or one of: any,"),
        (["collect", "-p", "EUW1", "--since", "2026-02-30"], "'2026-02-30' is not a date"),
        (["collect", "-p", "EUW1", "--until", "yesterday"], "'yesterday' is not a date"),
    ],
)
def test_invalid_option_values_exit_with_code_2(
    argv: list[str], message: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(argv)

    assert excinfo.value.code == 2
    assert message in capsys.readouterr().err


@pytest.mark.parametrize(
    "option", [["-n", "0"], ["--concurrency", "0"], ["--max-depth", "-1"], ["--min-duration", "-5"]]
)
def test_counts_reject_values_below_their_minimum(option: list[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(["collect", "-p", "EUW1", *option])

    assert excinfo.value.code == 2


def test_max_depth_zero_is_accepted_for_seed_players_only() -> None:
    args = build_parser().parse_args(["collect", "-p", "EUW1", "--max-depth", "0"])

    assert args.max_depth == 0


@pytest.mark.parametrize(
    ("value", "expected"),
    [("any", None), (" ANY ", None), ("ranked-solo", 420), ("Aram", 450), ("1700", 1700)],
)
def test_parse_queue_accepts_any_presets_and_ids(value: str, expected: int | None) -> None:
    assert parse_queue(value) == expected


@pytest.mark.parametrize("api_key", [None, "", "   "])
def test_collect_without_api_key_logs_an_error_and_returns_1(
    api_key: str | None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    if api_key is None:
        _unset_env(monkeypatch, "RIOT_API_KEY")
    else:
        monkeypatch.setenv("RIOT_API_KEY", api_key)

    assert main(["collect", "-p", "EUW1", "-s", "X#Y"]) == 1
    assert _error_messages(caplog) == [
        "RIOT_API_KEY is not set. Put it in the environment or in a .env file."
    ]


@pytest.mark.parametrize("command", ["info", "export", "report"])
def test_commands_return_1_for_a_missing_database(
    command: str, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    missing = tmp_path / "missing.sqlite3"

    assert main([command, "--db", str(missing)]) == 1
    assert _error_messages(caplog) == [
        f"Database {missing} does not exist. Run 'lol-collector collect' first."
    ]
    assert not missing.exists()


def test_load_dotenv_parses_values_and_skips_comments_and_bad_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _unset_env(monkeypatch, *DOTENV_NAMES)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# RIOT_TEST_COMMENTED=1\n"
        "RIOT_TEST_PLAIN=token=abc\n"
        'RIOT_TEST_DOUBLE="quoted value"\n'
        "RIOT_TEST_SINGLE='single'\n"
        "  RIOT_TEST_SPACED =  spaced  \n"
        "RIOT_TEST_EMPTY=\n"
        "not a setting\n",
        encoding="utf-8",
    )

    load_dotenv(env_file)

    assert os.environ["RIOT_TEST_PLAIN"] == "token=abc"
    assert os.environ["RIOT_TEST_DOUBLE"] == "quoted value"
    assert os.environ["RIOT_TEST_SINGLE"] == "single"
    assert os.environ["RIOT_TEST_SPACED"] == "spaced"
    assert os.environ["RIOT_TEST_EMPTY"] == ""
    assert "RIOT_TEST_COMMENTED" not in os.environ


def test_load_dotenv_does_not_override_existing_variables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RIOT_TEST_EXISTING", "from-shell")
    env_file = tmp_path / ".env"
    env_file.write_text("RIOT_TEST_EXISTING=from-file\n", encoding="utf-8")

    load_dotenv(env_file)

    assert os.environ["RIOT_TEST_EXISTING"] == "from-shell"


async def test_info_prints_every_summary_field(
    collected_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["info", "--db", str(collected_db)]) == 0

    with Store(collected_db) as store:
        summary = store.summary()
    printed = {}
    for line in capsys.readouterr().out.splitlines():
        key, value = re.split(r"\s{2,}", line.strip(), maxsplit=1)
        printed[key] = value
    assert printed == {key.replace("_", " "): str(value) for key, value in summary.items()}


async def test_export_writes_default_tables_as_csv(collected_db: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "exports" / "csv"

    assert main(["export", "--db", str(collected_db), "-o", str(out_dir)]) == 0

    with Store(collected_db) as store:
        collected = store.summary()["matches_collected"]
    participants = _csv_rows(out_dir / "participants.csv")
    teams = _csv_rows(out_dir / "teams.csv")
    assert participants[0] == [c.name for c in TABLES["participants"]]
    assert len(participants) - 1 == 10 * collected
    assert len(teams) - 1 == 2 * collected
    assert not (out_dir / "matches.csv").exists()


async def test_export_matches_table_includes_skipped_matches(
    collected_db: Path, tmp_path: Path
) -> None:
    out_dir = tmp_path / "exports"

    assert (
        main(["export", "--db", str(collected_db), "-o", str(out_dir), "--tables", "matches"]) == 0
    )

    with Store(collected_db) as store:
        summary = store.summary()
    rows = _csv_rows(out_dir / "matches.csv")
    assert len(rows) - 1 == summary["matches_collected"] + summary["matches_skipped"]


@pytest.mark.filterwarnings(
    "ignore:(Unknown extension|Conditional Formatting extension):UserWarning"
)
async def test_report_writes_workbook_with_player_sheet(collected_db: Path, tmp_path: Path) -> None:
    out = tmp_path / "out" / "report.xlsx"

    code = main(
        ["report", "--db", str(collected_db), "-o", str(out), "--player", "Player0#EUW",
         "--max-data-rows", "5"]
    )  # fmt: skip

    assert code == 0
    assert openpyxl.load_workbook(out).sheetnames == [
        "Overview",
        "Player",
        "Champions",
        "Matchups",
        "Roles",
        "Objectives",
        "Items",
        "Data",
        "Glossary",
    ]


async def test_report_defaults_to_dated_file_under_reports(
    collected_db: Path, tmp_path: Path
) -> None:
    assert main(["report", "--db", str(collected_db)]) == 0

    assert (tmp_path / "reports" / f"lol-report-{date.today():%Y%m%d}.xlsx").is_file()


async def test_report_with_no_matching_games_returns_1(collected_db: Path, tmp_path: Path) -> None:
    code = main(
        ["report", "--db", str(collected_db), "-o", str(tmp_path / "r.xlsx"), "--patch", "1.0"]
    )

    assert code == 1


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("﻿RIOT_TEST_KEY=bom-utf8".encode(), "bom-utf8"),
        ("RIOT_TEST_KEY=utf16\r\n".encode("utf-16"), "utf16"),
        (b"export RIOT_TEST_KEY=exported", "exported"),
        (b"RIOT_TEST_KEY=RGAPI-abc # dev key", "RGAPI-abc"),
        (b'RIOT_TEST_KEY="value # not a comment"', "value # not a comment"),
    ],
)
def test_load_dotenv_handles_common_file_variants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: bytes, expected: str
) -> None:
    monkeypatch.delenv("RIOT_TEST_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_bytes(content)

    load_dotenv(env_file)

    assert os.environ["RIOT_TEST_KEY"] == expected


def test_load_dotenv_fills_an_empty_environment_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RIOT_TEST_KEY", "")
    env_file = tmp_path / ".env"
    env_file.write_text("RIOT_TEST_KEY=from-file\n", encoding="utf-8")

    load_dotenv(env_file)

    assert os.environ["RIOT_TEST_KEY"] == "from-file"


async def test_export_into_a_path_blocked_by_a_file_returns_1(
    collected_db: Path, tmp_path: Path
) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")

    assert main(["export", "--db", str(collected_db), "-o", str(blocker / "out")]) == 1
