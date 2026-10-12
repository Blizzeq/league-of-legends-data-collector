from pathlib import Path

from lol_collector.storage import Store


async def test_collected_db_has_ranked_matches(collected_db: Path) -> None:
    with Store(collected_db) as store:
        summary = store.summary()
    assert summary["matches_collected"] > 0
    assert summary["participant_rows"] == summary["matches_collected"] * 10
