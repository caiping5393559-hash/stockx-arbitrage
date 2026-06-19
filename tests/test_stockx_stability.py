import io
import sqlite3
import unittest

import pandas as pd

from src.db import init_db
from src.firebase_cloud import (
    CORE_BACKUP_TABLES,
    _remote_backup_opportunity_score_count,
    _remote_opportunity_score_count,
    _save_score_watermark,
    _should_skip_regressive_score_backup,
    _should_skip_regressive_sqlite_restore,
)
from src.importer import import_sku_file
from src.progress import apply_stockx_progress_watermark


class _FakeDoc:
    exists = True

    def __init__(self, data: dict) -> None:
        self._data = data

    def get(self) -> "_FakeDoc":
        return self

    def to_dict(self) -> dict:
        return self._data

    def set(self, data: dict, merge: bool = False) -> None:
        if merge:
            self._data.update(data)
        else:
            self._data = data


class _FakeCollection:
    def __init__(self, docs: dict[str, _FakeDoc]) -> None:
        self.docs = docs

    def document(self, name: str) -> _FakeDoc:
        return self.docs.get(name, _FakeDoc({}))


class _FakeFirestore:
    def __init__(self, docs: dict[str, _FakeDoc]) -> None:
        self.docs = docs

    def collection(self, _name: str) -> _FakeCollection:
        return _FakeCollection(self.docs)


class _FakeSettings:
    firebase_collection_prefix = "test"


class StockxStabilityTests(unittest.TestCase):
    def _memory_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_db(conn)
        return conn

    def test_progress_watermark_never_regresses(self) -> None:
        conn = self._memory_conn()
        try:
            first = apply_stockx_progress_watermark(
                conn,
                import_id=7,
                scored_styles=72,
                scored_sizes=1836,
                pending_styles=949,
            )
            second = apply_stockx_progress_watermark(
                conn,
                import_id=7,
                scored_styles=65,
                scored_sizes=1598,
                pending_styles=982,
            )
            self.assertEqual(first["scored_styles"], 72)
            self.assertEqual(first["scored_sizes"], 1836)
            self.assertEqual(first["pending_styles"], 949)
            self.assertEqual(second["scored_styles"], 72)
            self.assertEqual(second["scored_sizes"], 1836)
            self.assertEqual(second["pending_styles"], 949)
        finally:
            conn.close()

    def test_import_finds_fuzzy_style_column_and_normalizes_nike_numbers(self) -> None:
        conn = self._memory_conn()
        try:
            frame = pd.DataFrame(
                [
                    {"商品货号": "924453 100", "商品名": "Nike test", "排名": 1},
                    {"商品货号": "HQ6998-200", "商品名": "Jordan test", "排名": 2},
                ]
            )
            buffer = io.BytesIO()
            frame.to_excel(buffer, index=False)
            result = import_sku_file(
                conn,
                file_name="rank.xlsx",
                content=buffer.getvalue(),
                source_name="unit",
            )
            styles = [
                row["style_no"]
                for row in conn.execute("SELECT style_no FROM sku_items ORDER BY rank")
            ]
            self.assertEqual(result.rows_imported, 2)
            self.assertEqual(styles, ["924453-100", "HQ6998-200"])
        finally:
            conn.close()

    def test_cloud_backup_rejects_lower_score_snapshot(self) -> None:
        fake_db = _FakeFirestore(
            {
                "core_backup": _FakeDoc({"row_counts": {"opportunity_scores": 2156}}),
                "sqlite_backup": _FakeDoc({"row_counts": {"opportunity_scores": 2156}}),
            }
        )
        self.assertTrue(
            _should_skip_regressive_score_backup(
                fake_db,
                _FakeSettings(),
                {"opportunity_scores": 1598},
                "unit",
            )
        )
        self.assertFalse(
            _should_skip_regressive_score_backup(
                fake_db,
                _FakeSettings(),
                {"opportunity_scores": 2156},
                "unit",
            )
        )

    def test_remote_score_count_uses_monotonic_watermark(self) -> None:
        fake_db = _FakeFirestore(
            {
                "stockx_score_watermark": _FakeDoc({"opportunity_scores": 2537}),
                "core_backup": _FakeDoc({"row_counts": {"opportunity_scores": 1598}}),
                "sqlite_backup": _FakeDoc({"row_counts": {"opportunity_scores": 1598}}),
            }
        )
        self.assertEqual(_remote_opportunity_score_count(fake_db, _FakeSettings()), 2537)

    def test_backup_regression_check_respects_score_watermark_floor(self) -> None:
        fake_db = _FakeFirestore(
            {
                "stockx_score_watermark": _FakeDoc({"opportunity_scores": 2537}),
                "core_backup": _FakeDoc({"row_counts": {"opportunity_scores": 1598}}),
                "sqlite_backup": _FakeDoc({"row_counts": {"opportunity_scores": 1598}}),
            }
        )
        self.assertEqual(_remote_backup_opportunity_score_count(fake_db, _FakeSettings()), 1598)
        self.assertTrue(
            _should_skip_regressive_score_backup(
                fake_db,
                _FakeSettings(),
                {"opportunity_scores": 1836},
                "unit",
            )
        )
        self.assertTrue(
            _should_skip_regressive_score_backup(
                fake_db,
                _FakeSettings(),
                {"opportunity_scores": 1500},
                "unit",
            )
        )
        self.assertFalse(
            _should_skip_regressive_score_backup(
                fake_db,
                _FakeSettings(),
                {"opportunity_scores": 2537},
                "unit",
            )
        )

    def test_score_watermark_only_moves_up(self) -> None:
        watermark = _FakeDoc({"opportunity_scores": 1800})
        fake_db = _FakeFirestore({"stockx_score_watermark": watermark})
        _save_score_watermark(fake_db, _FakeSettings(), {"opportunity_scores": 1700}, "unit")
        self.assertEqual(watermark.to_dict()["opportunity_scores"], 1800)
        _save_score_watermark(fake_db, _FakeSettings(), {"opportunity_scores": 1900}, "unit")
        self.assertEqual(watermark.to_dict()["opportunity_scores"], 1900)

    def test_score_watermark_pending_only_moves_down_when_scores_equal(self) -> None:
        watermark = _FakeDoc(
            {
                "opportunity_scores": 2130,
                "scored_sizes": 2130,
                "scored_styles": 80,
                "pending_styles": 899,
            }
        )
        fake_db = _FakeFirestore({"stockx_score_watermark": watermark})
        _save_score_watermark(
            fake_db,
            _FakeSettings(),
            {"opportunity_scores": 2130, "scored_sizes": 2130, "scored_styles": 80, "pending_styles": 879},
            "unit",
        )
        self.assertEqual(watermark.to_dict()["pending_styles"], 879)
        _save_score_watermark(
            fake_db,
            _FakeSettings(),
            {"opportunity_scores": 2130, "scored_sizes": 2130, "scored_styles": 80, "pending_styles": 899},
            "unit",
        )
        self.assertEqual(watermark.to_dict()["pending_styles"], 879)

    def test_progress_watermark_table_is_in_cloud_backup_scope(self) -> None:
        self.assertIn("stockx_import_progress_watermarks", CORE_BACKUP_TABLES)

    def test_sqlite_restore_rejects_lower_score_snapshot(self) -> None:
        self.assertTrue(_should_skip_regressive_sqlite_restore(1598, 2537))
        self.assertFalse(_should_skip_regressive_sqlite_restore(2537, 2537))
        self.assertFalse(_should_skip_regressive_sqlite_restore(2600, 2537))


if __name__ == "__main__":
    unittest.main()
