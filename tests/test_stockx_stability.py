import io
import sqlite3
import unittest

import pandas as pd

from src.db import init_db
from src.importer import import_sku_file
from src.progress import apply_stockx_progress_watermark


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


if __name__ == "__main__":
    unittest.main()
