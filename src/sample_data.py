from __future__ import annotations

import random
import sqlite3
from datetime import datetime, timedelta, timezone

from .db import json_dumps, utc_now


SAMPLE_PRODUCTS = [
    {
        "product_id": "sample-aj1-lf-001",
        "style_no": "CZ0790-106",
        "title": "Air Jordan 1 Low OG Neutral Grey",
        "brand": "Jordan",
        "release_date": "2021-06-24",
        "sizes": {
            "8": {"asks": [(168, 2), (178, 1), (205, 4), (218, 6)], "bids": [(152, 3), (145, 4)], "base": 198},
            "9": {"asks": [(172, 1), (181, 2), (210, 5), (224, 5)], "bids": [(158, 2), (149, 3)], "base": 205},
            "10": {"asks": [(175, 2), (190, 2), (212, 4), (226, 5)], "bids": [(160, 4), (151, 3)], "base": 208},
        },
    },
    {
        "product_id": "sample-dunk-panda-002",
        "style_no": "DD1391-100",
        "title": "Nike Dunk Low Black White",
        "brand": "Nike",
        "release_date": "2021-03-10",
        "sizes": {
            "7": {"asks": [(95, 4), (99, 6), (104, 8)], "bids": [(82, 4), (79, 5)], "base": 101},
            "8": {"asks": [(98, 3), (101, 5), (107, 10)], "bids": [(84, 3), (80, 6)], "base": 103},
        },
    },
    {
        "product_id": "sample-new-release-003",
        "style_no": "HF0400-001",
        "title": "Sample Recent Runner",
        "brand": "Nike",
        "release_date": "2026-05-01",
        "sizes": {
            "9": {"asks": [(140, 1), (160, 1), (175, 2)], "bids": [(120, 1)], "base": 166},
        },
    },
]


def seed_sample_data(conn: sqlite3.Connection) -> None:
    now = datetime.now(timezone.utc)
    import_time = utc_now()
    cur = conn.execute(
        "INSERT INTO sku_imports (source_name, file_name, imported_at) VALUES (?, ?, ?)",
        ("sample_data", "sample_data/skus.csv", import_time),
    )
    import_id = int(cur.lastrowid)
    raw_rows = [{"rank": index + 1, "styleNo": product["style_no"], "title": product["title"]} for index, product in enumerate(SAMPLE_PRODUCTS)]
    conn.execute(
        """
        INSERT INTO sku_import_sheets (import_id, sheet_name, row_count, raw_table_json)
        VALUES (?, ?, ?, ?)
        """,
        (import_id, "CSV", len(raw_rows), json_dumps(raw_rows)),
    )

    for index, product in enumerate(SAMPLE_PRODUCTS):
        conn.execute(
            """
            INSERT INTO sku_items (
                import_id, sheet_name, style_no, sku, rank, title_hint, raw_row_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                import_id,
                "CSV",
                product["style_no"],
                product["style_no"],
                index + 1,
                product["title"],
                json_dumps(raw_rows[index]),
                import_time,
            ),
        )
        conn.execute(
            """
            INSERT INTO products (
                product_id, style_no, title, brand, release_date, image_url, raw_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(style_no) DO UPDATE SET
                product_id=excluded.product_id,
                title=excluded.title,
                brand=excluded.brand,
                release_date=excluded.release_date,
                raw_json=excluded.raw_json,
                updated_at=excluded.updated_at
            """,
            (
                product["product_id"],
                product["style_no"],
                product["title"],
                product["brand"],
                product["release_date"],
                None,
                json_dumps(product),
                import_time,
            ),
        )
        conn.execute(
            """
            INSERT INTO raw_api_responses (
                endpoint, params_json, status_code, response_json, error_message, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "/sample_seed",
                json_dumps({"styleNo": product["style_no"]}),
                200,
                json_dumps(product),
                None,
                import_time,
            ),
        )
        _seed_depth_and_sales(conn, product, now)

    conn.commit()


def _seed_depth_and_sales(conn: sqlite3.Connection, product: dict, now: datetime) -> None:
    snapshot_time = utc_now()
    rng = random.Random(product["style_no"])
    for size, data in product["sizes"].items():
        for price, quantity in data["asks"]:
            conn.execute(
                """
                INSERT INTO ask_depth (
                    product_id, style_no, size, ask_price, ask_quantity,
                    service_level, is_consigned, snapshot_time, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product["product_id"],
                    product["style_no"],
                    size,
                    price,
                    quantity,
                    "standard",
                    0,
                    snapshot_time,
                    json_dumps({"price": price, "quantity": quantity, "size": size}),
                ),
            )
        for price, quantity in data["bids"]:
            conn.execute(
                """
                INSERT INTO bid_depth (
                    product_id, style_no, size, bid_price, bid_quantity, snapshot_time, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product["product_id"],
                    product["style_no"],
                    size,
                    price,
                    quantity,
                    snapshot_time,
                    json_dumps({"price": price, "quantity": quantity, "size": size}),
                ),
            )
        sales_count = 24 if product["style_no"] != "HF0400-001" else 8
        for day_index in range(sales_count):
            days_ago = rng.randint(0, 29)
            created_at = (now - timedelta(days=days_ago, hours=rng.randint(0, 23))).replace(microsecond=0).isoformat()
            amount = data["base"] + rng.randint(-12, 18)
            conn.execute(
                """
                INSERT INTO sales_history (
                    product_id, style_no, size, amount, created_at, order_type,
                    source_endpoint, raw_json, synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product["product_id"],
                    product["style_no"],
                    size,
                    amount,
                    created_at,
                    "SALE",
                    "/sample_seed",
                    json_dumps({"amount": amount, "createdAt": created_at, "size": size, "orderType": "SALE"}),
                    utc_now(),
                ),
            )

