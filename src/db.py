from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import get_settings


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def json_loads(value: str | None, default: Any = None) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def get_db_path() -> Path:
    return get_settings().db_path


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=60, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def db_session(db_path: Path | None = None) -> Iterable[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    for column_name, column_def in columns.items():
        if column_name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sku_imports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            file_name TEXT,
            imported_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sku_import_sheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id INTEGER NOT NULL,
            sheet_name TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            raw_table_json TEXT NOT NULL,
            FOREIGN KEY(import_id) REFERENCES sku_imports(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS sku_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id INTEGER NOT NULL,
            sheet_name TEXT,
            style_no TEXT NOT NULL,
            sku TEXT,
            rank INTEGER,
            title_hint TEXT,
            raw_row_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(import_id) REFERENCES sku_imports(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_sku_items_style_no ON sku_items(style_no);

        CREATE TABLE IF NOT EXISTS stockx_style_sync_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id INTEGER,
            style_no TEXT NOT NULL,
            status TEXT NOT NULL,
            product_id TEXT,
            sizes_count INTEGER DEFAULT 0,
            sales_rows INTEGER DEFAULT 0,
            ask_rows INTEGER DEFAULT 0,
            bid_rows INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            message TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(import_id, style_no)
        );
        CREATE INDEX IF NOT EXISTS idx_stockx_style_sync_status_style
            ON stockx_style_sync_status(style_no);

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            style_no TEXT NOT NULL,
            title TEXT,
            brand TEXT,
            release_date TEXT,
            image_url TEXT,
            raw_json TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(style_no)
        );
        CREATE INDEX IF NOT EXISTS idx_products_product_id ON products(product_id);

        CREATE TABLE IF NOT EXISTS release_date_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            style_no TEXT NOT NULL,
            title TEXT,
            release_date TEXT,
            source_name TEXT NOT NULL,
            source_url TEXT,
            confidence REAL,
            raw_text TEXT,
            fetched_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_release_date_sources_style
            ON release_date_sources(style_no, fetched_at);

        CREATE TABLE IF NOT EXISTS reference_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            style_no TEXT NOT NULL,
            size TEXT NOT NULL DEFAULT '',
            source_name TEXT NOT NULL,
            price REAL NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            note TEXT,
            raw_json TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(style_no, size, source_name)
        );
        CREATE INDEX IF NOT EXISTS idx_reference_prices_style_size
            ON reference_prices(style_no, size, updated_at);

        CREATE TABLE IF NOT EXISTS product_sizes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            style_no TEXT NOT NULL,
            size TEXT NOT NULL,
            raw_json TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(style_no, size)
        );
        CREATE INDEX IF NOT EXISTS idx_product_sizes_style
            ON product_sizes(style_no, size);

        CREATE TABLE IF NOT EXISTS raw_api_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT NOT NULL,
            params_json TEXT,
            status_code INTEGER,
            response_json TEXT,
            error_message TEXT,
            fetched_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_raw_endpoint_time
            ON raw_api_responses(endpoint, fetched_at);

        CREATE TABLE IF NOT EXISTS sync_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            severity TEXT NOT NULL,
            event_type TEXT NOT NULL,
            endpoint TEXT,
            style_no TEXT,
            product_id TEXT,
            size TEXT,
            message TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sync_logs_time ON sync_logs(created_at);

        CREATE TABLE IF NOT EXISTS market_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            style_no TEXT NOT NULL,
            size TEXT,
            lowest_ask REAL,
            highest_bid REAL,
            last_sale REAL,
            market_price REAL,
            raw_json TEXT NOT NULL,
            snapshot_time TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_market_style_size
            ON market_snapshots(style_no, size, snapshot_time);

        CREATE TABLE IF NOT EXISTS sales_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            style_no TEXT NOT NULL,
            size TEXT,
            amount REAL,
            created_at TEXT,
            order_type TEXT,
            source_endpoint TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            synced_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sales_style_size_time
            ON sales_history(style_no, size, created_at);

        CREATE TABLE IF NOT EXISTS ask_depth (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            style_no TEXT NOT NULL,
            size TEXT,
            ask_price REAL NOT NULL,
            ask_quantity INTEGER NOT NULL DEFAULT 1,
            service_level TEXT,
            is_consigned INTEGER NOT NULL DEFAULT 0,
            snapshot_time TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ask_depth_style_size
            ON ask_depth(style_no, size, snapshot_time);

        CREATE TABLE IF NOT EXISTS bid_depth (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            style_no TEXT NOT NULL,
            size TEXT,
            bid_price REAL NOT NULL,
            bid_quantity INTEGER NOT NULL DEFAULT 1,
            snapshot_time TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_bid_depth_style_size
            ON bid_depth(style_no, size, snapshot_time);

        CREATE TABLE IF NOT EXISTS opportunity_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT,
            style_no TEXT NOT NULL,
            title TEXT,
            brand TEXT,
            size TEXT NOT NULL,
            score REAL NOT NULL,
            rating TEXT NOT NULL,
            recommended_buy_qty INTEGER NOT NULL DEFAULT 0,
            max_buy_price REAL,
            weighted_avg_cost REAL,
            next_lowest_ask REAL,
            target_sell_price_low REAL,
            target_sell_price_high REAL,
            estimated_profit REAL,
            estimated_profit_per_pair REAL,
            estimated_days_to_sell REAL,
            release_date TEXT,
            release_days INTEGER,
            risk_notes TEXT,
            components_json TEXT NOT NULL,
            computed_at TEXT NOT NULL,
            UNIQUE(style_no, size)
        );
        CREATE INDEX IF NOT EXISTS idx_opportunity_rating
            ON opportunity_scores(rating, score);

        CREATE TABLE IF NOT EXISTS opportunity_import_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id INTEGER NOT NULL,
            source_name TEXT,
            file_name TEXT,
            row_count INTEGER NOT NULL DEFAULT 0,
            style_count INTEGER NOT NULL DEFAULT 0,
            score_count INTEGER NOT NULL DEFAULT 0,
            archived_at TEXT NOT NULL,
            note TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_opportunity_snapshots_import
            ON opportunity_import_snapshots(import_id, archived_at);

        CREATE TABLE IF NOT EXISTS opportunity_score_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            original_score_id INTEGER,
            product_id TEXT,
            style_no TEXT NOT NULL,
            title TEXT,
            brand TEXT,
            size TEXT NOT NULL,
            score REAL NOT NULL,
            rating TEXT NOT NULL,
            recommended_buy_qty INTEGER NOT NULL DEFAULT 0,
            max_buy_price REAL,
            weighted_avg_cost REAL,
            next_lowest_ask REAL,
            target_sell_price_low REAL,
            target_sell_price_high REAL,
            estimated_profit REAL,
            estimated_profit_per_pair REAL,
            estimated_days_to_sell REAL,
            release_date TEXT,
            release_days INTEGER,
            risk_notes TEXT,
            components_json TEXT NOT NULL,
            computed_at TEXT NOT NULL,
            archived_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_opportunity_history_snapshot
            ON opportunity_score_history(snapshot_id, score, estimated_profit);

        CREATE TABLE IF NOT EXISTS goat_consignment_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_batch TEXT NOT NULL,
            warehouse_id TEXT,
            warehouse_name TEXT,
            pid TEXT NOT NULL,
            product_template_id TEXT,
            style_no TEXT NOT NULL,
            size TEXT NOT NULL,
            title TEXT,
            sale_status TEXT,
            goat_price REAL NOT NULL,
            buy_cost REAL NOT NULL,
            raw_row_json TEXT NOT NULL,
            imported_at TEXT NOT NULL,
            UNIQUE(import_batch, pid, style_no, size)
        );
        CREATE INDEX IF NOT EXISTS idx_goat_consignment_style_size
            ON goat_consignment_items(style_no, size, imported_at);

        CREATE TABLE IF NOT EXISTS goat_hidden_styles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            style_no TEXT NOT NULL UNIQUE,
            hidden_at TEXT NOT NULL,
            note TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_goat_hidden_styles_style
            ON goat_hidden_styles(style_no);

        CREATE TABLE IF NOT EXISTS goat_consignment_import_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            archive_batch TEXT NOT NULL,
            archived_at TEXT NOT NULL,
            new_source_name TEXT,
            previous_batches TEXT,
            item_count INTEGER NOT NULL DEFAULT 0,
            score_count INTEGER NOT NULL DEFAULT 0,
            note TEXT
        );

        CREATE TABLE IF NOT EXISTS goat_consignment_history_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            archive_id INTEGER NOT NULL,
            original_item_id INTEGER,
            import_batch TEXT NOT NULL,
            warehouse_id TEXT,
            warehouse_name TEXT,
            pid TEXT NOT NULL,
            product_template_id TEXT,
            style_no TEXT NOT NULL,
            size TEXT NOT NULL,
            title TEXT,
            sale_status TEXT,
            goat_price REAL NOT NULL,
            buy_cost REAL NOT NULL,
            raw_row_json TEXT NOT NULL,
            imported_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_goat_history_items_archive
            ON goat_consignment_history_items(archive_id, style_no, size);

        CREATE TABLE IF NOT EXISTS goat_consignment_history_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            archive_id INTEGER NOT NULL,
            original_score_id INTEGER,
            original_item_id INTEGER,
            score REAL NOT NULL,
            rating TEXT NOT NULL,
            style_no TEXT NOT NULL,
            size TEXT NOT NULL,
            matched_stockx_size TEXT,
            pid TEXT NOT NULL,
            title TEXT,
            goat_price REAL,
            buy_cost REAL,
            stockx_lowest_ask REAL,
            ask_snapshot_time TEXT,
            sales_7d INTEGER,
            sales_30d INTEGER,
            avg_7d REAL,
            avg_30d REAL,
            estimated_sell_price REAL,
            estimated_profit REAL,
            estimated_profit_rate REAL,
            estimated_days_to_sell REAL,
            risk_notes TEXT,
            components_json TEXT NOT NULL,
            computed_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_goat_history_scores_archive
            ON goat_consignment_history_scores(archive_id, score, estimated_days_to_sell);

        CREATE TABLE IF NOT EXISTS goat_consignment_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            score REAL NOT NULL,
            rating TEXT NOT NULL,
            style_no TEXT NOT NULL,
            size TEXT NOT NULL,
            matched_stockx_size TEXT,
            pid TEXT NOT NULL,
            title TEXT,
            goat_price REAL,
            buy_cost REAL,
            stockx_lowest_ask REAL,
            ask_snapshot_time TEXT,
            sales_7d INTEGER,
            sales_30d INTEGER,
            avg_7d REAL,
            avg_30d REAL,
            estimated_sell_price REAL,
            estimated_profit REAL,
            estimated_profit_rate REAL,
            estimated_days_to_sell REAL,
            risk_notes TEXT,
            components_json TEXT NOT NULL,
            computed_at TEXT NOT NULL,
            UNIQUE(item_id)
        );
        CREATE INDEX IF NOT EXISTS idx_goat_consignment_scores_rank
            ON goat_consignment_scores(score, estimated_days_to_sell);

        CREATE TABLE IF NOT EXISTS portfolio_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            style_no TEXT NOT NULL,
            product_id TEXT,
            size TEXT NOT NULL,
            side TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            trade_time TEXT NOT NULL,
            notes TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_style_size
            ON portfolio_trades(style_no, size, trade_time);
        """
    )
    _ensure_columns(
        conn,
        "opportunity_scores",
        {
            "sales_7d": "sales_7d INTEGER DEFAULT 0",
            "sales_30d": "sales_30d INTEGER DEFAULT 0",
            "last_sale_at": "last_sale_at TEXT",
            "last_sale_days": "last_sale_days INTEGER",
            "estimated_profit_per_pair": "estimated_profit_per_pair REAL",
        },
    )
    _ensure_columns(
        conn,
        "goat_consignment_scores",
        {
            "matched_stockx_size": "matched_stockx_size TEXT",
        },
    )
def _dedupe_sales_history(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        DELETE FROM sales_history
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM sales_history
            GROUP BY
                style_no,
                COALESCE(size, ''),
                COALESCE(created_at, ''),
                COALESCE(amount, -1),
                COALESCE(order_type, '')
        )
        """
    )


def _index_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _dedupe_market_snapshots(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        DELETE FROM market_snapshots
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM market_snapshots
            GROUP BY
                style_no,
                COALESCE(size, ''),
                COALESCE(lowest_ask, -1),
                COALESCE(highest_bid, -1),
                COALESCE(last_sale, -1),
                COALESCE(market_price, -1),
                snapshot_time
        )
        """
    )


def _dedupe_ask_depth(conn: sqlite3.Connection) -> None:
    duplicate = conn.execute(
        """
        SELECT 1
        FROM ask_depth
        GROUP BY
            style_no,
            COALESCE(size, ''),
            ask_price,
            COALESCE(service_level, ''),
            is_consigned,
            snapshot_time
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    ).fetchone()
    if duplicate is None:
        return
    conn.execute("DROP TABLE IF EXISTS temp.ask_depth_dedupe")
    conn.execute(
        """
        CREATE TEMP TABLE ask_depth_dedupe AS
        SELECT
            MIN(id) AS keep_id,
            style_no,
            COALESCE(size, '') AS size_key,
            ask_price,
            COALESCE(service_level, '') AS service_level_key,
            is_consigned,
            snapshot_time,
            SUM(COALESCE(ask_quantity, 1)) AS merged_quantity
        FROM ask_depth
        GROUP BY
            style_no,
            COALESCE(size, ''),
            ask_price,
            COALESCE(service_level, ''),
            is_consigned,
            snapshot_time
        """
    )
    conn.execute(
        """
        UPDATE ask_depth
        SET ask_quantity = (
            SELECT merged_quantity
            FROM ask_depth_dedupe
            WHERE ask_depth_dedupe.keep_id = ask_depth.id
        )
        WHERE id IN (SELECT keep_id FROM ask_depth_dedupe)
        """
    )
    conn.execute(
        """
        DELETE FROM ask_depth
        WHERE EXISTS (
            SELECT 1
            FROM ask_depth_dedupe
            WHERE ask_depth_dedupe.style_no = ask_depth.style_no
              AND ask_depth_dedupe.size_key = COALESCE(ask_depth.size, '')
              AND ask_depth_dedupe.ask_price = ask_depth.ask_price
              AND ask_depth_dedupe.service_level_key = COALESCE(ask_depth.service_level, '')
              AND ask_depth_dedupe.is_consigned = ask_depth.is_consigned
              AND ask_depth_dedupe.snapshot_time = ask_depth.snapshot_time
              AND ask_depth_dedupe.keep_id <> ask_depth.id
        )
        """
    )
    conn.execute("DROP TABLE IF EXISTS temp.ask_depth_dedupe")


def _dedupe_bid_depth(conn: sqlite3.Connection) -> None:
    duplicate = conn.execute(
        """
        SELECT 1
        FROM bid_depth
        GROUP BY
            style_no,
            COALESCE(size, ''),
            bid_price,
            snapshot_time
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    ).fetchone()
    if duplicate is None:
        return
    conn.execute("DROP TABLE IF EXISTS temp.bid_depth_dedupe")
    conn.execute(
        """
        CREATE TEMP TABLE bid_depth_dedupe AS
        SELECT
            MIN(id) AS keep_id,
            style_no,
            COALESCE(size, '') AS size_key,
            bid_price,
            snapshot_time,
            SUM(COALESCE(bid_quantity, 1)) AS merged_quantity
        FROM bid_depth
        GROUP BY
            style_no,
            COALESCE(size, ''),
            bid_price,
            snapshot_time
        """
    )
    conn.execute(
        """
        UPDATE bid_depth
        SET bid_quantity = (
            SELECT merged_quantity
            FROM bid_depth_dedupe
            WHERE bid_depth_dedupe.keep_id = bid_depth.id
        )
        WHERE id IN (SELECT keep_id FROM bid_depth_dedupe)
        """
    )
    conn.execute(
        """
        DELETE FROM bid_depth
        WHERE EXISTS (
            SELECT 1
            FROM bid_depth_dedupe
            WHERE bid_depth_dedupe.style_no = bid_depth.style_no
              AND bid_depth_dedupe.size_key = COALESCE(bid_depth.size, '')
              AND bid_depth_dedupe.bid_price = bid_depth.bid_price
              AND bid_depth_dedupe.snapshot_time = bid_depth.snapshot_time
              AND bid_depth_dedupe.keep_id <> bid_depth.id
        )
        """
    )
    conn.execute("DROP TABLE IF EXISTS temp.bid_depth_dedupe")


def log_sync(
    conn: sqlite3.Connection,
    message: str,
    *,
    severity: str = "info",
    event_type: str = "sync",
    endpoint: str | None = None,
    style_no: str | None = None,
    product_id: str | None = None,
    size: str | None = None,
    details: Any | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO sync_logs (
            severity, event_type, endpoint, style_no, product_id, size,
            message, details_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            severity,
            event_type,
            endpoint,
            style_no,
            product_id,
            size,
            message,
            json_dumps(details) if details is not None else None,
            utc_now(),
        ),
    )


def save_raw_response(
    conn: sqlite3.Connection,
    endpoint: str,
    params: dict[str, Any] | None,
    *,
    status_code: int | None = None,
    response: Any | None = None,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO raw_api_responses (
            endpoint, params_json, status_code, response_json, error_message, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            endpoint,
            json_dumps(params or {}),
            status_code,
            json_dumps(response) if response is not None else None,
            error_message,
            utc_now(),
        ),
    )


def query_rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, params).fetchall())


def upsert_reference_price(
    conn: sqlite3.Connection,
    *,
    style_no: str,
    source_name: str,
    price: float,
    size: str | None = None,
    currency: str = "USD",
    note: str | None = None,
    raw_json: Any | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO reference_prices (
            style_no, size, source_name, price, currency, note, raw_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(style_no, size, source_name) DO UPDATE SET
            price=excluded.price,
            currency=excluded.currency,
            note=excluded.note,
            raw_json=excluded.raw_json,
            updated_at=excluded.updated_at
        """,
        (
            style_no,
            str(size or "").strip(),
            source_name,
            float(price),
            currency or "USD",
            note,
            json_dumps(raw_json) if raw_json is not None else None,
            utc_now(),
        ),
    )


def query_reference_prices(
    conn: sqlite3.Connection,
    style_no: str,
    *,
    size: str | None = None,
) -> list[sqlite3.Row]:
    params: tuple[Any, ...]
    if size is None or str(size).strip() == "":
        params = (style_no,)
        sql = """
            SELECT source_name, size, price, currency, note, raw_json, updated_at
            FROM reference_prices
            WHERE style_no = ? AND size = ''
            ORDER BY
                CASE source_name
                    WHEN 'GOAT' THEN 0
                    WHEN 'manual' THEN 1
                    WHEN 'import' THEN 2
                    ELSE 3
                END,
                updated_at DESC
        """
    else:
        params = (style_no, str(size).strip(), style_no)
        sql = """
            SELECT source_name, size, price, currency, note, raw_json, updated_at
            FROM reference_prices
            WHERE style_no = ? AND (size = ? OR size = '')
            ORDER BY
                CASE WHEN size = ? THEN 0 ELSE 1 END,
                CASE source_name
                    WHEN 'GOAT' THEN 0
                    WHEN 'manual' THEN 1
                    WHEN 'import' THEN 2
                    ELSE 3
                END,
                updated_at DESC
        """
    return query_rows(conn, sql, params)


def get_reference_price(
    conn: sqlite3.Connection,
    style_no: str,
    *,
    size: str | None = None,
) -> float | None:
    rows = query_reference_prices(conn, style_no, size=size)
    if not rows:
        return None
    for row in rows:
        price = row["price"]
        if price is not None:
            try:
                return float(price)
            except (TypeError, ValueError):
                continue
    return None


def scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return None
    return row[0]
