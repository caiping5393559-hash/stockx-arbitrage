from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any

from .db import query_rows, utc_now
from .parsing import normalize_style_no


def add_trade(
    conn: sqlite3.Connection,
    *,
    style_no: str,
    size: str,
    side: str,
    quantity: int,
    price: float,
    trade_time: str,
    product_id: str | None = None,
    notes: str | None = None,
) -> None:
    style_value = normalize_style_no(style_no) or str(style_no).strip().upper()
    conn.execute(
        """
        INSERT INTO portfolio_trades (
            style_no, product_id, size, side, quantity, price, trade_time, notes, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (style_value, product_id, size, side, quantity, price, trade_time, notes, utc_now()),
    )
    conn.commit()


def portfolio_summary(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    trades = query_rows(conn, "SELECT * FROM portfolio_trades ORDER BY trade_time ASC, id ASC")
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "style_no": "",
            "size": "",
            "bought_qty": 0,
            "sold_qty": 0,
            "buy_cost": 0.0,
            "sell_revenue": 0.0,
        }
    )
    for row in trades:
        key = (row["style_no"], row["size"])
        item = grouped[key]
        item["style_no"] = row["style_no"]
        item["size"] = row["size"]
        quantity = int(row["quantity"])
        price = float(row["price"])
        if row["side"] == "buy":
            item["bought_qty"] += quantity
            item["buy_cost"] += quantity * price
        else:
            item["sold_qty"] += quantity
            item["sell_revenue"] += quantity * price

    summaries: list[dict[str, Any]] = []
    for (style_no, size), item in grouped.items():
        remaining_qty = item["bought_qty"] - item["sold_qty"]
        avg_cost = item["buy_cost"] / item["bought_qty"] if item["bought_qty"] else 0
        realized_cost = avg_cost * item["sold_qty"]
        lowest_ask = _current_lowest_ask(conn, style_no, size)
        action = "观察"
        if lowest_ask is not None and avg_cost:
            if lowest_ask < avg_cost:
                action = "降价或停止加仓"
            elif remaining_qty <= 0:
                action = "已清仓"
            else:
                action = "可继续观察"
        summaries.append(
            {
                "style_no": style_no,
                "size": size,
                "current_position": remaining_qty,
                "average_cost": avg_cost,
                "sold_qty": item["sold_qty"],
                "remaining_qty": remaining_qty,
                "realized_profit": item["sell_revenue"] - realized_cost,
                "current_lowest_ask": lowest_ask,
                "action": action,
            }
        )
    return summaries


def _current_lowest_ask(conn: sqlite3.Connection, style_no: str, size: str) -> float | None:
    row = conn.execute(
        """
        SELECT ask_price
        FROM ask_depth
        WHERE style_no = ?
          AND COALESCE(size, '') = COALESCE(?, '')
          AND snapshot_time = (
            SELECT MAX(snapshot_time)
            FROM ask_depth
            WHERE style_no = ?
              AND COALESCE(size, '') = COALESCE(?, '')
          )
        ORDER BY ask_price ASC
        LIMIT 1
        """,
        (style_no, size, style_no, size),
    ).fetchone()
    return float(row["ask_price"]) if row else None
