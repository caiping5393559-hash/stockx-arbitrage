from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any, Callable

from .db import get_reference_price, json_dumps, json_loads, query_rows, utc_now
from .parsing import extract_release_date, extract_sizes_from_product, normalize_style_no
from .release_dates import lookup_release_date


RATING_ORDER = {"S": 5, "A": 4, "B+": 3, "B": 2, "C": 1, "D": 0}
ProgressCallback = Callable[[dict[str, Any]], None]


@dataclass
class SalesStats:
    sales_7d: int
    sales_14d: int
    sales_30d: int
    median: float | None
    p75: float | None
    p90: float | None
    last_sale_at: str | None = None
    last_sale_days: int | None = None
    last_sale_amount: float | None = None
    avg_7d: float | None = None
    avg_30d: float | None = None


def percentile(values: list[float], pct: float) -> float | None:
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * pct
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return clean[int(position)]
    lower_value = clean[lower]
    upper_value = clean[upper]
    return lower_value + (upper_value - lower_value) * (position - lower)


def _daily_sales_velocity(sales: SalesStats) -> float:
    return max(
        sales.sales_7d / 7 if sales.sales_7d else 0.0,
        sales.sales_14d / 14 if sales.sales_14d else 0.0,
        sales.sales_30d / 30 if sales.sales_30d else 0.0,
    )


def parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                parsed = datetime.strptime(text[: len(fmt)], fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def release_days(release_date: str | None) -> int | None:
    parsed = parse_datetime(release_date)
    if parsed is None:
        return None
    return max(0, (datetime.now(timezone.utc) - parsed).days)


def get_sales_stats(conn: sqlite3.Connection, style_no: str, size: str | None) -> SalesStats:
    rows = query_rows(
        conn,
        """
        SELECT amount, created_at, order_type
        FROM sales_history
        WHERE style_no = ? AND COALESCE(size, '') = COALESCE(?, '')
        """,
        (style_no, size),
    )
    now = datetime.now(timezone.utc)
    amounts_7d: list[float] = []
    amounts_30d: list[float] = []
    latest_sale_at: datetime | None = None
    latest_sale_amount: float | None = None
    sales_7d = sales_14d = sales_30d = 0
    seen_sales: set[tuple[str, str, str]] = set()
    for row in rows:
        created_text = str(row["created_at"] or "")
        amount_text = str(row["amount"] if row["amount"] is not None else "")
        order_type_text = str(row["order_type"] or "")
        sale_key = (created_text, amount_text, order_type_text)
        if sale_key in seen_sales:
            continue
        seen_sales.add(sale_key)
        created_at = parse_datetime(created_text)
        if created_at is None:
            continue
        if latest_sale_at is None or created_at > latest_sale_at:
            latest_sale_at = created_at
            amount_value = row["amount"]
            latest_sale_amount = float(amount_value) if amount_value is not None else None
        days = (now - created_at).days
        amount = row["amount"]
        if 0 <= days <= 30:
            sales_30d += 1
            if amount is not None:
                amounts_30d.append(float(amount))
        if 0 <= days <= 14:
            sales_14d += 1
        if 0 <= days <= 7:
            sales_7d += 1
            if amount is not None:
                amounts_7d.append(float(amount))
    return SalesStats(
        sales_7d=sales_7d,
        sales_14d=sales_14d,
        sales_30d=sales_30d,
        median=median(amounts_30d) if amounts_30d else None,
        p75=percentile(amounts_30d, 0.75),
        p90=percentile(amounts_30d, 0.90),
        last_sale_at=latest_sale_at.isoformat() if latest_sale_at else None,
        last_sale_days=(now - latest_sale_at).days if latest_sale_at else None,
        last_sale_amount=latest_sale_amount,
        avg_7d=round(sum(amounts_7d) / len(amounts_7d), 2) if amounts_7d else None,
        avg_30d=round(sum(amounts_30d) / len(amounts_30d), 2) if amounts_30d else None,
    )


def derive_target_sell_prices(
    sales: SalesStats,
    *,
    fallback_price: float | None = None,
    analysis_qty: int = 1,
) -> tuple[float | None, float | None, str]:
    source = "missing"
    if sales.sales_30d >= 15 and sales.median is not None:
        low = float(sales.median)
        high = float(sales.p75 if sales.p75 is not None else sales.median)
        source = "fast_median/p75"
    elif sales.sales_30d >= 5 and sales.median is not None and sales.p75 is not None:
        low = float(sales.median)
        high = float(sales.p75)
        source = "fast_median/p75"
    elif sales.sales_30d >= 5 and sales.median is not None and sales.p90 is not None:
        low = float(sales.median)
        high = float(sales.p90)
        source = "fast_median/p90"
    elif sales.median is not None:
        low = float(sales.median)
        high = float(sales.p75 if sales.sales_30d >= 5 and sales.p75 is not None else sales.median)
        source = "low_volume_median" if sales.sales_30d < 5 else "fast_median"
    elif sales.p75 is not None:
        low = high = float(sales.p75)
        source = "sales_p75"
    elif sales.p90 is not None:
        low = high = float(sales.p90)
        source = "sales_p90"
    elif sales.last_sale_amount is not None:
        low = high = float(sales.last_sale_amount)
        source = "sales_last_sale"
    elif fallback_price is not None:
        return float(fallback_price), float(fallback_price), "fallback"
    else:
        return None, None, "missing"

    qty = max(1, int(analysis_qty or 1))
    velocity = _daily_sales_velocity(sales)
    expected_days = qty / velocity if velocity else math.inf
    pressure_discount = 0.0
    if expected_days > 21:
        pressure_discount += min(0.15, (expected_days - 21) * 0.01)
    elif expected_days > 14:
        pressure_discount += min(0.08, (expected_days - 14) * 0.008)

    if sales.last_sale_days is not None:
        if sales.last_sale_days > 30:
            pressure_discount += 0.06
        elif sales.last_sale_days > 14:
            pressure_discount += 0.03
    elif sales.sales_30d == 0:
        pressure_discount += 0.05

    if sales.sales_30d > 0 and sales.sales_7d == 0:
        pressure_discount += 0.03

    pressure_discount = min(0.18, pressure_discount)
    low *= 1 - pressure_discount
    high *= 1 - pressure_discount / 2
    if high < low:
        high = low
    return round(low, 2), round(high, 2), source


def _legacy_derive_target_sell_prices(
    sales: SalesStats,
    *,
    fallback_price: float | None = None,
) -> tuple[float | None, float | None, str]:
    if sales.p75 is not None and sales.p90 is not None:
        return sales.p75, sales.p90, "p75/p90"
    if sales.p75 is not None:
        return sales.p75, sales.p75, "p75"
    if sales.p90 is not None:
        return sales.p90, sales.p90, "p90"
    if sales.median is not None:
        return sales.median, sales.median, "median"
    if sales.last_sale_amount is not None:
        return sales.last_sale_amount, sales.last_sale_amount, "last_sale"
    if fallback_price is not None:
        return fallback_price, fallback_price, "fallback"
    return None, None, "missing"


def latest_ask_rows(conn: sqlite3.Connection, style_no: str, size: str | None) -> list[dict[str, Any]]:
    rows = query_rows(
        conn,
        """
        SELECT *
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
        """,
        (style_no, size, style_no, size),
    )
    return [dict(row) for row in rows]


def latest_bid_rows(conn: sqlite3.Connection, style_no: str, size: str | None) -> list[dict[str, Any]]:
    rows = query_rows(
        conn,
        """
        SELECT *
        FROM bid_depth
        WHERE style_no = ?
          AND COALESCE(size, '') = COALESCE(?, '')
          AND snapshot_time = (
            SELECT MAX(snapshot_time)
            FROM bid_depth
            WHERE style_no = ?
              AND COALESCE(size, '') = COALESCE(?, '')
          )
        ORDER BY bid_price DESC
        """,
        (style_no, size, style_no, size),
    )
    return [dict(row) for row in rows]


def latest_market_rows(conn: sqlite3.Connection, style_no: str, size: str | None) -> list[dict[str, Any]]:
    rows = query_rows(
        conn,
        """
        SELECT *
        FROM market_snapshots
        WHERE style_no = ?
          AND COALESCE(size, '') = COALESCE(?, '')
          AND snapshot_time = (
            SELECT MAX(snapshot_time)
            FROM market_snapshots
            WHERE style_no = ?
              AND COALESCE(size, '') = COALESCE(?, '')
          )
        ORDER BY lowest_ask ASC
        """,
        (style_no, size, style_no, size),
    )
    return [dict(row) for row in rows]


def rating_from_score(score: float) -> str:
    if score >= 90:
        return "S"
    if score >= 80:
        return "A"
    if score >= 70:
        return "B+"
    if score >= 60:
        return "B"
    if score >= 40:
        return "C"
    return "D"



def _sell_strategy_priority(name: str) -> int:
    return {"快速出货": 0, "平衡出货": 1, "控盘提价": 2, "无法定价": 3}.get(str(name), 9)


def _format_buy_plan_text(buy_plan: list[dict[str, Any]] | None) -> str:
    parts: list[str] = []
    for level in buy_plan or []:
        price = level.get("price")
        qty = level.get("quantity")
        if price is None or qty in (None, ""):
            continue
        qty_int = int(float(qty))
        consigned_qty = int(float(level.get("consigned_qty") or 0))
        seller_qty = int(float(level.get("seller_qty") or 0))
        known_qty = consigned_qty + seller_qty
        mix_parts: list[str] = []
        if known_qty > 0:
            mix_parts.append(f"寄存 {consigned_qty}")
            mix_parts.append(f"挂售 {seller_qty}")
            if known_qty < qty_int:
                mix_parts.append(f"未识别 {qty_int - known_qty}")
        mix_text = f"（{' / '.join(mix_parts)}）" if mix_parts else ""
        parts.append(f"USD ${float(price):,.2f} x {qty_int}{mix_text}")
    return " / ".join(parts) if parts else "-"


def _strategy_option_score(item: dict[str, Any]) -> tuple[float, float, float, float, float, float]:
    days = float(item.get("estimated_days_to_sell") or math.inf)
    if not math.isfinite(days) or days <= 0:
        days = 9999.0
    profit = float(item.get("estimated_profit") or 0)
    profit_per_pair = float(item.get("estimated_profit_per_pair") or 0)
    qualifies = 1.0 if item.get("qualifies") else 0.0
    consigned_ratio = float(item.get("consigned_ratio") or 0)
    turnover = profit_per_pair / max(days, 1.0)
    return (qualifies, turnover, profit, profit_per_pair, consigned_ratio, -days)


def _sellout_speed_score(days_to_sell: float) -> float:
    if not math.isfinite(days_to_sell) or days_to_sell <= 0:
        return 0.0
    if days_to_sell <= 3:
        return 10.0
    if days_to_sell <= 7:
        return 9.0
    if days_to_sell <= 14:
        return 7.0
    if days_to_sell <= 21:
        return 5.0
    if days_to_sell <= 30:
        return 2.0
    return 0.0


def _price_proximity_score(target: Any, reference: Any, *, points: float) -> float:
    if target in (None, "") or reference in (None, ""):
        return points * 0.45
    target_value = float(target)
    reference_value = float(reference)
    if target_value <= 0 or reference_value <= 0:
        return points * 0.45
    premium = (target_value / reference_value) - 1
    if premium <= 0:
        return points
    if premium <= 0.03:
        return points * 0.85
    if premium <= 0.07:
        return points * 0.60
    if premium <= 0.10:
        return points * 0.35
    return 0.0


def _ask_availability_days(row: dict[str, Any]) -> float:
    if bool(row.get("is_consigned")):
        return 0.2
    service_level = str(row.get("service_level") or "").strip()
    if service_level == "expressExpedited":
        return 1.0
    if service_level == "expressStandard":
        return 4.0
    return 8.0


def _merge_ask_levels(ask_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[float, dict[str, Any]] = {}
    for row in ask_rows:
        price = row.get("ask_price")
        if price is None:
            continue
        price_value = round(float(price), 2)
        quantity = max(1, int(row.get("ask_quantity") or 1))
        is_consigned = bool(row.get("is_consigned"))
        consigned_qty = quantity if is_consigned else 0
        seller_qty = quantity - consigned_qty
        service_level = str(row.get("service_level") or "unknown").strip() or "unknown"
        delay = _ask_availability_days(row)
        level = merged.setdefault(
            price_value,
            {
                "price": price_value,
                "quantity": 0,
                "consigned_qty": 0,
                "seller_qty": 0,
                "service_level_counts": {},
                "delay_sum": 0.0,
            },
        )
        level["quantity"] += quantity
        level["consigned_qty"] += consigned_qty
        level["seller_qty"] += seller_qty
        level["delay_sum"] += delay * quantity
        level["service_level_counts"][service_level] = level["service_level_counts"].get(service_level, 0) + quantity

    levels = sorted(merged.values(), key=lambda item: float(item["price"]))
    for level in levels:
        qty = int(level["quantity"] or 0)
        level["availability_delay_days"] = round(float(level.pop("delay_sum", 0.0)) / qty, 2) if qty else 0.0
        level["consigned_ratio"] = round(int(level["consigned_qty"]) / qty, 4) if qty else 0.0
        level["seller_ratio"] = round(int(level["seller_qty"]) / qty, 4) if qty else 0.0
    return levels


def _size_sort_key(size: str) -> tuple[int, float | str]:
    text = str(size).upper().replace("US", "").strip()
    try:
        return (0, float(text.replace("W", "")))
    except ValueError:
        return (1, text)


def _numeric_us_size(size: Any) -> float | None:
    text = str(size or "").upper().replace("US", "").replace("W", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def latest_lowest_ask_by_size(conn: sqlite3.Connection, style_no: str) -> dict[str, dict[str, Any]]:
    rows = query_rows(
        conn,
        """
        SELECT a.size, MIN(a.ask_price) AS lowest_ask, MAX(a.snapshot_time) AS snapshot_time
        FROM ask_depth a
        WHERE a.style_no = ?
          AND a.size IS NOT NULL
          AND TRIM(a.size) != ''
          AND a.snapshot_time = (
              SELECT MAX(x.snapshot_time)
              FROM ask_depth x
              WHERE x.style_no = a.style_no
                AND COALESCE(x.size, '') = COALESCE(a.size, '')
          )
        GROUP BY a.size
        """,
        (style_no,),
    )
    result: dict[str, dict[str, Any]] = {
        str(row["size"]): {"lowest_ask": row["lowest_ask"], "snapshot_time": row["snapshot_time"], "source": "ask_depth"}
        for row in rows
        if row["lowest_ask"] is not None
    }
    market_rows = query_rows(
        conn,
        """
        SELECT m.size, MIN(m.lowest_ask) AS lowest_ask, MAX(m.snapshot_time) AS snapshot_time
        FROM market_snapshots m
        WHERE m.style_no = ?
          AND m.size IS NOT NULL
          AND TRIM(m.size) != ''
          AND m.lowest_ask IS NOT NULL
          AND m.snapshot_time = (
              SELECT MAX(x.snapshot_time)
              FROM market_snapshots x
              WHERE x.style_no = m.style_no
                AND COALESCE(x.size, '') = COALESCE(m.size, '')
          )
        GROUP BY m.size
        """,
        (style_no,),
    )
    for row in market_rows:
        size = str(row["size"])
        if size not in result and row["lowest_ask"] is not None:
            result[size] = {"lowest_ask": row["lowest_ask"], "snapshot_time": row["snapshot_time"], "source": "market_snapshot"}
    return result


def adjacent_size_context(size: str, ask_by_size: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    current = _numeric_us_size(size)
    if current is None:
        return None
    neighbors: list[dict[str, Any]] = []
    for other_size, data in ask_by_size.items():
        other = _numeric_us_size(other_size)
        price = data.get("lowest_ask")
        if other is None or price is None or other == current:
            continue
        neighbors.append(
            {
                "size": other_size,
                "numeric_size": other,
                "lowest_ask": float(price),
                "snapshot_time": data.get("snapshot_time"),
                "source": data.get("source"),
                "distance": abs(other - current),
            }
        )
    if not neighbors:
        return None
    neighbors.sort(key=lambda item: (float(item["distance"]), float(item["lowest_ask"])))
    selected = neighbors[:2]
    reference_price = min(float(item["lowest_ask"]) for item in selected)
    return {
        "neighbors": selected,
        "reference_price": round(reference_price, 2),
        "price_ceiling": round(reference_price * 1.10, 2),
    }


def decode_components(row: dict[str, Any]) -> dict[str, Any]:
    return json_loads(row.get("components_json"), {})


def _supported_sell_ceiling(
    sales: SalesStats,
    *,
    cost: float,
    next_lowest_ask: float | None,
    reference_price: float | None,
    neighbor_price_ceiling: float | None = None,
) -> float | None:
    ceilings: list[float] = []
    if next_lowest_ask is not None and next_lowest_ask > 0:
        ceilings.append(float(next_lowest_ask) - 1)
    if neighbor_price_ceiling is not None and neighbor_price_ceiling > 0:
        ceilings.append(float(neighbor_price_ceiling))
    if sales.p90:
        ceilings.append(float(sales.p90) * 1.03)
    elif sales.p75:
        ceilings.append(float(sales.p75) * 1.05)
    elif sales.median:
        ceilings.append(float(sales.median) * 1.08)
    elif sales.last_sale_amount:
        ceilings.append(float(sales.last_sale_amount) * 1.08)
    if reference_price and reference_price > 0:
        ceilings.append(float(reference_price))
    if not ceilings:
        return None
    ceiling = max(cost + 1, min(ceilings))
    return round(ceiling, 2)


def _estimate_strategy_days_v3(
    sales: SalesStats,
    qty: int,
    target_price: float,
    sell_mode: str,
    *,
    supply_delay_days: float = 0.0,
) -> float:
    velocity = _daily_sales_velocity(sales)
    if not velocity:
        return math.inf
    days = max(1, qty) / velocity
    reference = sales.p90 or sales.p75 or sales.median or sales.last_sale_amount
    if reference and target_price > reference:
        premium = (target_price / reference) - 1
        days *= 1 + min(8.0, premium * 8.0)
    if sell_mode == "平衡出货":
        days *= 1.15
    elif sell_mode == "控盘提价":
        days *= 1.45
    return round(days + max(0.0, float(supply_delay_days or 0.0)), 2)


def _build_sell_targets_v3(
    sales: SalesStats,
    *,
    weighted_avg_cost: float,
    next_lowest_ask: float | None,
    gap: float,
    reference_price: float | None = None,
    neighbor_price_ceiling: float | None = None,
) -> list[dict[str, Any]]:
    cost = max(0.0, float(weighted_avg_cost))
    ceiling = _supported_sell_ceiling(
        sales,
        cost=cost,
        next_lowest_ask=next_lowest_ask,
        reference_price=reference_price,
        neighbor_price_ceiling=neighbor_price_ceiling,
    )
    if ceiling is None or ceiling <= cost:
        return []
    anchor = max(
        [value for value in [sales.median, sales.p75, sales.last_sale_amount, cost] if value is not None and float(value) > 0],
        default=cost,
    )
    quick = min(ceiling, max(float(anchor), cost + max(3.0, cost * 0.03)))
    balanced = min(ceiling, max(quick, cost + max(6.0, cost * 0.06), float(sales.p75 or 0)))
    control = min(ceiling, max(balanced, cost + max(10.0, cost * 0.10), float(sales.p90 or 0)))
    targets = [
        ("快速出货", quick, max(quick, min(ceiling, quick + max(2.0, cost * 0.02))), "优先资金周转，价格贴近成交支撑"),
        ("平衡出货", balanced, max(balanced, min(ceiling, balanced + max(3.0, cost * 0.03))), "兼顾利润和成交速度"),
        ("控盘提价", control, max(control, min(ceiling, control + max(4.0, cost * 0.04))), "只在成交价和下一口 Ask 支撑内提价"),
    ]
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, float]] = set()
    for name, low, high, note in targets:
        low = round(float(low), 2)
        high = round(float(max(low, high)), 2)
        key = (name, low)
        if key in seen or low <= 0:
            continue
        seen.add(key)
        result.append(
            {
                "sell_strategy": name,
                "target_price": low,
                "target_price_high": high,
                "pricing_note": note,
                "sell_ceiling": ceiling,
                "neighbor_price_ceiling": neighbor_price_ceiling,
            }
        )
    return result


def build_ask_depth_strategies(
    ask_rows: list[dict[str, Any]],
    sales: SalesStats,
    *,
    fee_rate: float,
    sales_fraction: float,
    reference_price: float | None = None,
    neighbor_context: dict[str, Any] | None = None,
    ask_snapshot_time: str | None = None,
) -> list[dict[str, Any]]:
    levels = _merge_ask_levels(ask_rows)
    if len(levels) < 2:
        return []
    lowest_ask = float(levels[0]["price"])
    max_price_increase = min(100.0, lowest_ask * 0.20)
    max_buy_price_allowed = lowest_ask + max_price_increase
    sales_cap = max(
        1,
        math.ceil(max(sales.sales_14d * max(0.1, sales_fraction), sales.sales_7d, sales.sales_30d * 0.35)),
    )
    strategies: list[dict[str, Any]] = []
    cumulative_qty = 0
    cumulative_cost = 0.0
    cumulative_consigned = 0
    cumulative_seller = 0
    cumulative_delay_sum = 0.0
    buy_plan: list[dict[str, Any]] = []

    for index, level in enumerate(levels[:-1]):
        price = float(level["price"])
        if price > max_buy_price_allowed:
            break
        qty = int(level["quantity"] or 0)
        if qty <= 0:
            continue
        cumulative_qty += qty
        cumulative_cost += price * qty
        cumulative_consigned += int(level.get("consigned_qty") or 0)
        cumulative_seller += int(level.get("seller_qty") or 0)
        cumulative_delay_sum += float(level.get("availability_delay_days") or 0) * qty
        level_plan = {
            "price": price,
            "quantity": qty,
            "consigned_qty": int(level.get("consigned_qty") or 0),
            "seller_qty": int(level.get("seller_qty") or 0),
            "consigned_ratio": float(level.get("consigned_ratio") or 0),
            "seller_ratio": float(level.get("seller_ratio") or 0),
            "availability_delay_days": float(level.get("availability_delay_days") or 0),
            "cumulative_qty": cumulative_qty,
            "cumulative_cost": round(cumulative_cost, 2),
            "cumulative_consigned_qty": cumulative_consigned,
            "cumulative_seller_qty": cumulative_seller,
            "cumulative_consigned_ratio": round(cumulative_consigned / cumulative_qty, 4) if cumulative_qty else 0.0,
            "cumulative_seller_ratio": round(cumulative_seller / cumulative_qty, 4) if cumulative_qty else 0.0,
        }
        buy_plan.append(level_plan)
        if cumulative_qty > sales_cap:
            break

        next_level = levels[index + 1]
        next_price = float(next_level["price"])
        weighted_avg = cumulative_cost / cumulative_qty
        gap = max(0.0, next_price - price)
        gap_rate = gap / price if price else 0.0
        delay_days = cumulative_delay_sum / cumulative_qty if cumulative_qty else 0.0
        sell_targets = _build_sell_targets_v3(
            sales,
            weighted_avg_cost=weighted_avg,
            next_lowest_ask=next_price,
            gap=gap,
            reference_price=reference_price,
            neighbor_price_ceiling=(neighbor_context or {}).get("price_ceiling"),
        )
        buyout_text = _format_buy_plan_text(buy_plan)
        for target in sell_targets:
            target_low = float(target["target_price"])
            target_high = float(target["target_price_high"])
            proceeds_each = target_low * (1 - max(0.0, float(fee_rate or 0.0)))
            profit_each = proceeds_each - weighted_avg
            total_profit = profit_each * cumulative_qty
            days = _estimate_strategy_days_v3(
                sales,
                cumulative_qty,
                target_low,
                str(target["sell_strategy"]),
                supply_delay_days=delay_days,
            )
            support_price = sales.p90 or sales.p75 or sales.median or sales.last_sale_amount
            supported = support_price is None or target_low <= float(support_price) * 1.08
            qualifies = (
                total_profit > 0
                and gap_rate >= 0.03
                and cumulative_qty <= sales_cap
                and math.isfinite(days)
                and days <= 35
                and supported
            )
            reason_parts: list[str] = []
            if gap_rate < 0.03:
                reason_parts.append(f"断层率只有 {gap_rate * 100:.1f}%")
            if cumulative_qty > sales_cap:
                reason_parts.append(f"买断数量超过销量上限 {sales_cap}")
            if not math.isfinite(days) or days > 35:
                reason_parts.append("预计消化时间偏长")
            if not supported:
                reason_parts.append("建议卖价高于成交价支撑")
            if total_profit <= 0:
                reason_parts.append("利润为负")
            if neighbor_context:
                reason_parts.append(
                    f"相邻尺码参考 {neighbor_context.get('reference_price')}，卖价上限 {neighbor_context.get('price_ceiling')}"
                )
            if not reason_parts:
                reason_parts.append("完整买断低价层，卖价仍在成交支撑范围内")
            strategies.append(
                {
                    "strategy_name": f"买断到 USD ${price:,.2f}",
                    "sell_strategy": target["sell_strategy"],
                    "layer_count": index + 1,
                    "buy_plan": [dict(item) for item in buy_plan],
                    "buyout_levels": buyout_text,
                    "analysis_qty": cumulative_qty,
                    "recommended_buy_qty": cumulative_qty if qualifies else 0,
                    "lowest_buy_price": lowest_ask,
                    "min_buy_price": lowest_ask,
                    "max_buy_price": price,
                    "buyout_level_price": price,
                    "buyout_level_qty": qty,
                    "weighted_avg_cost": round(weighted_avg, 2),
                    "total_buy_cost": round(cumulative_cost, 2),
                    "buy_total_cost": round(cumulative_cost, 2),
                    "next_lowest_ask": next_price,
                    "post_buyout_lowest_ask": next_price,
                    "next_lowest_ask_qty": int(next_level.get("quantity") or 0),
                    "theoretical_gap": round(gap, 2),
                    "gap_rate": round(gap_rate, 4),
                    "price_gap_to_next": round(gap, 2),
                    "target_sell_price_low": round(target_low, 2),
                    "target_sell_price_high": round(target_high, 2),
                    "target_source": target["sell_strategy"],
                    "pricing_note": target["pricing_note"],
                    "sell_ceiling": target.get("sell_ceiling"),
                    "neighbor_context": neighbor_context,
                    "neighbor_price_ceiling": target.get("neighbor_price_ceiling"),
                    "ask_snapshot_time": ask_snapshot_time,
                    "estimated_profit": round(total_profit, 2),
                    "estimated_profit_per_pair": round(profit_each, 2),
                    "estimated_days_to_sell": days,
                    "consigned_qty": cumulative_consigned,
                    "seller_qty": cumulative_seller,
                    "consigned_ratio": round(cumulative_consigned / cumulative_qty, 4) if cumulative_qty else 0.0,
                    "seller_ratio": round(cumulative_seller / cumulative_qty, 4) if cumulative_qty else 0.0,
                    "availability_delay_days": round(delay_days, 2),
                    "qualifies": qualifies,
                    "reason": "；".join(reason_parts),
                    "has_full_ask_depth": True,
                }
            )

    strategies.sort(key=_strategy_option_score, reverse=True)
    return strategies


def build_strategy_options(
    ask_rows: list[dict[str, Any]],
    sales: SalesStats,
    *,
    fee_rate: float,
    sales_fraction: float,
    reference_price: float | None = None,
    neighbor_context: dict[str, Any] | None = None,
    ask_snapshot_time: str | None = None,
    max_options: int = 3,
) -> list[dict[str, Any]]:
    raw_strategies = build_ask_depth_strategies(
        ask_rows,
        sales,
        fee_rate=fee_rate,
        sales_fraction=sales_fraction,
        reference_price=reference_price,
        neighbor_context=neighbor_context,
        ask_snapshot_time=ask_snapshot_time,
    )
    if not raw_strategies:
        return []
    grouped: dict[tuple[tuple[float, int], ...], list[dict[str, Any]]] = {}
    for item in raw_strategies:
        signature = tuple((round(float(level.get("price") or 0), 2), int(level.get("quantity") or 0)) for level in item.get("buy_plan") or [])
        grouped.setdefault(signature, []).append(item)
    options: list[dict[str, Any]] = []
    for items in grouped.values():
        best = sorted(items, key=_strategy_option_score, reverse=True)[0]
        sell_targets = []
        for item in sorted(items, key=lambda row: _sell_strategy_priority(str(row.get("sell_strategy") or ""))):
            sell_targets.append(
                {
                    "sell_strategy": item.get("sell_strategy"),
                    "target_price_low": item.get("target_sell_price_low"),
                    "target_price_high": item.get("target_sell_price_high"),
                    "estimated_profit": item.get("estimated_profit"),
                    "estimated_profit_per_pair": item.get("estimated_profit_per_pair"),
                    "estimated_days_to_sell": item.get("estimated_days_to_sell"),
                    "qualifies": item.get("qualifies"),
                    "pricing_note": item.get("pricing_note"),
                    "reason": item.get("reason"),
                }
            )
        qty = int(best.get("analysis_qty") or 0)
        consigned_qty = int(best.get("consigned_qty") or 0)
        seller_qty = int(best.get("seller_qty") or 0)
        option = {
            **best,
            "total_buy_qty": qty,
            "recommended_buy_qty": int(best.get("recommended_buy_qty") or qty),
            "buy_total_cost": round(float(best.get("buy_total_cost") or best.get("total_buy_cost") or 0), 2),
            "min_buy_price": float(best.get("min_buy_price") or best.get("lowest_buy_price") or best.get("max_buy_price") or 0),
            "buyout_levels": _format_buy_plan_text(best.get("buy_plan") or []),
            "buy_plan_text": _format_buy_plan_text(best.get("buy_plan") or []),
            "buy_mix_text": f"平台寄存 {consigned_qty} / 卖家挂售 {seller_qty} / 到账 {float(best.get('availability_delay_days') or 0):.1f} 天",
            "sell_targets": sell_targets,
            "option_score": _strategy_option_score(best),
        }
        options.append(option)
    options.sort(key=lambda item: item["option_score"], reverse=True)
    return options[:max_options]


def market_snapshot_ask_simulation(
    market_rows: list[dict[str, Any]],
    sales: SalesStats,
    *,
    fee_rate: float,
    ask_snapshot_time: str | None = None,
    neighbor_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asks = [float(row["lowest_ask"]) for row in market_rows if row.get("lowest_ask") is not None]
    if not asks:
        return {
            "has_full_ask_depth": False,
            "recommended_buy_qty": 0,
            "analysis_qty": 0,
            "reason": "缺少完整 Ask Depth，只拿到市场快照",
            "estimated_profit": 0.0,
            "estimated_profit_per_pair": 0.0,
        }
    lowest = min(asks)
    low, high, source = derive_target_sell_prices(sales, fallback_price=lowest, analysis_qty=1)
    profit = ((low or lowest) * (1 - max(0.0, float(fee_rate or 0.0)))) - lowest
    return {
        "has_full_ask_depth": False,
        "recommended_buy_qty": 0,
        "analysis_qty": 1,
        "max_buy_price": lowest,
        "weighted_avg_cost": lowest,
        "target_sell_price_low": low,
        "target_sell_price_high": high,
        "target_source": source,
        "estimated_profit": round(profit, 2),
        "estimated_profit_per_pair": round(profit, 2),
        "estimated_days_to_sell": math.inf,
        "ask_snapshot_time": ask_snapshot_time,
        "neighbor_context": neighbor_context,
        "neighbor_price_ceiling": (neighbor_context or {}).get("price_ceiling"),
        "reason": "缺少完整 Ask Depth，只能用最低 Ask 快照参考",
    }


def simulate_ask_depth(
    ask_rows: list[dict[str, Any]],
    sales: SalesStats,
    *,
    fee_rate: float,
    sales_fraction: float,
    reference_price: float | None = None,
    neighbor_context: dict[str, Any] | None = None,
    ask_snapshot_time: str | None = None,
) -> dict[str, Any]:
    strategies = build_ask_depth_strategies(
        ask_rows,
        sales,
        fee_rate=fee_rate,
        sales_fraction=sales_fraction,
        reference_price=reference_price,
        neighbor_context=neighbor_context,
        ask_snapshot_time=ask_snapshot_time,
    )
    if not strategies:
        return {
            "has_full_ask_depth": False,
            "recommended_buy_qty": 0,
            "analysis_qty": 0,
            "reason": "缺少完整 Ask Depth 或成交支撑不足",
            "estimated_profit": 0.0,
            "estimated_profit_per_pair": 0.0,
            "ask_snapshot_time": ask_snapshot_time,
            "neighbor_context": neighbor_context,
            "neighbor_price_ceiling": (neighbor_context or {}).get("price_ceiling"),
        }
    positive = [item for item in strategies if item.get("qualifies") and float(item.get("estimated_profit") or 0) > 0]
    if positive:
        return {"has_full_ask_depth": True, **max(positive, key=_strategy_option_score)}
    best = max(strategies, key=_strategy_option_score)
    return {"has_full_ask_depth": True, **best, "recommended_buy_qty": 0, "qualifies": False}


def score_opportunity(
    *,
    product: dict[str, Any],
    size: str,
    sales: SalesStats,
    ask_sim: dict[str, Any],
    bid_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    days = release_days(product.get("release_date"))
    notes: list[str] = []
    if days is None:
        release_score = 12
        notes.append("缺少发售日期")
    elif days < 90:
        release_score = 6
        notes.append("发售未满90天")
    elif days <= 365:
        release_score = 18
    else:
        release_score = 14

    if sales.sales_30d >= 30:
        sales_score = 25
    elif sales.sales_30d >= 15:
        sales_score = 22
    elif sales.sales_30d >= 5:
        sales_score = 16
    else:
        sales_score = max(0, sales.sales_30d * 2)
        notes.append(f"近30天销量低，最近成交 {sales.last_sale_days} 天前" if sales.last_sale_days is not None else "近30天销量低")

    gap_rate = float(ask_sim.get("gap_rate") or 0)
    qty = int(ask_sim.get("recommended_buy_qty") or 0)
    total_profit = float(ask_sim.get("estimated_profit") or 0)
    profit_per_pair = float(ask_sim.get("estimated_profit_per_pair") or 0)
    days_to_sell = float(ask_sim.get("estimated_days_to_sell") or math.inf)
    if not ask_sim.get("has_full_ask_depth"):
        ask_score = 0
        notes.append(str(ask_sim.get("reason") or "缺少完整 Ask Depth"))
    else:
        speed_score = _sellout_speed_score(days_to_sell)
        if gap_rate >= 0.10:
            gap_score = 8
        elif gap_rate >= 0.06:
            gap_score = 6
        elif gap_rate >= 0.03:
            gap_score = 4
        else:
            gap_score = 1
            notes.append(str(ask_sim.get("reason") or f"Ask 断层不足：{gap_rate * 100:.1f}%"))
        if total_profit >= 80:
            depth_profit_score = 4
        elif total_profit >= 30:
            depth_profit_score = 3
        elif total_profit > 0:
            depth_profit_score = 2
        else:
            depth_profit_score = 0
        qty_score = 3 if qty >= 3 else (2 if qty >= 2 else (1 if qty == 1 else 0))
        ask_score = min(25, speed_score + gap_score + depth_profit_score + qty_score)
        if math.isfinite(days_to_sell):
            notes.append(f"预计卖完 {days_to_sell:.1f} 天，售罄越快评分越高")

    cost = ask_sim.get("weighted_avg_cost") or ask_sim.get("max_buy_price")
    target = ask_sim.get("target_sell_price_low")
    neighbor_context = ask_sim.get("neighbor_context") or {}
    neighbor_reference = neighbor_context.get("reference_price")
    if cost and target and float(target) > float(cost) and profit_per_pair > 0:
        if profit_per_pair >= 15:
            profit_support_score = 3
        elif profit_per_pair >= 8:
            profit_support_score = 2
        else:
            profit_support_score = 1
        seven_avg_score = _price_proximity_score(target, sales.avg_7d, points=7)
        neighbor_score = _price_proximity_score(target, neighbor_reference, points=6)
        history_score = 4 if sales.sales_7d >= 5 else (3 if sales.sales_30d >= 10 else (2 if sales.sales_30d >= 5 else 1))
        support_score = round(min(20, profit_support_score + seven_avg_score + neighbor_score + history_score), 2)
        if sales.avg_7d and float(target) > float(sales.avg_7d) * 1.07:
            notes.append("建议卖价高于7日均价，成交支撑降分")
        if neighbor_reference and float(target) > float(neighbor_reference) * 1.10:
            notes.append("建议卖价高于相邻尺码10%，成交支撑降分")
    elif sales.avg_7d or sales.median:
        support_score = 8
        notes.append("成交支撑不足")
    else:
        support_score = 5
        notes.append("缺少成交价支撑")

    highest_bid = float(bid_rows[0]["bid_price"]) if bid_rows else None
    supply_score = 8
    if not bid_rows:
        supply_score = 5
        notes.append("缺少 Bid 深度")
    elif cost and highest_bid and highest_bid >= float(cost) * 0.85:
        supply_score = 10
    else:
        notes.append("最高 Bid 离成本较远")

    consigned_qty = int(ask_sim.get("consigned_qty") or 0)
    seller_qty = int(ask_sim.get("seller_qty") or 0)
    total_ask_qty = consigned_qty + seller_qty
    availability_delay_days = float(ask_sim.get("availability_delay_days") or 0.0)
    if total_ask_qty:
        consigned_ratio = consigned_qty / total_ask_qty
        if consigned_ratio >= 0.7:
            supply_score += 1
            notes.append("买断层寄存占比较高，到账更快")
        elif consigned_ratio <= 0.3:
            supply_score -= 1
            notes.append("买断层挂售占比较高，到账偏慢")
        if availability_delay_days >= 7:
            supply_score -= 1
    supply_score = max(0, min(10, supply_score))

    score = round(release_score + sales_score + ask_score + support_score + supply_score, 2)
    if not ask_sim.get("has_full_ask_depth"):
        score = min(score, 79)
    if sales.sales_30d < 5:
        score = min(score, 79)
    if qty <= 0 or total_profit <= 0:
        score = min(score, 59)
    elif qty <= 1 or total_profit < 30:
        score = min(score, 64)
        notes.append("单次数量或总利润偏低")
    elif profit_per_pair < 8:
        score = min(score, 69)
        notes.append("平均每双利润偏低")
    if math.isfinite(days_to_sell) and days_to_sell > 30:
        score = min(score, 69)
        notes.append("预计消化周期偏长")
    if ask_sim.get("neighbor_price_ceiling"):
        notes.append(f"卖价已受相邻尺码 10% 上限约束：{ask_sim.get('neighbor_price_ceiling')}")

    components = {
        "release": release_score,
        "size_sales": sales_score,
        "ask_depth": ask_score,
        "sales_support": support_score,
        "supply_risk": supply_score,
        "model_notes": {
            "sellout_speed_score": _sellout_speed_score(days_to_sell),
            "price_vs_7d_avg_score": _price_proximity_score(target, sales.avg_7d, points=7) if target else 0,
            "price_vs_neighbor_score": _price_proximity_score(target, (ask_sim.get("neighbor_context") or {}).get("reference_price"), points=6) if target else 0,
            "sales_avg_7d": sales.avg_7d,
            "neighbor_reference_price": (ask_sim.get("neighbor_context") or {}).get("reference_price"),
        },
        "sales": sales.__dict__,
        "ask_simulation": ask_sim,
        "highest_bid": highest_bid,
        "fulfillment": {
            "consigned_qty": consigned_qty,
            "seller_qty": seller_qty,
            "availability_delay_days": availability_delay_days,
            "consigned_ratio": round(consigned_qty / total_ask_qty, 4) if total_ask_qty else 0.0,
        },
    }
    return {
        "score": score,
        "rating": rating_from_score(score),
        "risk_notes": "；".join(notes) if notes else "暂无明显风险",
        "components": components,
    }


def compute_and_store_opportunities(
    conn: sqlite3.Connection,
    *,
    fee_rate: float,
    sales_fraction: float,
    progress_callback: ProgressCallback | None = None,
    style_nos: set[str] | list[str] | tuple[str, ...] | None = None,
) -> int:
    style_keys: list[str] = []
    if style_nos is not None:
        seen_style_keys: set[str] = set()
        for value in style_nos:
            normalized = normalize_style_no(value) or str(value).strip().upper()
            style_key = normalized.replace(" ", "").replace("-", "").upper()
            if style_key and style_key not in seen_style_keys:
                seen_style_keys.add(style_key)
                style_keys.append(style_key)
        if not style_keys:
            return 0

    if style_keys:
        placeholders = ",".join("?" for _ in style_keys)
        product_rows = query_rows(
            conn,
            f"""
            SELECT p.product_id, p.style_no, p.title, p.brand, p.release_date, p.raw_json
            FROM products p
            WHERE UPPER(REPLACE(REPLACE(p.style_no, ' ', ''), '-', '')) IN ({placeholders})
            ORDER BY p.style_no
            """,
            tuple(style_keys),
        )
    else:
        product_rows = query_rows(
            conn,
            """
            SELECT p.product_id, p.style_no, p.title, p.brand, p.release_date, p.raw_json
            FROM products p
            ORDER BY p.style_no
            """,
        )
    rows: list[dict[str, Any]] = []
    for product_row in product_rows:
        product = dict(product_row)
        style_no = str(product["style_no"])
        raw_product = json_loads(product.get("raw_json"), {})
        if not product.get("release_date") and isinstance(raw_product, dict):
            parsed_release = extract_release_date(raw_product)
            if parsed_release:
                product["release_date"] = parsed_release
                conn.execute("UPDATE products SET release_date = ?, updated_at = ? WHERE style_no = ?", (parsed_release, utc_now(), style_no))
        sizes: set[str] = set()
        if isinstance(raw_product, dict):
            sizes.update(extract_sizes_from_product(raw_product))
        for table_name in ("product_sizes", "market_snapshots", "ask_depth", "sales_history", "bid_depth"):
            size_rows = query_rows(
                conn,
                f"""
                SELECT DISTINCT size
                FROM {table_name}
                WHERE style_no = ?
                  AND size IS NOT NULL
                  AND TRIM(size) != ''
                """,
                (style_no,),
            )
            sizes.update(str(row["size"]) for row in size_rows if row["size"] not in (None, ""))
        for size in sorted(sizes, key=_size_sort_key):
            row = dict(product)
            row["size"] = size
            rows.append(row)

    if style_keys and not rows:
        return 0
    if style_keys:
        placeholders = ",".join("?" for _ in style_keys)
        conn.execute(
            f"""
            DELETE FROM opportunity_scores
            WHERE UPPER(REPLACE(REPLACE(style_no, ' ', ''), '-', '')) IN ({placeholders})
            """,
            tuple(style_keys),
        )

    computed = 0
    computed_at = utc_now()
    total = len(rows)
    if progress_callback:
        progress_callback({"phase": "评分", "status": "running", "message": f"准备计算 {total} 个货号尺码", "score_completed": 0, "score_total": total})

    style_ask_context_cache: dict[str, dict[str, dict[str, Any]]] = {}

    for row in rows:
        style_no = str(row["style_no"])
        size = str(row["size"])
        if progress_callback:
            progress_callback(
                {
                    "phase": "评分",
                    "style_no": style_no,
                    "size": size,
                    "endpoint": "score_opportunity",
                    "status": "running",
                    "message": f"正在评分 {style_no} / US {size}",
                    "score_completed": computed,
                    "score_total": total,
                }
            )
        product = dict(row)
        sales = get_sales_stats(conn, style_no, size)
        ask_rows = latest_ask_rows(conn, style_no, size)
        bid_rows = latest_bid_rows(conn, style_no, size)
        market_rows = latest_market_rows(conn, style_no, size)
        reference_price = get_reference_price(conn, style_no, size=size)
        if style_no not in style_ask_context_cache:
            style_ask_context_cache[style_no] = latest_lowest_ask_by_size(conn, style_no)
        neighbor_context = adjacent_size_context(size, style_ask_context_cache[style_no])
        ask_snapshot_time = max((str(item.get("snapshot_time")) for item in ask_rows if item.get("snapshot_time")), default=None)
        if not ask_snapshot_time:
            ask_snapshot_time = max((str(item.get("snapshot_time")) for item in market_rows if item.get("snapshot_time")), default=None)
        if not bid_rows and market_rows:
            market_bids = [float(item["highest_bid"]) for item in market_rows if item.get("highest_bid") is not None]
            if market_bids:
                bid_rows = [{"bid_price": max(market_bids), "bid_quantity": 1}]
        if ask_rows:
            ask_sim = simulate_ask_depth(
                ask_rows,
                sales,
                fee_rate=fee_rate,
                sales_fraction=sales_fraction,
                reference_price=reference_price,
                neighbor_context=neighbor_context,
                ask_snapshot_time=ask_snapshot_time,
            )
        else:
            ask_sim = market_snapshot_ask_simulation(
                market_rows,
                sales,
                fee_rate=fee_rate,
                ask_snapshot_time=ask_snapshot_time,
                neighbor_context=neighbor_context,
            )

        scored = score_opportunity(product=product, size=size, sales=sales, ask_sim=ask_sim, bid_rows=bid_rows)
        days = release_days(product.get("release_date"))
        conn.execute(
            """
            INSERT INTO opportunity_scores (
                product_id, style_no, title, brand, size, score, rating,
                recommended_buy_qty, max_buy_price, weighted_avg_cost,
                next_lowest_ask, target_sell_price_low, target_sell_price_high,
                estimated_profit, estimated_profit_per_pair, estimated_days_to_sell, sales_7d, sales_30d,
                last_sale_at, last_sale_days, release_date, release_days,
                risk_notes, components_json, computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(style_no, size) DO UPDATE SET
                product_id=excluded.product_id,
                title=excluded.title,
                brand=excluded.brand,
                score=excluded.score,
                rating=excluded.rating,
                recommended_buy_qty=excluded.recommended_buy_qty,
                max_buy_price=excluded.max_buy_price,
                weighted_avg_cost=excluded.weighted_avg_cost,
                next_lowest_ask=excluded.next_lowest_ask,
                target_sell_price_low=excluded.target_sell_price_low,
                target_sell_price_high=excluded.target_sell_price_high,
                estimated_profit=excluded.estimated_profit,
                estimated_profit_per_pair=excluded.estimated_profit_per_pair,
                estimated_days_to_sell=excluded.estimated_days_to_sell,
                sales_7d=excluded.sales_7d,
                sales_30d=excluded.sales_30d,
                last_sale_at=excluded.last_sale_at,
                last_sale_days=excluded.last_sale_days,
                release_date=excluded.release_date,
                release_days=excluded.release_days,
                risk_notes=excluded.risk_notes,
                components_json=excluded.components_json,
                computed_at=excluded.computed_at
            """,
            (
                product.get("product_id"),
                style_no,
                product.get("title"),
                product.get("brand"),
                size,
                scored["score"],
                scored["rating"],
                int(ask_sim.get("recommended_buy_qty") or 0),
                ask_sim.get("max_buy_price"),
                ask_sim.get("weighted_avg_cost"),
                ask_sim.get("next_lowest_ask"),
                ask_sim.get("target_sell_price_low"),
                ask_sim.get("target_sell_price_high"),
                ask_sim.get("estimated_profit"),
                ask_sim.get("estimated_profit_per_pair"),
                None if math.isinf(float(ask_sim.get("estimated_days_to_sell", math.inf))) else ask_sim.get("estimated_days_to_sell"),
                sales.sales_7d,
                sales.sales_30d,
                sales.last_sale_at,
                sales.last_sale_days,
                product.get("release_date"),
                days,
                scored["risk_notes"],
                json_dumps(scored["components"]),
                computed_at,
            ),
        )
        computed += 1
        if progress_callback:
            progress_callback(
                {
                    "phase": "评分",
                    "style_no": style_no,
                    "size": size,
                    "endpoint": "score_opportunity",
                    "status": "ok",
                    "message": f"{style_no} / US {size} 评分 {scored['score']}，评级 {scored['rating']}",
                    "score_completed": computed,
                    "score_total": total,
                }
            )
    conn.commit()
    return computed
