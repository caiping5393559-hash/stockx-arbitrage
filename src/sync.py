from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Callable

from .db import json_dumps, log_sync, utc_now
from .parsing import (
    extract_product_uuid,
    extract_size_variants,
    extract_product,
    extract_release_date,
    extract_sizes_from_product,
    first_value,
    iter_records,
    looks_like_size,
    normalize_style_no,
    unwrap_payload,
    normalize_service_level,
    to_float,
    to_int,
)
from .stockx_client import ApiCallResult, StockXClient

ProgressCallback = Callable[[dict[str, Any]], None]


@dataclass
class SyncSummary:
    style_no: str
    product_id: str | None
    sizes: list[str]
    sales_rows: int = 0
    ask_rows: int = 0
    bid_rows: int = 0
    errors: list[str] | None = None


def reset_style_snapshots(conn: sqlite3.Connection, style_no: str) -> None:
    normalized = normalize_style_no(style_no) or str(style_no).strip().upper()
    if not normalized:
        return
    for table_name in (
        "market_snapshots",
        "sales_history",
        "ask_depth",
        "bid_depth",
        "product_sizes",
        "opportunity_scores",
    ):
        conn.execute(f"DELETE FROM {table_name} WHERE style_no = ?", (normalized,))
    log_sync(
        conn,
        f"{normalized} 刷新前已清空旧快照",
        event_type="snapshot_reset",
        style_no=normalized,
    )


def _target_size_candidates(value: Any) -> set[str]:
    text = str(value or "").strip().upper()
    if text.startswith("US "):
        text = text[3:].strip()
    elif text.startswith("US-"):
        text = text[3:].strip()
    elif text.startswith("US") and len(text) > 2:
        text = text[2:].strip()
    text = text.replace(" ", "")
    if not text:
        return set()
    try:
        number_value = float(text)
        if number_value.is_integer():
            text = str(int(number_value))
    except ValueError:
        pass
    base = re.sub(r"^[A-Z]+", "", text)
    base = re.sub(r"[A-Z]+$", "", base)
    candidates = {text}
    if re.fullmatch(r"\d{1,2}(?:\.5)?", base):
        if "." not in base:
            candidates.add(f"{float(base):.1f}")
        candidates.update(
            {
                base,
                f"{base}W",
                f"{base}Y",
                f"{base}C",
                f"{base}M",
                f"W{base}",
                f"Y{base}",
                f"C{base}",
                f"M{base}",
            }
        )
    return candidates


def sync_style(
    conn: sqlite3.Connection,
    style_no: str,
    *,
    title_hint: str | None = None,
    include_sales: bool = True,
    include_depth: bool = True,
    include_size_endpoints: bool = False,
    target_size: str | None = None,
    reset_snapshot: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> SyncSummary:
    style_no = normalize_style_no(style_no) or str(style_no).strip().upper()
    client = StockXClient(conn)
    errors: list[str] = []
    product_uuid: str | None = None
    product_id: str | None = None
    resolved_product: dict[str, Any] | None = None
    sizes: list[str] = []
    size_variants: list[dict[str, Any]] = []
    target_size_candidates = _target_size_candidates(target_size)

    if reset_snapshot:
        reset_style_snapshots(conn, style_no)
        conn.commit()

    def notify(endpoint: str, status: str, *, size: str | None = None, message: str | None = None) -> None:
        if progress_callback:
            progress_callback(
                {
                    "phase": "同步接口",
                    "style_no": style_no,
                    "size": size,
                    "endpoint": endpoint,
                    "status": status,
                    "message": message or "",
                }
            )

    notify("/get_product_detail_info_by_sku", "running")
    detail_lookup = client.get_product_detail_info_by_sku(style_no)
    lookup_payload = detail_lookup.pages[0] if detail_lookup.ok and detail_lookup.pages else None
    lookup_data = unwrap_payload(lookup_payload) if lookup_payload is not None else None
    if detail_lookup.ok and isinstance(lookup_data, dict):
        product_uuid = extract_product_uuid(lookup_data) or product_uuid
        product = extract_product(lookup_data)
        if product:
            release_date = extract_release_date(lookup_data) or extract_release_date(product)
            if release_date and not first_value(product, ["releaseDate", "release_date", "releaseAt"]):
                product["releaseDate"] = release_date
            resolved_product = product
            product_id = _save_product(conn, product, style_no)
            notify("/get_product_detail_info_by_sku", "ok", message=f"已找到 {product.get('title') or style_no}")
        else:
            errors.append("get_product_detail_info_by_sku returned empty product")
            notify("/get_product_detail_info_by_sku", "error", message="商品详情为空")
    else:
        errors.append(detail_lookup.error or "get_product_detail_info_by_sku failed")
        notify("/get_product_detail_info_by_sku", "error", message=detail_lookup.error or "get_product_detail_info_by_sku failed")

    if not product_uuid:
        search_terms = [style_no]
        cleaned_title = str(title_hint or "").strip()
        if cleaned_title and cleaned_title not in search_terms:
            search_terms.append(cleaned_title)

        search_found = False
        for search_term in search_terms:
            for country in ("US", "HK"):
                notify("search_product", "running", message=f"{search_term} / {country}")
                search = client.search_product(keyword=search_term, page=1, country=country)
                search_payload = search.pages[0] if search.ok and search.pages else None
                search_data = unwrap_payload(search_payload) if search_payload is not None else None
                search_product, search_uuid = _resolve_product_from_search(search_data, style_no)
                if search_uuid:
                    product_uuid = search_uuid
                if search_product and not product_id:
                    if resolved_product is None:
                        resolved_product = search_product
                    release_date = extract_release_date(search_data) or extract_release_date(search_product)
                    if release_date and not first_value(search_product, ["releaseDate", "release_date", "releaseAt"]):
                        search_product["releaseDate"] = release_date
                    product_id = _save_product(conn, search_product, style_no)
                if product_uuid:
                    notify("search_product", "ok", message=f"fallback 找到 {product_uuid}")
                    search_found = True
                    break
            if search_found:
                break
        if not product_uuid:
            errors.append("sku lookup did not resolve stockx uuid")
            notify("search_product", "error", message="未找到 stockx_uuid")

    notify("/get_product_size_info_by_sku", "running")
    size_lookup = client.get_product_size_info_by_sku(style_no)
    size_payload = size_lookup.pages[0] if size_lookup.ok and size_lookup.pages else None
    size_data = unwrap_payload(size_payload) if size_payload is not None else None
    if size_lookup.ok and isinstance(size_data, dict):
        product_uuid = extract_product_uuid(size_data) or product_uuid
        size_variants = extract_size_variants(size_data)
        sizes = [str(item["size"]) for item in size_variants if item.get("size")]
        _save_product_sizes(conn, style_no, product_id or product_uuid, size_variants)
        notify("/get_product_size_info_by_sku", "ok", message=f"发现 {len(size_variants)} 个尺码")
    else:
        if product_uuid:
            notify(
                "/get_product_size_info_by_sku",
                "ok",
                message="尺码信息接口未命中，后续将用商品详情兜底尺码列表",
            )
        else:
            errors.append(size_lookup.error or "get_product_size_info_by_sku failed")
            notify("/get_product_size_info_by_sku", "error", message=size_lookup.error or "get_product_size_info_by_sku failed")

    if not size_variants and resolved_product:
        fallback_sizes = extract_sizes_from_product(resolved_product)
        if fallback_sizes:
            size_variants = [{"product_size_uuid": None, "size": size, "raw": {"size": size}} for size in fallback_sizes]
            sizes = [str(size) for size in fallback_sizes]
            _save_product_sizes(conn, style_no, product_id or product_uuid, size_variants)
            notify("size_fallback", "ok", message=f"从商品主对象补到 {len(fallback_sizes)} 个尺码")

    if not product_uuid:
        search_terms = [style_no]
        cleaned_title = str(title_hint or "").strip()
        if cleaned_title and cleaned_title not in search_terms:
            search_terms.append(cleaned_title)

        search_found = False
        for search_term in search_terms:
            for country in ("US", "HK"):
                notify("search_product", "running", message=f"{search_term} / {country}")
                search = client.search_product(keyword=search_term, page=1, country=country)
                search_payload = search.pages[0] if search.ok and search.pages else None
                search_data = unwrap_payload(search_payload) if search_payload is not None else None
                search_product, search_uuid = _resolve_product_from_search(search_data, style_no)
                if search_uuid:
                    product_uuid = search_uuid
                if search_product and not product_id:
                    if resolved_product is None:
                        resolved_product = search_product
                    release_date = extract_release_date(search_data) or extract_release_date(search_product)
                    if release_date and not first_value(search_product, ["releaseDate", "release_date", "releaseAt"]):
                        search_product["releaseDate"] = release_date
                    product_id = _save_product(conn, search_product, style_no)
                if product_uuid:
                    notify("search_product", "ok", message=f"fallback 找到 {product_uuid}")
                    search_found = True
                    break
            if search_found:
                break
        if not product_uuid:
            errors.append("sku lookup did not resolve stockx uuid")
            notify("search_product", "error", message="未找到 stockx_uuid")

    if not size_variants and resolved_product:
        fallback_sizes = extract_sizes_from_product(resolved_product)
        if fallback_sizes:
            size_variants = [{"product_size_uuid": None, "size": size, "raw": {"size": size}} for size in fallback_sizes]
            sizes = [str(size) for size in fallback_sizes]
            _save_product_sizes(conn, style_no, product_id or product_uuid, size_variants)
            notify("size_fallback", "ok", message=f"从商品主对象补到 {len(fallback_sizes)} 个尺码")

    sales_rows = 0
    ask_rows = 0
    bid_rows = 0

    if product_uuid:
        notify("/product_detail", "running")
        detail = client.product_detail(product_uuid=product_uuid)
        if detail.ok:
            detail_payload = detail.pages[0] if detail.pages else None
            detail_data = unwrap_payload(detail_payload) if detail_payload is not None else None
            detail_product = extract_product(detail_data) if detail_data is not None else None
            if detail_product:
                detail_release = extract_release_date(detail_data) or extract_release_date(detail_product)
                if detail_release and not first_value(detail_product, ["releaseDate", "release_date", "releaseAt"]):
                    detail_product["releaseDate"] = detail_release
                resolved_product = detail_product
                product_id = _save_product(conn, detail_product, style_no)
                detail_size_variants = extract_size_variants(detail_data)
                if detail_size_variants:
                    size_variants = detail_size_variants
                    sizes = [str(item["size"]) for item in size_variants if item.get("size")]
                    _save_product_sizes(conn, style_no, product_id or product_uuid, size_variants)
                    notify("product_detail_sizes", "ok", message=f"从商品详情补到 {len(size_variants)} 个尺码")
            notify("/product_detail", "ok", message="商品详情已刷新")
        else:
            errors.append(detail.error or "product_detail failed")
            notify("/product_detail", "error", message=detail.error or "product_detail failed")

        notify("/product_market_info", "running")
        market = client.product_market_info(product_id=product_uuid)
        if market.ok:
            _save_market_snapshots(conn, market, style_no, product_uuid)
            notify("/product_market_info", "ok")
        else:
            errors.append(market.error or "product_market_info failed")
            notify("/product_market_info", "error", message=market.error or "product_market_info failed")

        notify("/product_size_price", "running")
        size_price = client.product_size_price(product_id=product_uuid)
        if size_price.ok:
            _save_market_snapshots(conn, size_price, style_no, product_uuid)
            notify("/product_size_price", "ok")
        else:
            errors.append(size_price.error or "product_size_price failed")
            notify("/product_size_price", "error", message=size_price.error or "product_size_price failed")

        if include_sales:
            notify("/product_activity_new", "running")
            sales = client.product_activity_new(product_uuid=product_uuid)
            if sales.ok:
                sales_rows += _save_sales(conn, sales, style_no, product_uuid)
                notify("/product_activity_new", "ok", message=f"成交 {sales_rows} 行")
            else:
                errors.append(sales.error or "product_activity_new failed")
                notify("/product_activity_new", "error", message=sales.error or "product_activity_new failed")

        if include_depth:
            notify("/product_ask_list", "running")
            asks = client.product_ask_list(product_uuid=product_uuid)
            if asks.ok:
                ask_rows += _save_depth(conn, asks, style_no, product_uuid, side="ask")
                notify("/product_ask_list", "ok", message=f"Ask {ask_rows} 行")
            else:
                errors.append(asks.error or "product_ask_list failed")
                notify("/product_ask_list", "error", message=asks.error or "product_ask_list failed")

            notify("/product_bid_list", "running")
            bids = client.product_bid_list(product_uuid=product_uuid)
            if bids.ok:
                bid_rows += _save_depth(conn, bids, style_no, product_uuid, side="bid")
                notify("/product_bid_list", "ok", message=f"Bid {bid_rows} 行")
            else:
                errors.append(bids.error or "product_bid_list failed")
                notify("/product_bid_list", "error", message=bids.error or "product_bid_list failed")
    else:
        message = "没有拿到 stockx_uuid，跳过 market/activity/ask/bid；需要先确认 sku 查详情接口或搜索接口"
        errors.append(message)
        notify("stockx_uuid lookup", "error", message=message)

    if include_size_endpoints and size_variants:
        for variant in size_variants:
            size = str(variant.get("size") or "").strip()
            normalized_size = size.replace("US", "").strip().upper().replace(" ", "")
            if target_size_candidates and normalized_size not in target_size_candidates:
                continue
            size_uuid = str(variant.get("product_size_uuid") or "").strip()
            if not size_uuid:
                continue
            notify("/product_size_market_info", "running", size=size)
            market_size = client.product_size_market_info(size_uuid)
            if market_size.ok:
                _save_market_snapshots(conn, market_size, style_no, product_uuid, forced_size=size)
                notify("/product_size_market_info", "ok", size=size)
            else:
                notify("/product_size_market_info", "error", size=size, message=market_size.error or "")
            if include_sales:
                notify("/product_size_activity_new", "running", size=size)
                sales_size = client.product_size_activity_new(size_uuid)
                if sales_size.ok:
                    sales_rows += _save_sales(conn, sales_size, style_no, product_uuid, forced_size=size)
                    notify("/product_size_activity_new", "ok", size=size)
                else:
                    notify("/product_size_activity_new", "error", size=size, message=sales_size.error or "")
            if include_depth:
                notify("/product_size_ask_list", "running", size=size)
                asks_size = client.product_size_ask_list(size_uuid)
                if asks_size.ok:
                    ask_rows += _save_depth(conn, asks_size, style_no, product_uuid, side="ask", forced_size=size)
                    notify("/product_size_ask_list", "ok", size=size)
                else:
                    notify("/product_size_ask_list", "error", size=size, message=asks_size.error or "")
                notify("/product_size_bid_list", "running", size=size)
                bids_size = client.product_size_bid_list(size_uuid)
                if bids_size.ok:
                    bid_rows += _save_depth(conn, bids_size, style_no, product_uuid, side="bid", forced_size=size)
                    notify("/product_size_bid_list", "ok", size=size)
                else:
                    notify("/product_size_bid_list", "error", size=size, message=bids_size.error or "")

    log_sync(
        conn,
        f"{style_no} 同步完成",
        severity="info" if not errors else "warning",
        event_type="sync_complete",
        style_no=style_no,
        product_id=product_uuid,
        details={"errors": errors, "sales_rows": sales_rows, "ask_rows": ask_rows, "bid_rows": bid_rows},
    )
    conn.commit()
    return SyncSummary(style_no, product_uuid, sizes, sales_rows, ask_rows, bid_rows, errors)


def _save_product(conn: sqlite3.Connection, product: dict[str, Any], fallback_style_no: str) -> str | None:
    product_id = first_value(product, ["productId", "product_id", "id", "uuid"])
    style_no = first_value(product, ["styleNo", "style_no", "sku", "styleId"], fallback_style_no)
    title = first_value(product, ["title", "name", "productName"])
    brand = first_value(product, ["brand", "brandName"])
    if isinstance(brand, dict):
        brand = first_value(brand, ["name", "title"])
    media = product.get("media") if isinstance(product.get("media"), dict) else {}
    image_url = first_value(
        product,
        ["imageUrl", "image_url", "thumbnail", "thumbUrl"],
        first_value(media, ["imageUrl", "smallImageUrl", "thumbUrl"]),
    )
    images = product.get("images")
    if not image_url and isinstance(images, list) and images:
        first_image = images[0]
        image_url = first_image.get("url") if isinstance(first_image, dict) else str(first_image)
    release_date = extract_release_date(product) or first_value(product, ["releaseDate", "release_date", "releaseAt"])

    conn.execute(
        """
        INSERT INTO products (
            product_id, style_no, title, brand, release_date, image_url, raw_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(style_no) DO UPDATE SET
            product_id=excluded.product_id,
            title=COALESCE(excluded.title, products.title),
            brand=COALESCE(excluded.brand, products.brand),
            release_date=COALESCE(excluded.release_date, products.release_date),
            image_url=COALESCE(excluded.image_url, products.image_url),
            raw_json=excluded.raw_json,
            updated_at=excluded.updated_at
        """,
        (
            str(product_id) if product_id else None,
            str(style_no),
            str(title) if title else None,
            str(brand) if brand else None,
            str(release_date) if release_date else None,
            str(image_url) if image_url else None,
            json_dumps(product),
            utc_now(),
        ),
    )
    return str(product_id) if product_id else None


def _save_product_sizes(
    conn: sqlite3.Connection,
    style_no: str,
    product_id: str | None,
    sizes: list[dict[str, Any] | str],
) -> None:
    updated_at = utc_now()
    for size_entry in sizes:
        if isinstance(size_entry, dict):
            size_text = str(size_entry.get("size") or "").strip()
            raw_payload = size_entry.get("raw") if isinstance(size_entry.get("raw"), dict) else size_entry
        else:
            size_text = str(size_entry).strip()
            raw_payload = {"size": size_text}
        if not size_text:
            continue
        conn.execute(
            """
            INSERT INTO product_sizes (
                product_id, style_no, size, raw_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(style_no, size) DO UPDATE SET
                product_id=excluded.product_id,
                raw_json=excluded.raw_json,
                updated_at=excluded.updated_at
            """,
            (
                product_id,
                style_no,
                size_text,
                json_dumps(raw_payload),
                updated_at,
            ),
        )


def _normalize_style_key(value: Any) -> str:
    return "".join(ch for ch in str(value).upper() if ch.isalnum())


def _extract_search_uuid(item: dict[str, Any]) -> str | None:
    value = first_value(item, ["productUuid", "product_uuid", "uuid", "id"])
    return str(value) if value not in (None, "") else None


def _resolve_product_from_search(payload: Any, style_no: str) -> tuple[dict[str, Any] | None, str | None]:
    payload = unwrap_payload(payload)
    if not isinstance(payload, dict):
        return None, None
    products = first_value(payload, ["Products", "products"], [])
    if not isinstance(products, list):
        return None, None
    target = _normalize_style_key(style_no)
    fallback: dict[str, Any] | None = None
    for item in products:
        if not isinstance(item, dict):
            continue
        fallback = fallback or item
        candidate = first_value(item, ["styleId", "styleNo", "style_no", "sku"])
        if candidate and _normalize_style_key(candidate) == target:
            return item, _extract_search_uuid(item)
    if fallback:
        return fallback, _extract_search_uuid(fallback)
    return None, None


def _variant_size_text(variant: dict[str, Any] | None, forced_size: str | None = None) -> str | None:
    if forced_size:
        text = str(forced_size).strip()
        return text or None
    if not isinstance(variant, dict):
        return None
    size = first_value(variant, ["size", "shoeSize", "displaySize", "variantSize"])
    traits = variant.get("traits") if isinstance(variant.get("traits"), dict) else None
    if not size and isinstance(traits, dict):
        size = first_value(traits, ["size", "shoeSize", "displaySize", "variantSize"])
    if not size:
        size_chart = variant.get("sizeChart") if isinstance(variant.get("sizeChart"), dict) else None
        display_options = size_chart.get("displayOptions") if isinstance(size_chart, dict) else None
        if isinstance(display_options, list):
            for option in display_options:
                if not isinstance(option, dict):
                    continue
                candidate = str(option.get("size") or "").strip()
                upper = candidate.upper()
                if upper.startswith("US "):
                    candidate = candidate[3:].strip()
                elif upper.startswith("US-"):
                    candidate = candidate[3:].strip()
                elif upper.startswith("US"):
                    candidate = candidate[2:].strip()
                candidate = candidate.replace("M ", "").replace("W ", "").strip()
                if candidate:
                    size = candidate
                    break
    text = str(size).strip() if size not in (None, "") else ""
    return text or None


def _amount_value(value: Any) -> float | None:
    if isinstance(value, dict):
        return to_float(first_value(value, ["amount", "price", "value"]))
    return to_float(value)


def _extract_market_rows(page: Any, *, forced_size: str | None = None) -> list[dict[str, Any]]:
    payload = unwrap_payload(page)
    rows: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return rows

    product = payload.get("product")
    if isinstance(product, dict):
        if isinstance(product.get("variants"), list):
            for variant in product["variants"]:
                if not isinstance(variant, dict):
                    continue
                market = variant.get("market") if isinstance(variant.get("market"), dict) else {}
                state = market.get("state") if isinstance(market.get("state"), dict) else {}
                sales_info = market.get("salesInformation") if isinstance(market.get("salesInformation"), dict) else {}
                rows.append(
                    {
                        "size": _variant_size_text(variant, forced_size=forced_size),
                        "lowest_ask": _amount_value(first_value(state, ["lowestAsk"])),
                        "highest_bid": _amount_value(first_value(state, ["highestBid"])),
                        "last_sale": _amount_value(first_value(sales_info, ["lastSale"])),
                        "market_price": _amount_value(
                            first_value(sales_info, ["lastSale", "averagePrice"])
                            or first_value(state, ["lowestAsk", "highestBid"])
                        ),
                        "raw_json": json_dumps(variant),
                    }
                )
            return rows
        market = product.get("market")
        if isinstance(market, dict):
            bid_ask = market.get("bidAskData") if isinstance(market.get("bidAskData"), dict) else {}
            sales_info = market.get("salesInformation") if isinstance(market.get("salesInformation"), dict) else {}
            state = market.get("state") if isinstance(market.get("state"), dict) else {}
            rows.append(
                {
                    "size": forced_size,
                    "lowest_ask": _amount_value(first_value(bid_ask, ["lowestAsk"]) or first_value(state, ["lowestAsk"])),
                    "highest_bid": _amount_value(first_value(bid_ask, ["highestBid"]) or first_value(state, ["highestBid"])),
                    "last_sale": _amount_value(first_value(sales_info, ["lastSale"])),
                    "market_price": _amount_value(
                        first_value(sales_info, ["lastSale", "averagePrice"])
                        or first_value(bid_ask, ["lowestAsk", "highestBid"])
                        or first_value(state, ["lowestAsk", "highestBid"])
                    ),
                    "raw_json": json_dumps(market),
                }
            )
            return rows

    variant = payload.get("variant")
    if isinstance(variant, dict):
        market = variant.get("market") if isinstance(variant.get("market"), dict) else {}
        state = market.get("state") if isinstance(market.get("state"), dict) else {}
        sales_info = market.get("salesInformation") if isinstance(market.get("salesInformation"), dict) else {}
        rows.append(
            {
                "size": _variant_size_text(variant, forced_size=forced_size),
                "lowest_ask": _amount_value(first_value(state, ["lowestAsk"])),
                "highest_bid": _amount_value(first_value(state, ["highestBid"])),
                "last_sale": _amount_value(first_value(sales_info, ["lastSale"])),
                "market_price": _amount_value(
                    first_value(sales_info, ["lastSale", "averagePrice"])
                    or first_value(state, ["lowestAsk", "highestBid"])
                ),
                "raw_json": json_dumps(variant),
            }
        )
        return rows

    return rows


def _extract_sales_rows(page: Any, *, forced_size: str | None = None) -> list[dict[str, Any]]:
    payload = unwrap_payload(page)
    rows: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return rows

    def handle_edges(edges: Any) -> None:
        if not isinstance(edges, list):
            return
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node") if isinstance(edge.get("node"), dict) else edge
            if not isinstance(node, dict):
                continue
            variant = node.get("associatedVariant") if isinstance(node.get("associatedVariant"), dict) else node.get("variant")
            rows.append(
                {
                    "size": _variant_size_text(variant, forced_size=forced_size),
                    "amount": _amount_value(first_value(node, ["amount", "price", "salePrice", "lastSale", "value"])),
                    "created_at": first_value(node, ["createdAt", "created_at", "date", "eventTime", "time"]) or edge.get("cursor"),
                    "order_type": first_value(node, ["orderType", "order_type", "type"]),
                    "raw_json": json_dumps(node),
                }
            )

    product = payload.get("product")
    if isinstance(product, dict):
        market = product.get("market")
        if isinstance(market, dict):
            sales = market.get("sales")
            if isinstance(sales, dict):
                handle_edges(sales.get("edges"))
                if rows:
                    return rows

    variant = payload.get("variant")
    if isinstance(variant, dict):
        market = variant.get("market")
        if isinstance(market, dict):
            sales = market.get("sales")
            if isinstance(sales, dict):
                handle_edges(sales.get("edges"))
                if rows:
                    return rows

    return rows


def _extract_depth_rows(page: Any, *, side: str, forced_size: str | None = None) -> list[dict[str, Any]]:
    payload = unwrap_payload(page)
    rows: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return rows

    def handle_edges(edges: Any) -> None:
        if not isinstance(edges, list):
            return
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node") if isinstance(edge.get("node"), dict) else edge
            if not isinstance(node, dict):
                continue
            variant = node.get("variant") if isinstance(node.get("variant"), dict) else node.get("associatedVariant")
            amount = _amount_value(first_value(node, ["amount", "askPrice", "bidPrice", "price"]))
            if amount is None:
                continue
            rows.append(
                {
                    "size": _variant_size_text(variant, forced_size=forced_size),
                    "price": amount,
                    "quantity": to_int(first_value(node, ["count", "quantity", "qty", "askQuantity", "bidQuantity"]), 1),
                    "service_level": first_value(node, ["serviceLevel", "service_level", "fulfillmentType", "deliveryType"]),
                    "is_consigned": bool(first_value(node, ["availableForFlex", "isConsigned", "is_consigned", "platformConsigned"], False)),
                    "raw_json": json_dumps(node),
                }
            )

    product = payload.get("product")
    if isinstance(product, dict):
        market = product.get("market")
        if isinstance(market, dict):
            levels = market.get("priceLevels")
            if isinstance(levels, dict):
                handle_edges(levels.get("edges"))
                if rows:
                    return rows

    variant = payload.get("variant")
    if isinstance(variant, dict):
        market = variant.get("market")
        if isinstance(market, dict):
            levels = market.get("priceLevels")
            if isinstance(levels, dict):
                handle_edges(levels.get("edges"))
                if rows:
                    return rows

    return rows


def _save_market_snapshots(
    conn: sqlite3.Connection,
    result: ApiCallResult,
    style_no: str,
    product_id: str | None,
    *,
    forced_size: str | None = None,
) -> int:
    count = 0
    snapshot_time = utc_now()
    all_rows: list[dict[str, Any]] = []
    for page in result.pages:
        rows = _extract_market_rows(page, forced_size=forced_size)
        if not rows:
            for record in iter_records(page):
                if not isinstance(record, dict):
                    continue
                size = forced_size or first_value(record, ["size", "shoeSize", "variantSize", "displaySize"])
                rows.append(
                    {
                        "size": size,
                        "lowest_ask": to_float(first_value(record, ["lowestAsk", "lowest_ask", "ask", "askPrice"])),
                        "highest_bid": to_float(first_value(record, ["highestBid", "highest_bid", "bid", "bidPrice"])),
                        "last_sale": to_float(first_value(record, ["lastSale", "last_sale", "lastSalePrice"])),
                        "market_price": to_float(first_value(record, ["marketPrice", "market_price", "price"])),
                        "raw_json": json_dumps(record),
                    }
                )
        all_rows.extend(rows)
    for row in _dedupe_market_rows(all_rows):
        if not any(row.get(key) is not None for key in ("lowest_ask", "highest_bid", "last_sale", "market_price")):
            continue
        row_size = str(row.get("size")) if row.get("size") else None
        cursor = conn.execute(
            """
            INSERT INTO market_snapshots (
                product_id, style_no, size, lowest_ask, highest_bid, last_sale,
                market_price, raw_json, snapshot_time
            )
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM market_snapshots
                WHERE style_no = ?
                  AND COALESCE(size, '') = COALESCE(?, '')
                  AND COALESCE(lowest_ask, -1) = COALESCE(?, -1)
                  AND COALESCE(highest_bid, -1) = COALESCE(?, -1)
                  AND COALESCE(last_sale, -1) = COALESCE(?, -1)
                  AND COALESCE(market_price, -1) = COALESCE(?, -1)
                  AND snapshot_time = ?
                LIMIT 1
            )
            """,
            (
                product_id,
                style_no,
                row_size,
                row.get("lowest_ask"),
                row.get("highest_bid"),
                row.get("last_sale"),
                row.get("market_price"),
                row.get("raw_json") or json_dumps(row),
                snapshot_time,
                style_no,
                row_size,
                row.get("lowest_ask"),
                row.get("highest_bid"),
                row.get("last_sale"),
                row.get("market_price"),
                snapshot_time,
            ),
        )
        if cursor.rowcount and cursor.rowcount > 0:
            count += 1
    return count


def _save_sales(
    conn: sqlite3.Connection,
    result: ApiCallResult,
    style_no: str,
    product_id: str | None,
    *,
    forced_size: str | None = None,
) -> int:
    count = 0
    synced_at = utc_now()
    for page in result.pages:
        rows = _extract_sales_rows(page, forced_size=forced_size)
        if not rows:
            for record in iter_records(page):
                if not isinstance(record, dict):
                    continue
                rows.append(
                    {
                        "size": forced_size or first_value(record, ["size", "shoeSize", "variantSize", "displaySize"]),
                        "amount": to_float(first_value(record, ["amount", "price", "salePrice", "lastSale", "value"])),
                        "created_at": first_value(record, ["createdAt", "created_at", "date", "eventTime", "time"]),
                        "order_type": first_value(record, ["orderType", "order_type", "type"]),
                        "raw_json": json_dumps(record),
                    }
                )
        for row in rows:
            if row.get("amount") is None and not row.get("created_at"):
                continue
            row_size = str(row.get("size")) if row.get("size") else None
            created_at = str(row.get("created_at")) if row.get("created_at") else None
            order_type = str(row.get("order_type")) if row.get("order_type") else None
            cursor = conn.execute(
                """
                INSERT INTO sales_history (
                    product_id, style_no, size, amount, created_at, order_type,
                    source_endpoint, raw_json, synced_at
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM sales_history
                    WHERE style_no = ?
                      AND COALESCE(size, '') = COALESCE(?, '')
                      AND COALESCE(created_at, '') = COALESCE(?, '')
                      AND COALESCE(amount, -1) = COALESCE(?, -1)
                      AND COALESCE(order_type, '') = COALESCE(?, '')
                    LIMIT 1
                )
                """,
                (
                    product_id,
                    style_no,
                    row_size,
                    row.get("amount"),
                    created_at,
                    order_type,
                    result.endpoint,
                    row.get("raw_json") or json_dumps(row),
                    synced_at,
                    style_no,
                    row_size,
                    created_at,
                    row.get("amount"),
                    order_type,
                ),
            )
            if cursor.rowcount and cursor.rowcount > 0:
                count += 1
    return count


def _save_depth(
    conn: sqlite3.Connection,
    result: ApiCallResult,
    style_no: str,
    product_id: str | None,
    *,
    side: str,
    forced_size: str | None = None,
) -> int:
    count = 0
    snapshot_time = utc_now()
    all_rows: list[dict[str, Any]] = []
    for page in result.pages:
        rows = _extract_depth_rows(page, side=side, forced_size=forced_size)
        if not rows:
            for record, inherited_size in _iter_depth_records(page, forced_size):
                if not isinstance(record, dict):
                    continue
                size = forced_size or inherited_size or first_value(record, ["size", "shoeSize", "variantSize", "displaySize"])
                if side == "ask":
                    rows.append(
                        {
                            "size": size,
                            "price": to_float(first_value(record, ["ask_price", "askPrice", "price", "amount", "lowestAsk"])),
                            "quantity": to_int(first_value(record, ["ask_quantity", "askQuantity", "quantity", "qty", "count"]), 1),
                            "service_level": normalize_service_level(
                                first_value(record, ["service_level", "serviceLevel", "fulfillmentType", "deliveryType"])
                            ),
                            "is_consigned": bool(
                                first_value(
                                    record,
                                    ["isConsigned", "is_consigned", "isPlatformConsigned", "platformConsigned", "instantShip"],
                                    False,
                                )
                            ),
                            "raw_json": json_dumps(record),
                        }
                    )
                else:
                    rows.append(
                        {
                            "size": size,
                            "price": to_float(first_value(record, ["bid_price", "bidPrice", "price", "amount", "highestBid"])),
                            "quantity": to_int(first_value(record, ["bid_quantity", "bidQuantity", "quantity", "qty", "count"]), 1),
                            "raw_json": json_dumps(record),
                        }
                    )
        all_rows.extend(rows)
    for row in _aggregate_depth_rows(all_rows, side=side):
        if row.get("price") is None:
            continue
        row_size = str(row.get("size")) if row.get("size") else None
        if side == "ask":
            service_level = row.get("service_level")
            is_consigned = 1 if row.get("is_consigned") else 0
            cursor = conn.execute(
                """
                INSERT INTO ask_depth (
                    product_id, style_no, size, ask_price, ask_quantity,
                    service_level, is_consigned, snapshot_time, raw_json
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM ask_depth
                    WHERE style_no = ?
                      AND COALESCE(size, '') = COALESCE(?, '')
                      AND ask_price = ?
                      AND COALESCE(service_level, '') = COALESCE(?, '')
                      AND is_consigned = ?
                      AND snapshot_time = ?
                    LIMIT 1
                )
                """,
                (
                    product_id,
                    style_no,
                    row_size,
                    row.get("price"),
                    row.get("quantity") or 1,
                    service_level,
                    is_consigned,
                    snapshot_time,
                    row.get("raw_json") or json_dumps(row),
                    style_no,
                    row_size,
                    row.get("price"),
                    service_level,
                    is_consigned,
                    snapshot_time,
                ),
            )
        else:
            cursor = conn.execute(
                """
                INSERT INTO bid_depth (
                    product_id, style_no, size, bid_price, bid_quantity,
                    snapshot_time, raw_json
                )
                SELECT ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM bid_depth
                    WHERE style_no = ?
                      AND COALESCE(size, '') = COALESCE(?, '')
                      AND bid_price = ?
                      AND snapshot_time = ?
                    LIMIT 1
                )
                """,
                (
                    product_id,
                    style_no,
                    row_size,
                    row.get("price"),
                    row.get("quantity") or 1,
                    snapshot_time,
                    row.get("raw_json") or json_dumps(row),
                    style_no,
                    row_size,
                    row.get("price"),
                    snapshot_time,
                ),
            )
        if cursor.rowcount and cursor.rowcount > 0:
            count += 1
    return count


def _dedupe_market_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("size") or ""),
            row.get("lowest_ask"),
            row.get("highest_bid"),
            row.get("last_sale"),
            row.get("market_price"),
        )
        if key not in deduped:
            deduped[key] = dict(row)
    return list(deduped.values())


def _aggregate_depth_rows(rows: list[dict[str, Any]], *, side: str) -> list[dict[str, Any]]:
    aggregated: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        price = row.get("price")
        if price is None:
            continue
        if side == "ask":
            key = (
                str(row.get("size") or ""),
                price,
                str(row.get("service_level") or ""),
                1 if row.get("is_consigned") else 0,
            )
        else:
            key = (str(row.get("size") or ""), price)
        quantity = max(1, to_int(row.get("quantity"), 1))
        if key in aggregated:
            aggregated[key]["quantity"] = int(aggregated[key].get("quantity") or 0) + quantity
            continue
        clean = dict(row)
        clean["quantity"] = quantity
        aggregated[key] = clean
    return list(aggregated.values())


def _iter_depth_records(payload: Any, current_size: str | None = None):
    if isinstance(payload, list):
        for item in payload:
            yield from _iter_depth_records(item, current_size)
        return
    if not isinstance(payload, dict):
        return

    price_keys = {
        "ask_price",
        "askPrice",
        "bid_price",
        "bidPrice",
        "price",
        "amount",
        "lowestAsk",
        "highestBid",
    }
    quantity_keys = {"quantity", "qty", "count", "askQuantity", "bidQuantity", "ask_quantity", "bid_quantity"}
    if price_keys.intersection(payload.keys()) and quantity_keys.intersection(payload.keys()):
        yield payload, current_size
        return

    size = current_size
    direct_size = first_value(payload, ["size", "shoeSize", "variantSize", "displaySize"])
    if direct_size:
        size = str(direct_size)

    for key, value in payload.items():
        next_size = str(key) if looks_like_size(key) else size
        if isinstance(value, (dict, list)):
            yield from _iter_depth_records(value, next_size)
