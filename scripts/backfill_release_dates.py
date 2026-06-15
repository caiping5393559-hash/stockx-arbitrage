from __future__ import annotations

import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from src.analytics import compute_and_store_opportunities  # noqa: E402
from src.config import get_settings  # noqa: E402
from src.db import connect, init_db, json_loads, log_sync, query_rows, utc_now  # noqa: E402
from src.parsing import extract_release_date  # noqa: E402
from src.release_dates import lookup_release_date  # noqa: E402

MARKER_PATH = BASE_DIR / "data" / "release_date_backfill.json"
LOCK_PATH = BASE_DIR / "data" / "release_date_backfill.lock"
LOCK_STALE_SECONDS = 6 * 60 * 60


def _now() -> datetime:
    return datetime.utcnow()


def _write_marker(data: dict[str, Any]) -> None:
    MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    MARKER_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _read_marker() -> dict[str, Any]:
    try:
        return json_loads(MARKER_PATH.read_text(encoding="utf-8"), {}) or {}
    except OSError:
        return {}


def _acquire_lock() -> bool:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists():
        try:
            age = _now().timestamp() - LOCK_PATH.stat().st_mtime
            if age > LOCK_STALE_SECONDS:
                LOCK_PATH.unlink()
            else:
                return False
        except OSError:
            return False
    try:
        with LOCK_PATH.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps({"started_at": _now().isoformat()}))
        return True
    except OSError:
        return False


def _release_lock() -> None:
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink()
    except OSError:
        pass


def _lookup_local_cache(conn, style_no: str, raw_json: str | None) -> tuple[str | None, str | None]:
    raw_product = json_loads(raw_json, {}) if raw_json else {}
    release_date = extract_release_date(raw_product)
    if release_date:
        return release_date, "products.raw_json"

    rows = query_rows(
        conn,
        """
        SELECT endpoint, response_json
        FROM raw_api_responses
        WHERE params_json LIKE ?
        ORDER BY fetched_at DESC
        LIMIT 80
        """,
        (f"%{style_no}%",),
    )
    for row in rows:
        payload = json_loads(row["response_json"], {})
        release_date = extract_release_date(payload)
        if release_date:
            return release_date, f"raw_api:{row['endpoint']}"
    return None, None


def _save_source(
    conn,
    *,
    product_id: str | None,
    style_no: str,
    title: str,
    release_date: str,
    source_name: str | None,
    source_url: str | None,
    confidence: float | None,
    raw_text: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO release_date_sources (
            product_id, style_no, title, release_date,
            source_name, source_url, confidence, raw_text, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            product_id,
            style_no,
            title,
            release_date,
            source_name,
            source_url,
            confidence,
            raw_text,
            utc_now(),
        ),
    )


def main() -> None:
    if not _acquire_lock():
        marker = _read_marker()
        marker.update({"status": "running", "message": "已有发售日期补齐任务在运行"})
        _write_marker(marker)
        return

    settings = get_settings()
    conn = connect(settings.db_path)
    init_db(conn)
    try:
        rows = [dict(row) for row in query_rows(
            conn,
            """
            SELECT product_id, style_no, title, brand, release_date, raw_json
            FROM products
            WHERE release_date IS NULL OR TRIM(release_date) = ''
            ORDER BY style_no
            """,
        )]
        total = len(rows)
        filled = 0
        searched = 0
        failed = 0
        started = _now()
        _write_marker(
            {
                "status": "running",
                "started_at": started.isoformat(timespec="seconds"),
                "total": total,
                "completed": 0,
                "filled": 0,
                "searched": 0,
                "failed": 0,
                "message": f"开始补齐发售日期：{total} 个缺失商品",
            }
        )
        log_sync(conn, f"开始联网补齐发售日期：{total} 个缺失商品", event_type="release_date_web_backfill_start")
        conn.commit()

        for index, row in enumerate(rows, start=1):
            style_no = str(row.get("style_no") or "").strip()
            title = str(row.get("title") or "").strip()
            brand = str(row.get("brand") or "").strip() or None
            _write_marker(
                {
                    "status": "running",
                    "started_at": started.isoformat(timespec="seconds"),
                    "last_checked_at": _now().isoformat(timespec="seconds"),
                    "total": total,
                    "completed": index - 1,
                    "filled": filled,
                    "searched": searched,
                    "failed": failed,
                    "current_style": style_no,
                    "message": f"正在联网查找发售日期：{index}/{total} {style_no}",
                }
            )
            release_date = None
            source_name = None
            source_url = None
            confidence = None
            raw_text = None
            try:
                release_date, source_name = _lookup_local_cache(conn, style_no, row.get("raw_json"))
                if not release_date and title:
                    searched += 1
                    result = lookup_release_date(
                        style_no=style_no,
                        title=title,
                        brand=brand,
                        timeout=min(max(int(settings.timeout), 6), 15),
                        candidate_limit=18,
                        allow_search=True,
                    )
                    if result:
                        release_date = result.release_date
                        source_name = result.source_name
                        source_url = result.source_url
                        confidence = result.confidence
                        raw_text = result.raw_text
                if release_date:
                    conn.execute(
                        "UPDATE products SET release_date = ?, updated_at = ? WHERE style_no = ?",
                        (release_date, utc_now(), style_no),
                    )
                    _save_source(
                        conn,
                        product_id=row.get("product_id"),
                        style_no=style_no,
                        title=title,
                        release_date=release_date,
                        source_name=source_name or "unknown",
                        source_url=source_url,
                        confidence=confidence if confidence is not None else 1.0,
                        raw_text=raw_text,
                    )
                    filled += 1
                    log_sync(
                        conn,
                        f"{style_no} 发售日期补齐：{release_date}",
                        event_type="release_date_web_backfill",
                        style_no=style_no,
                        product_id=row.get("product_id"),
                        details={"release_date": release_date, "source": source_name, "source_url": source_url},
                    )
                else:
                    failed += 1
                    log_sync(
                        conn,
                        f"{style_no} 未查到发售日期",
                        severity="warning",
                        event_type="release_date_web_backfill",
                        style_no=style_no,
                        product_id=row.get("product_id"),
                        details={"title": title, "brand": brand},
                    )
                if index == 1 or index % 3 == 0 or index == total:
                    conn.commit()
                    _write_marker(
                        {
                            "status": "running",
                            "started_at": started.isoformat(timespec="seconds"),
                            "last_checked_at": _now().isoformat(timespec="seconds"),
                            "total": total,
                            "completed": index,
                            "filled": filled,
                            "searched": searched,
                            "failed": failed,
                            "current_style": style_no,
                            "message": f"联网补齐发售日期中：{index}/{total}，已补 {filled}",
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                conn.commit()
                log_sync(
                    conn,
                    f"{style_no} 发售日期补齐失败：{exc}",
                    severity="error",
                    event_type="release_date_web_backfill_error",
                    style_no=style_no,
                    product_id=row.get("product_id"),
                    details={"error": str(exc)},
                )

        recomputed = compute_and_store_opportunities(
            conn,
            fee_rate=settings.estimated_seller_fee_rate,
            sales_fraction=settings.buy_depth_sales_fraction,
        )
        conn.commit()
        _write_marker(
            {
                "status": "done",
                "started_at": started.isoformat(timespec="seconds"),
                "finished_at": _now().isoformat(timespec="seconds"),
                "total": total,
                "completed": total,
                "filled": filled,
                "searched": searched,
                "failed": failed,
                "recomputed": recomputed,
                "message": f"发售日期补齐完成：补到 {filled}/{total}，重算 {recomputed} 个尺码",
            }
        )
        log_sync(
            conn,
            f"发售日期联网补齐完成：补到 {filled}/{total}，重算 {recomputed} 个尺码",
            event_type="release_date_web_backfill_done",
            details={"filled": filled, "total": total, "searched": searched, "failed": failed, "recomputed": recomputed},
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        _write_marker(
            {
                "status": "error",
                "finished_at": _now().isoformat(timespec="seconds"),
                "message": f"发售日期补齐失败：{exc}",
                "error": str(exc),
                "traceback": traceback.format_exc(limit=8),
            }
        )
        try:
            log_sync(
                conn,
                f"发售日期联网补齐失败：{exc}",
                severity="error",
                event_type="release_date_web_backfill_error",
                details={"error": str(exc)},
            )
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()
        _release_lock()


if __name__ == "__main__":
    main()
