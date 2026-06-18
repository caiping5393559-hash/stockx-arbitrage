from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
import traceback
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from src.analytics import compute_and_store_opportunities  # noqa: E402
from src.config import get_settings  # noqa: E402
from src.db import connect, init_db, json_loads, log_sync, query_rows  # noqa: E402
from src.firebase_cloud import backup_core_tables_to_firestore  # noqa: E402
from src.sync import sync_style  # noqa: E402


MARKER_PATH = BASE_DIR / "data" / "auto_hourly_full_sync.json"
JOB_LOCK_PATH = BASE_DIR / "data" / "sync_job.lock"
WORKER_LOCK_PATH = BASE_DIR / "data" / "auto_full_sync_worker.lock"
POLL_SECONDS = 60
MIN_INTERVAL_SECONDS = 15 * 60
LOCK_STALE_SECONDS = 6 * 60 * 60
WORKER_LOCK_STALE_SECONDS = 3 * 60 * 60
INCREMENTAL_SCORE_BATCH_SIZE = 4
STYLE_SYNC_HARD_TIMEOUT_SECONDS = 180
CHECKPOINT_STYLE_INTERVAL = 10
CHECKPOINT_MIN_SECONDS = 180


def _now() -> datetime:
    return datetime.utcnow()


def _chunks(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        size = 1
    return [values[index : index + size] for index in range(0, len(values), size)]


def _read_marker() -> dict[str, Any]:
    try:
        return json_loads(MARKER_PATH.read_text(encoding="utf-8"), {}) or {}
    except OSError:
        return {}


def _write_marker(data: dict[str, Any]) -> None:
    MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    MARKER_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _count_opportunity_scores(conn: sqlite3.Connection) -> int:
    try:
        return int(conn.execute("SELECT COUNT(*) FROM opportunity_scores").fetchone()[0] or 0)
    except Exception:
        return 0


def _timestamp(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        return datetime.fromisoformat(str(value).strip().replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _due(marker: dict[str, Any], interval_seconds: int) -> bool:
    started_ts = _timestamp(marker.get("last_started_ts")) or _timestamp(marker.get("last_started_at"))
    finished_ts = _timestamp(marker.get("last_finished_ts")) or _timestamp(marker.get("last_finished_at"))
    last_status = str(marker.get("last_status") or "")
    valid_finished_ts = finished_ts if finished_ts and (not started_ts or finished_ts >= started_ts) else None

    if last_status == "running":
        return False
    if valid_finished_ts is None:
        return started_ts is None
    return (_now().timestamp() - valid_finished_ts) >= interval_seconds


def _acquire_job_lock(job_id: str) -> bool:
    JOB_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if JOB_LOCK_PATH.exists():
        try:
            age = _now().timestamp() - JOB_LOCK_PATH.stat().st_mtime
            if age > LOCK_STALE_SECONDS:
                JOB_LOCK_PATH.unlink()
            else:
                return False
        except OSError:
            return False
    try:
        with JOB_LOCK_PATH.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps({"job_id": job_id, "kind": "auto_hourly_full_sync", "pid": os.getpid(), "started_at": _now().isoformat()}))
        return True
    except OSError:
        return False


def _release_job_lock(job_id: str) -> None:
    try:
        if not JOB_LOCK_PATH.exists():
            return
        data = json_loads(JOB_LOCK_PATH.read_text(encoding="utf-8"), {}) or {}
        if data.get("job_id") == job_id:
            JOB_LOCK_PATH.unlink()
    except OSError:
        pass


def _acquire_worker_lock() -> bool:
    WORKER_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if WORKER_LOCK_PATH.exists():
        try:
            age = _now().timestamp() - WORKER_LOCK_PATH.stat().st_mtime
            if age > WORKER_LOCK_STALE_SECONDS:
                WORKER_LOCK_PATH.unlink()
            else:
                return False
        except OSError:
            return False
    try:
        with WORKER_LOCK_PATH.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps({"pid": os.getpid(), "started_at": _now().isoformat()}))
        return True
    except OSError:
        return False


def _touch_worker_lock() -> None:
    try:
        WORKER_LOCK_PATH.write_text(json.dumps({"pid": os.getpid(), "heartbeat_at": _now().isoformat()}), encoding="utf-8")
    except OSError:
        pass


def _release_worker_lock() -> None:
    try:
        if WORKER_LOCK_PATH.exists():
            WORKER_LOCK_PATH.unlink()
    except OSError:
        pass


def _load_imported_styles(conn) -> list[str]:
    rows = query_rows(
        conn,
        """
        SELECT DISTINCT style_no
        FROM sku_items
        WHERE style_no IS NOT NULL AND TRIM(style_no) != ''
        ORDER BY style_no
        """,
    )
    return [str(row["style_no"]).strip().upper() for row in rows if row["style_no"]]


def _sync_one_style(db_path: Path, style_no: str) -> dict[str, Any]:
    worker_conn = connect(db_path)
    init_db(worker_conn)
    try:
        summary = sync_style(
            worker_conn,
            style_no,
            include_sales=True,
            include_depth=True,
            include_size_endpoints=True,
            reset_snapshot=True,
        )
        return {
            "style_no": style_no,
            "sizes": len(summary.sizes),
            "sales_rows": summary.sales_rows,
            "ask_rows": summary.ask_rows,
            "bid_rows": summary.bid_rows,
            "errors": summary.errors or [],
        }
    finally:
        worker_conn.close()


def _run_full_sync() -> None:
    settings = get_settings()
    job_id = f"auto-{uuid.uuid4().hex[:8]}"
    marker = _read_marker()
    if not _acquire_job_lock(job_id):
        marker.update(
            {
                "enabled": True,
                "last_checked_at": _now().isoformat(timespec="seconds"),
                "last_message": "自动全量同步到点，但已有同步任务在运行",
            }
        )
        _write_marker(marker)
        return

    conn = connect(settings.db_path)
    init_db(conn)
    try:
        styles = _load_imported_styles(conn)
        started = _now()
        marker.update(
            {
                "enabled": True,
                "active_job_id": job_id,
                "last_started_at": started.isoformat(timespec="seconds"),
                "last_started_ts": started.timestamp(),
                "last_status": "running",
                "last_style_count": len(styles),
                "completed": 0,
                "total": len(styles),
                "recomputed": 0,
                "last_finished_at": None,
                "last_finished_ts": None,
                "last_error": None,
                "last_traceback": None,
                "current_style": None,
                "last_message": f"今日机会全量刷新StockX API开始：{len(styles)} 个货号",
            }
        )
        _write_marker(marker)

        if not styles:
            log_sync(conn, "自动全量同步跳过：没有导入货号", event_type="auto_full_sync")
            conn.commit()
            return

        log_sync(
            conn,
            f"今日机会全量刷新StockX API开始：{len(styles)} 个货号",
            event_type="auto_full_sync_start",
            details={"job_id": job_id, "style_count": len(styles)},
        )
        conn.commit()

        errors = 0
        recomputed = 0
        completed = 0
        last_checkpoint_completed = 0
        last_checkpoint_ts = 0.0
        worker_count = max(1, min(int(settings.sync_max_workers or 4), 8, len(styles)))
        batch_size = max(1, min(worker_count, INCREMENTAL_SCORE_BATCH_SIZE))
        pending_recompute: list[str] = []

        def try_recompute_pending(current_style: str) -> None:
            nonlocal last_checkpoint_completed, last_checkpoint_ts, pending_recompute, recomputed
            if not pending_recompute:
                return
            batch_styles = pending_recompute[:]
            try:
                batch_recomputed = compute_and_store_opportunities(
                    conn,
                    fee_rate=settings.estimated_seller_fee_rate,
                    sales_fraction=settings.buy_depth_sales_fraction,
                    style_nos=batch_styles,
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower():
                    conn.rollback()
                    return
                raise
            pending_recompute = []
            recomputed += batch_recomputed
            conn.commit()
            score_count = _count_opportunity_scores(conn)
            marker.update(
                {
                    "active_job_id": job_id,
                    "completed": completed,
                    "total": len(styles),
                    "current_style": current_style,
                    "workers": worker_count,
                    "recomputed": recomputed,
                    "opportunity_scores": score_count,
                    "last_progress_at": _now().isoformat(timespec="seconds"),
                    "last_progress_ts": _now().timestamp(),
                    "last_checked_at": _now().isoformat(timespec="seconds"),
                    "last_message": f"今日机会已同步并增量重算：{completed}/{len(styles)} 个货号，新增 {batch_recomputed} 个尺码，累计 {recomputed} 个尺码",
                }
            )
            _write_marker(marker)
            now_ts = _now().timestamp()
            should_checkpoint = (
                score_count > 0
                and (
                    completed == 1
                    or completed >= len(styles)
                    or completed - last_checkpoint_completed >= CHECKPOINT_STYLE_INTERVAL
                    or now_ts - last_checkpoint_ts >= CHECKPOINT_MIN_SECONDS
                )
            )
            if should_checkpoint:
                try:
                    result = backup_core_tables_to_firestore(
                        settings.db_path,
                        reason=f"auto_full_sync_checkpoint:{completed}/{len(styles)}",
                    )
                    last_checkpoint_completed = completed
                    last_checkpoint_ts = now_ts
                    marker.update(
                        {
                            "last_checkpoint_at": _now().isoformat(timespec="seconds"),
                            "last_checkpoint_completed": completed,
                            "last_checkpoint_scores": score_count,
                            "last_checkpoint_ok": bool(result.get("ok")),
                            "last_checkpoint_message": result.get("message") or "ok",
                        }
                    )
                    _write_marker(marker)
                except Exception as exc:  # noqa: BLE001
                    marker.update(
                        {
                            "last_checkpoint_at": _now().isoformat(timespec="seconds"),
                            "last_checkpoint_completed": completed,
                            "last_checkpoint_scores": score_count,
                            "last_checkpoint_ok": False,
                            "last_checkpoint_message": str(exc),
                        }
                    )
                    _write_marker(marker)

        next_index = 0
        future_map: dict[Any, dict[str, Any]] = {}
        executor = ThreadPoolExecutor(max_workers=worker_count)

        def submit_next() -> None:
            nonlocal next_index
            if next_index >= len(styles):
                return
            style_no = styles[next_index]
            next_index += 1
            future_map[executor.submit(_sync_one_style, settings.db_path, style_no)] = {
                "style_no": style_no,
                "started_at": time.monotonic(),
            }

        try:
            for _ in range(worker_count):
                submit_next()

            while future_map:
                done, _ = wait(future_map.keys(), timeout=5, return_when=FIRST_COMPLETED)
                now_monotonic = time.monotonic()
                timeout_futures = [
                    future
                    for future, meta in list(future_map.items())
                    if future not in done and now_monotonic - float(meta["started_at"]) >= STYLE_SYNC_HARD_TIMEOUT_SECONDS
                ]
                for future in list(done) + timeout_futures:
                    meta = future_map.pop(future, None)
                    if not meta:
                        continue
                    completed += 1
                    style_no = str(meta["style_no"])
                    pending_recompute.append(style_no)
                    if future in timeout_futures:
                        errors += 1
                        future.cancel()
                        log_sync(
                            conn,
                            f"自动全量同步 {style_no} 超时，已跳过继续下一个",
                            severity="error",
                            event_type="auto_full_sync_style_timeout",
                            style_no=style_no,
                            details={"job_id": job_id, "timeout_seconds": STYLE_SYNC_HARD_TIMEOUT_SECONDS},
                        )
                    else:
                        try:
                            result = future.result(timeout=1)
                            errors += len(result.get("errors") or [])
                        except Exception as exc:  # noqa: BLE001
                            errors += 1
                            log_sync(
                                conn,
                                f"自动全量同步 {style_no} 失败：{exc}",
                                severity="error",
                                event_type="auto_full_sync_style_error",
                                style_no=style_no,
                                details={"job_id": job_id, "error": str(exc)},
                            )
                    if completed == 1 or completed % 5 == 0 or completed == len(styles):
                        marker.update(
                            {
                                "active_job_id": job_id,
                                "completed": completed,
                                "total": len(styles),
                                "current_style": style_no,
                                "workers": worker_count,
                                "recomputed": recomputed,
                                "last_progress_at": _now().isoformat(timespec="seconds"),
                                "last_progress_ts": _now().timestamp(),
                                "last_checked_at": _now().isoformat(timespec="seconds"),
                                "last_message": f"今日机会全量刷新StockX API中：{completed}/{len(styles)} {style_no}（并发 {worker_count}，已增量重算 {recomputed} 个尺码）",
                            }
                        )
                        _write_marker(marker)
                        conn.commit()
                    try_recompute_pending(style_no)
                    submit_next()
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        for batch in []:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_map = {executor.submit(_sync_one_style, settings.db_path, style_no): style_no for style_no in batch}
                for future in as_completed(future_map):
                    completed += 1
                    style_no = future_map[future]
                    pending_recompute.append(style_no)
                    try:
                        result = future.result()
                        errors += len(result.get("errors") or [])
                    except Exception as exc:  # noqa: BLE001
                        errors += 1
                        log_sync(
                            conn,
                            f"自动全量同步 {style_no} 失败：{exc}",
                            severity="error",
                            event_type="auto_full_sync_style_error",
                            style_no=style_no,
                            details={"job_id": job_id, "error": str(exc)},
                        )
                    if completed == 1 or completed % 5 == 0 or completed == len(styles):
                        marker.update(
                            {
                                "active_job_id": job_id,
                                "completed": completed,
                                "total": len(styles),
                                "current_style": style_no,
                                "workers": worker_count,
                                "recomputed": recomputed,
                                "last_progress_at": _now().isoformat(timespec="seconds"),
                                "last_progress_ts": _now().timestamp(),
                                "last_checked_at": _now().isoformat(timespec="seconds"),
                                "last_message": f"今日机会全量刷新StockX API中：{completed}/{len(styles)} {style_no}（并发 {worker_count}，已增量重算 {recomputed} 个尺码）",
                            }
                        )
                        _write_marker(marker)
                        conn.commit()
                    try_recompute_pending(style_no)

        if pending_recompute:
            try_recompute_pending(pending_recompute[-1])

        finished = _now()
        marker.update(
            {
                "active_job_id": None,
                "last_finished_at": finished.isoformat(timespec="seconds"),
                "last_finished_ts": finished.timestamp(),
                "last_status": "done",
                "completed": len(styles),
                "total": len(styles),
                "recomputed": recomputed,
                "last_error_count": errors,
                "last_message": f"今日机会全量刷新StockX API完成：{len(styles)} 个货号，重算 {recomputed} 个尺码",
            }
        )
        _write_marker(marker)
        log_sync(
            conn,
            marker["last_message"],
            event_type="auto_full_sync_done",
            details={"job_id": job_id, "style_count": len(styles), "recomputed": recomputed, "errors": errors},
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        marker.update(
            {
                "active_job_id": None,
                "last_finished_at": _now().isoformat(timespec="seconds"),
                "last_finished_ts": _now().timestamp(),
                "last_status": "error",
                "last_error": str(exc),
                "last_traceback": traceback.format_exc(limit=8),
                "last_message": f"自动全量同步失败：{exc}",
            }
        )
        _write_marker(marker)
        try:
            log_sync(
                conn,
                marker["last_message"],
                severity="error",
                event_type="auto_full_sync_error",
                details={"job_id": job_id, "error": str(exc)},
            )
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()
        _release_job_lock(job_id)


def main() -> None:
    if not _acquire_worker_lock():
        return
    try:
        while True:
            _touch_worker_lock()
            try:
                settings = get_settings()
                marker = _read_marker()
                interval_seconds = max(MIN_INTERVAL_SECONDS, int(settings.auto_full_sync_interval_minutes or 60) * 60)
                if settings.auto_full_sync_enabled and settings.credentials_ready and _due(marker, interval_seconds):
                    _run_full_sync()
                else:
                    marker.update(
                        {
                            "enabled": bool(settings.auto_full_sync_enabled),
                            "interval_minutes": int(settings.auto_full_sync_interval_minutes or 60),
                            "last_checked_at": _now().isoformat(timespec="seconds"),
                        }
                    )
                    if not settings.credentials_ready:
                        marker["last_message"] = "自动全量同步等待凭证"
                    elif not settings.auto_full_sync_enabled:
                        marker["last_message"] = "自动全量同步已关闭"
                    _write_marker(marker)
            except Exception as exc:  # noqa: BLE001
                marker = _read_marker()
                marker.update(
                    {
                        "last_error_at": _now().isoformat(timespec="seconds"),
                        "last_error": str(exc),
                        "last_traceback": traceback.format_exc(limit=8),
                    }
                )
                try:
                    _write_marker(marker)
                except Exception:
                    pass
            time.sleep(POLL_SECONDS)
    finally:
        _release_worker_lock()


if __name__ == "__main__":
    main()
