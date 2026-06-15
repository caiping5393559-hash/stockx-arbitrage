from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

import app as app_mod  # noqa: E402
from src.config import get_settings  # noqa: E402
from src.db import connect, init_db, json_loads, log_sync, query_rows  # noqa: E402


REQUEST_PATH = BASE_DIR / "data" / "goat_rescore_request.json"
MARKER_PATH = BASE_DIR / "data" / "goat_stockx_worker.json"
LOCK_PATH = BASE_DIR / "data" / "goat_stockx_worker.lock"
PAUSED_STOCKX_TASK_PATH = BASE_DIR / "data" / "paused_stockx_task.json"
LOCK_STALE_SECONDS = 6 * 60 * 60
POLL_SECONDS = 10
PROGRESS_LOCK = threading.Lock()


def _now() -> datetime:
    return datetime.utcnow()


def _write_marker(data: dict[str, Any]) -> None:
    MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    last_error: Exception | None = None
    for attempt in range(30):
        tmp_path = MARKER_PATH.with_name(f"{MARKER_PATH.name}.{os.getpid()}.{threading.get_ident()}.{attempt}.tmp")
        try:
            tmp_path.write_text(payload, encoding="utf-8")
            os.replace(tmp_path, MARKER_PATH)
            return
        except OSError as exc:
            last_error = exc
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            time.sleep(min(1.0, 0.05 * (attempt + 1)))
    try:
        MARKER_PATH.with_suffix(".latest.json").write_text(payload, encoding="utf-8")
    except OSError:
        pass
    if last_error:
        print(f"marker write skipped: {last_error}", file=sys.stderr)


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
            handle.write(json.dumps({"pid": os.getpid(), "started_at": _now().isoformat()}))
        return True
    except OSError:
        return False


def _touch_lock() -> None:
    try:
        LOCK_PATH.write_text(json.dumps({"pid": os.getpid(), "heartbeat_at": _now().isoformat()}), encoding="utf-8")
    except OSError:
        pass


def _release_lock() -> None:
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink()
    except OSError:
        pass


def _consume_request() -> dict[str, Any] | None:
    if not REQUEST_PATH.exists():
        return None
    try:
        request = json_loads(REQUEST_PATH.read_text(encoding="utf-8"), {}) or {}
    except OSError:
        request = {}
    try:
        REQUEST_PATH.unlink()
    except OSError:
        pass
    return request


def _counts(conn) -> dict[str, Any]:
    total = query_rows(conn, "SELECT COUNT(*) AS c FROM goat_consignment_items")[0]["c"]
    scored = query_rows(conn, "SELECT COUNT(*) AS c FROM goat_consignment_scores")[0]["c"]
    with_ask = query_rows(conn, "SELECT COUNT(*) AS c FROM goat_consignment_scores WHERE stockx_lowest_ask IS NOT NULL")[0]["c"]
    missing_ask = query_rows(conn, "SELECT COUNT(*) AS c FROM goat_consignment_scores WHERE stockx_lowest_ask IS NULL")[0]["c"]
    style_count = query_rows(conn, "SELECT COUNT(DISTINCT style_no) AS c FROM goat_consignment_items")[0]["c"]
    return {
        "total_items": int(total or 0),
        "scored_items": int(scored or 0),
        "with_stockx_ask": int(with_ask or 0),
        "missing_stockx_ask": int(missing_ask or 0),
        "style_count": int(style_count or 0),
    }


def _resume_paused_stockx_task() -> None:
    if not PAUSED_STOCKX_TASK_PATH.exists():
        return
    try:
        paused = json_loads(PAUSED_STOCKX_TASK_PATH.read_text(encoding="utf-8"), {}) or {}
    except OSError:
        paused = {}
    try:
        PAUSED_STOCKX_TASK_PATH.unlink()
    except OSError:
        pass
    if paused.get("kind") != "stockx_full_refresh":
        return
    try:
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from scripts.auto_full_sync_worker import _run_full_sync; _run_full_sync()",
            ],
            cwd=str(BASE_DIR),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    except Exception:
        try:
            PAUSED_STOCKX_TASK_PATH.write_text(json.dumps(paused, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError:
            pass


def _progress(job_id: str, event: dict[str, Any]) -> None:
    with PROGRESS_LOCK:
        marker = _read_marker()
        total = event.get("total")
        completed = event.get("completed")
        marker.update(
            {
                "job_id": job_id,
                "status": "running",
                "phase": event.get("phase") or marker.get("phase"),
                "message": event.get("message") or marker.get("message", ""),
                "current_style": event.get("style_no") or marker.get("current_style"),
                "current_size": event.get("size") or marker.get("current_size"),
                "current_pid": event.get("pid") or marker.get("current_pid"),
                "updated_at": _now().isoformat(timespec="seconds"),
            }
        )
        if isinstance(total, int):
            marker["total"] = total
        if isinstance(completed, int):
            marker["completed"] = completed
        if isinstance(total, int) and total > 0 and isinstance(completed, int):
            marker["progress"] = round(max(0.0, min(0.99, completed / total)), 4)
        _write_marker(marker)


def _goat_stockx_worker_count(settings, style_count: int) -> int:
    raw = str(os.getenv("GOAT_STOCKX_MAX_WORKERS", "")).strip()
    try:
        configured = int(raw) if raw else min(int(getattr(settings, "sync_max_workers", 2) or 2), 2)
    except (TypeError, ValueError):
        configured = 2
    return max(1, min(configured, 3, max(1, style_count)))


def _load_goat_style_groups(conn) -> tuple[dict[str, list[dict[str, Any]]], int]:
    rows = [dict(row) for row in query_rows(conn, "SELECT * FROM goat_consignment_items ORDER BY imported_at DESC, id DESC")]
    style_groups: dict[str, list[dict[str, Any]]] = {}
    for item in rows:
        style_no = app_mod.normalize_style_no(item.get("style_no")) or str(item.get("style_no") or "").strip().upper()
        if not style_no:
            continue
        item["style_no"] = style_no
        style_groups.setdefault(style_no, []).append(item)
    return style_groups, len(rows)


def _score_goat_style_group(
    db_path: Path,
    style_no: str,
    items: list[dict[str, Any]],
    *,
    computed_at: str,
    live_refresh_missing: bool,
    progress_callback,
    item_done_callback,
) -> dict[str, Any]:
    conn = connect(db_path)
    init_db(conn)
    count = 0
    errors: list[str] = []
    style_refresh_cache: set[str] = set()
    size_refresh_cache: set[str] = set()
    try:
        if live_refresh_missing and app_mod._goat_style_needs_refresh(conn, style_no, items):
            progress_callback(
                {
                    "phase": "同步接口",
                    "style_no": style_no,
                    "message": f"正在查 {style_no} 的商品详情和历史数据",
                }
            )
            app_mod._refresh_stockx_style_snapshot_for_goat(
                conn,
                style_no,
                refresh_cache=style_refresh_cache,
                progress_callback=progress_callback,
            )

        for item in items:
            progress_callback(
                {
                    "phase": "评分",
                    "pid": item.get("pid"),
                    "style_no": item.get("style_no"),
                    "size": item.get("size"),
                    "message": f"正在处理 {item.get('style_no')} US {item.get('size')}",
                }
            )
            scored = app_mod._score_goat_consignment_item(
                conn,
                item,
                live_refresh_missing=live_refresh_missing,
                refresh_cache=size_refresh_cache,
                progress_callback=progress_callback,
            )
            app_mod._store_goat_consignment_score(conn, item, scored, computed_at)
            conn.commit()
            count += 1
            item_done_callback(item)
    except Exception as exc:  # noqa: BLE001 - one style must not stop the batch.
        errors.append(str(exc))
        try:
            log_sync(
                conn,
                f"GOAT清单补StockX失败 {style_no}: {exc}",
                severity="error",
                event_type="goat_stockx_style_error",
                style_no=style_no,
                details={"error": str(exc)},
            )
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()
    return {"style_no": style_no, "count": count, "errors": errors}


def _compute_goat_scores_parallel(settings, job_id: str, *, live_refresh_missing: bool) -> int:
    conn = connect(settings.db_path)
    init_db(conn)
    try:
        style_groups, total_items = _load_goat_style_groups(conn)
    finally:
        conn.close()

    computed_at = _now().isoformat(timespec="seconds")
    worker_count = _goat_stockx_worker_count(settings, len(style_groups))
    completed_lock = threading.Lock()
    completed = 0

    def progress_callback(event: dict[str, Any]) -> None:
        payload = dict(event)
        payload.setdefault("total", total_items)
        _progress(job_id, payload)

    def item_done_callback(item: dict[str, Any]) -> None:
        nonlocal completed
        with completed_lock:
            completed += 1
            current_completed = completed
        if current_completed == 1 or current_completed % 5 == 0 or current_completed == total_items:
            _progress(
                job_id,
                {
                    "phase": "评分",
                    "completed": current_completed,
                    "total": total_items,
                    "pid": item.get("pid"),
                    "style_no": item.get("style_no"),
                    "size": item.get("size"),
                    "message": f"已处理 {current_completed}/{total_items}（并发 {worker_count}）",
                },
            )

    _progress(
        job_id,
        {
            "phase": "启动并发",
            "completed": 0,
            "total": total_items,
            "message": f"StockX补数开始：{len(style_groups)} 个货号，{total_items} 行，并发 {worker_count}",
        },
    )

    errors = 0
    if not style_groups:
        return 0

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _score_goat_style_group,
                settings.db_path,
                style_no,
                items,
                computed_at=computed_at,
                live_refresh_missing=live_refresh_missing,
                progress_callback=progress_callback,
                item_done_callback=item_done_callback,
            ): style_no
            for style_no, items in style_groups.items()
        }
        for index, future in enumerate(as_completed(future_map), start=1):
            style_no = future_map[future]
            try:
                result = future.result()
                errors += len(result.get("errors") or [])
            except Exception as exc:  # noqa: BLE001
                errors += 1
                result = {"style_no": style_no, "count": 0, "errors": [str(exc)]}
            if index == 1 or index % 5 == 0 or index == len(future_map):
                with completed_lock:
                    current_completed = completed
                _progress(
                    job_id,
                    {
                        "phase": "并发补数",
                        "completed": current_completed,
                        "total": total_items,
                        "style_no": style_no,
                        "message": f"货号完成 {index}/{len(future_map)}，行完成 {current_completed}/{total_items}，错误 {errors}",
                    },
                )

    if errors:
        conn = connect(settings.db_path)
        init_db(conn)
        try:
            log_sync(
                conn,
                f"GOAT清单补StockX完成但有 {errors} 个货号错误",
                severity="warning",
                event_type="goat_stockx_worker_warning",
                details={"errors": errors},
            )
            conn.commit()
        finally:
            conn.close()
    return completed


def _run_job(request: dict[str, Any] | None = None) -> None:
    settings = get_settings()
    job_id = f"goat-{uuid.uuid4().hex[:8]}"
    conn = connect(settings.db_path)
    init_db(conn)
    started = _now()
    live_refresh_missing = bool((request or {}).get("live_refresh_missing", True))
    task_name = "GOAT清单补StockX并重评" if live_refresh_missing else "GOAT清单本地快照重评"
    try:
        before = _counts(conn)
        marker = {
            "job_id": job_id,
            "status": "running",
            "source_name": (request or {}).get("source_name") or "goat_stockx_worker",
            "started_at": started.isoformat(timespec="seconds"),
            "phase": task_name,
            "message": f"{task_name}已启动",
            "progress": 0,
            "completed": 0,
            "total": before["total_items"],
            "live_refresh_missing": live_refresh_missing,
            **before,
        }
        _write_marker(marker)
        conn.close()
        conn = None
        computed = _compute_goat_scores_parallel(settings, job_id, live_refresh_missing=live_refresh_missing)
        conn = connect(settings.db_path)
        init_db(conn)
        conn.commit()
        after = _counts(conn)
        finished = _now()
        marker = _read_marker()
        marker.update(
            {
                "job_id": job_id,
                "status": "done",
                "phase": "完成",
                "message": f"{task_name}完成：评分 {computed} 行，StockX最低Ask覆盖 {after['with_stockx_ask']}/{after['total_items']}",
                "progress": 1,
                "computed": computed,
                "finished_at": finished.isoformat(timespec="seconds"),
                **after,
            }
        )
        _write_marker(marker)
        log_sync(conn, marker["message"], event_type="goat_stockx_worker_done", details={"job_id": job_id, **after})
        conn.commit()
        _resume_paused_stockx_task()
    except Exception as exc:  # noqa: BLE001
        marker = _read_marker()
        marker.update(
            {
                "job_id": job_id,
                "status": "error",
                "phase": "失败",
                "message": f"GOAT清单补StockX失败：{exc}",
                "error": str(exc),
                "traceback": traceback.format_exc(limit=8),
                "finished_at": _now().isoformat(timespec="seconds"),
            }
        )
        _write_marker(marker)
        try:
            if conn is None:
                conn = connect(settings.db_path)
                init_db(conn)
            log_sync(
                conn,
                marker["message"],
                severity="error",
                event_type="goat_stockx_worker_error",
                details={"job_id": job_id, "error": str(exc)},
            )
            conn.commit()
        except Exception:
            pass
    finally:
        if conn is not None:
            conn.close()


def main() -> None:
    if not _acquire_lock():
        return
    try:
        force_once = "--once" in sys.argv
        while True:
            _touch_lock()
            request = _consume_request()
            if request is not None or force_once:
                _run_job(request or {"source_name": "manual_once"})
                if force_once:
                    return
            else:
                marker = _read_marker()
                marker.update(
                    {
                        "status": marker.get("status") if marker.get("status") in {"running", "done", "error"} else "idle",
                        "worker_heartbeat_at": _now().isoformat(timespec="seconds"),
                    }
                )
                _write_marker(marker)
            time.sleep(POLL_SECONDS)
    finally:
        _release_lock()


if __name__ == "__main__":
    main()
