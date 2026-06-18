from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
import traceback
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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
SYNC_STATE_PATH = BASE_DIR / "data" / "sync_state.json"
WORKER_RUN_DIR = BASE_DIR / "data" / "stockx_worker_runs"
POLL_SECONDS = 60
MIN_INTERVAL_SECONDS = 15 * 60
LOCK_STALE_SECONDS = 15 * 60
WORKER_LOCK_STALE_SECONDS = 15 * 60
INCREMENTAL_SCORE_BATCH_SIZE = 4
STYLE_SYNC_HARD_TIMEOUT_SECONDS = 180
CHECKPOINT_STYLE_INTERVAL = 2
CHECKPOINT_MIN_SECONDS = 60


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


def _write_sync_state(data: dict[str, Any]) -> None:
    SYNC_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = SYNC_STATE_PATH.with_name(f"{SYNC_STATE_PATH.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp_path, SYNC_STATE_PATH)


def _update_sync_state(job_id: str, **changes: Any) -> None:
    try:
        try:
            state = json_loads(SYNC_STATE_PATH.read_text(encoding="utf-8"), {}) or {}
        except OSError:
            state = {}
        state.update(
            {
                "job_id": job_id,
                "status": "running",
                "updated_at": _now().isoformat(timespec="seconds"),
            }
        )
        state.update(changes)
        _write_sync_state(state)
    except Exception:
        pass


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


def _load_imported_styles(conn, import_id: int | None = None) -> list[str]:
    if import_id is None:
        rows = query_rows(
            conn,
            """
            SELECT DISTINCT style_no
            FROM sku_items
            WHERE style_no IS NOT NULL AND TRIM(style_no) != ''
            ORDER BY style_no
            """,
        )
    else:
        rows = query_rows(
            conn,
            """
            SELECT DISTINCT style_no
            FROM sku_items
            WHERE import_id = ?
              AND style_no IS NOT NULL AND TRIM(style_no) != ''
            ORDER BY style_no
            """,
            (import_id,),
        )
    return [str(row["style_no"]).strip().upper() for row in rows if row["style_no"]]


def _latest_stockx_import_id(conn) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM sku_imports
        WHERE source_name IN ('stockx_top1000', 'manual')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    return int(row["id"]) if row else None


def _record_style_sync_status(
    conn: sqlite3.Connection,
    *,
    import_id: int | None,
    style_no: str,
    status: str,
    result: dict[str, Any] | None = None,
    message: str | None = None,
) -> None:
    result = result or {}
    errors = list(result.get("errors") or [])
    conn.execute(
        """
        INSERT INTO stockx_style_sync_status (
            import_id, style_no, status, product_id, sizes_count,
            sales_rows, ask_rows, bid_rows, error_count, message, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(import_id, style_no) DO UPDATE SET
            status=excluded.status,
            product_id=excluded.product_id,
            sizes_count=excluded.sizes_count,
            sales_rows=excluded.sales_rows,
            ask_rows=excluded.ask_rows,
            bid_rows=excluded.bid_rows,
            error_count=excluded.error_count,
            message=excluded.message,
            updated_at=excluded.updated_at
        """,
        (
            import_id,
            style_no,
            status,
            result.get("product_id"),
            int(result.get("sizes") or 0),
            int(result.get("sales_rows") or 0),
            int(result.get("ask_rows") or 0),
            int(result.get("bid_rows") or 0),
            len(errors),
            message or (errors[0] if errors else status),
            _now().isoformat(timespec="seconds"),
        ),
    )


def _sync_one_style(db_path: Path, style_no: str) -> dict[str, Any]:
    settings = get_settings()
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
        score_rows = 0
        score_error = None
        try:
            worker_conn.commit()
            score_rows = compute_and_store_opportunities(
                worker_conn,
                fee_rate=settings.estimated_seller_fee_rate,
                sales_fraction=settings.buy_depth_sales_fraction,
                style_nos=[style_no],
            )
        except Exception as exc:  # noqa: BLE001
            worker_conn.rollback()
            score_error = f"score_failed: {exc}"
        return {
            "style_no": style_no,
            "sizes": len(summary.sizes),
            "sales_rows": summary.sales_rows,
            "ask_rows": summary.ask_rows,
            "bid_rows": summary.bid_rows,
            "score_rows": score_rows,
            "errors": [*(summary.errors or []), *([score_error] if score_error else [])],
        }
    finally:
        worker_conn.close()


def _run_style_child() -> int:
    db_path = Path(os.environ["STOCKX_STYLE_DB_PATH"])
    style_no = str(os.environ["STOCKX_STYLE_NO"]).strip().upper()
    result = _sync_one_style(db_path, style_no)
    result_path = os.environ.get("STOCKX_STYLE_RESULT_PATH")
    if result_path:
        Path(result_path).write_text(json.dumps(result, ensure_ascii=False, default=str), encoding="utf-8")
    else:
        print(json.dumps(result, ensure_ascii=False, default=str), flush=True)
    return 0


def _parse_child_result(result_path: Path, style_no: str) -> dict[str, Any]:
    try:
        data = json_loads(result_path.read_text(encoding="utf-8"), {})
        if isinstance(data, dict):
            return data
    except OSError:
        pass
    return {"style_no": style_no, "errors": ["子进程没有返回可解析结果"]}


def _read_tail(path: Path, limit: int = 2000) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[-limit:]
    except OSError:
        return ""


def _kill_process(proc: subprocess.Popen[Any]) -> None:
    try:
        proc.kill()
    except Exception:
        pass
    try:
        proc.wait(timeout=10)
    except Exception:
        pass


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
        import_id = _latest_stockx_import_id(conn)
        styles = _load_imported_styles(conn, import_id)
        started = _now()
        existing_scores = _count_opportunity_scores(conn)
        marker.update(
            {
                "enabled": True,
                "active_job_id": job_id,
                "last_started_at": started.isoformat(timespec="seconds"),
                "last_started_ts": started.timestamp(),
                "last_status": "running",
                "last_style_count": len(styles),
                "completed": int(marker.get("completed") or 0),
                "total": len(styles),
                "recomputed": int(marker.get("recomputed") or 0),
                "opportunity_scores": existing_scores,
                "last_finished_at": None,
                "last_finished_ts": None,
                "last_error": None,
                "last_traceback": None,
                "current_style": None,
                "last_message": f"今日机会全量刷新StockX API开始：{len(styles)} 个货号",
            }
        )
        _write_marker(marker)
        _update_sync_state(
            job_id,
            status="running",
            message=f"今日机会全量刷新StockX API开始：{len(styles)} 个货号",
            progress=0.0,
            completed=int(marker.get("completed") or 0),
            total=len(styles),
            current_style=None,
            current_size=None,
            current_phase="StockX API",
            current_endpoint="启动",
            recomputed=int(marker.get("recomputed") or 0),
        )

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
            score_conn = connect(settings.db_path)
            init_db(score_conn)
            try:
                batch_recomputed = compute_and_store_opportunities(
                    score_conn,
                    fee_rate=settings.estimated_seller_fee_rate,
                    sales_fraction=settings.buy_depth_sales_fraction,
                    style_nos=batch_styles,
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower():
                    score_conn.rollback()
                    return
                raise
            finally:
                try:
                    score_conn.close()
                except Exception:
                    pass
            pending_recompute = []
            recomputed += batch_recomputed
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

        def finish_style_result(
            style_no: str,
            result: dict[str, Any] | None = None,
            *,
            timed_out: bool = False,
            error_message: str | None = None,
            log_tail: str = "",
        ) -> None:
            nonlocal completed, errors, recomputed
            completed += 1
            result_score_rows = int((result or {}).get("score_rows") or 0)
            if result_score_rows > 0:
                recomputed += result_score_rows
            else:
                pending_recompute.append(style_no)
            if timed_out:
                errors += 1
                _record_style_sync_status(
                    conn,
                    import_id=import_id,
                    style_no=style_no,
                    status="timeout",
                    message=f"单货号超过 {STYLE_SYNC_HARD_TIMEOUT_SECONDS} 秒，已跳过",
                )
                log_sync(
                    conn,
                    f"自动全量同步 {style_no} 超时，已杀掉该货号进程并继续下一个",
                    severity="error",
                    event_type="auto_full_sync_style_timeout",
                    style_no=style_no,
                    details={
                        "job_id": job_id,
                        "timeout_seconds": STYLE_SYNC_HARD_TIMEOUT_SECONDS,
                        "log_tail": log_tail,
                    },
                )
            else:
                result = result or {"style_no": style_no, "errors": []}
                style_errors = list(result.get("errors") or [])
                if error_message:
                    style_errors.append(error_message)
                if log_tail:
                    style_errors.append(log_tail)
                errors += len(style_errors)
                _record_style_sync_status(
                    conn,
                    import_id=import_id,
                    style_no=style_no,
                    status="error" if style_errors else "done",
                    result=result,
                    message=style_errors[0] if style_errors else "done",
                )
                if style_errors:
                    log_sync(
                        conn,
                        f"自动全量同步 {style_no} 失败：{style_errors[0]}",
                        severity="error",
                        event_type="auto_full_sync_style_error",
                        style_no=style_no,
                        details={"job_id": job_id, "errors": style_errors[:5]},
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
            _update_sync_state(
                job_id,
                completed=completed,
                total=len(styles),
                current_style=style_no,
                current_phase="StockX API",
                current_endpoint="timeout_skip" if timed_out else "style_done",
                progress=completed / len(styles) if styles else 1.0,
                message=f"已处理 {completed}/{len(styles)}，已增量重算 {recomputed} 个尺码",
                recomputed=recomputed,
            )
            # Commit the status/log writes before opening a second connection for scoring.
            # Otherwise SQLite can keep the writer lock on the main connection and the
            # incremental scoring pass silently waits/returns without updating scores.
            conn.commit()
            if pending_recompute:
                try_recompute_pending(style_no)

        inline_mode = os.environ.get("STOCKX_INLINE_STYLE_WORKER") == "1"
        if inline_mode:
            worker_count = max(1, min(worker_count, 3))
            next_index = 0
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_map: dict[Any, tuple[str, float]] = {}

                def submit_inline() -> None:
                    nonlocal next_index
                    if next_index >= len(styles):
                        return
                    style_no = styles[next_index]
                    next_index += 1
                    future = executor.submit(_sync_one_style, settings.db_path, style_no)
                    future_map[future] = (style_no, time.monotonic())
                    _update_sync_state(
                        job_id,
                        completed=completed,
                        total=len(styles),
                        current_style=style_no,
                        current_phase="StockX API",
                        current_endpoint="inline_dispatch",
                        progress=completed / len(styles) if styles else 1.0,
                        message=f"已派发 {next_index}/{len(styles)}，Render内联并发 {worker_count}",
                        recomputed=recomputed,
                    )

                for _ in range(worker_count):
                    submit_inline()

                while future_map:
                    done, _ = wait(future_map.keys(), timeout=2, return_when=FIRST_COMPLETED)
                    timed_out = [
                        future
                        for future, (_, started_at) in list(future_map.items())
                        if time.monotonic() - started_at >= STYLE_SYNC_HARD_TIMEOUT_SECONDS and not future.done()
                    ]
                    for future in timed_out:
                        style_no, _ = future_map.pop(future)
                        future.cancel()
                        finish_style_result(style_no, timed_out=True)
                        submit_inline()
                    for future in done:
                        if future not in future_map:
                            continue
                        style_no, _ = future_map.pop(future)
                        try:
                            finish_style_result(style_no, future.result())
                        except Exception as exc:  # noqa: BLE001
                            finish_style_result(
                                style_no,
                                {"style_no": style_no, "errors": [str(exc)]},
                                error_message=traceback.format_exc(limit=4),
                            )
                        submit_inline()
        else:
            next_index = 0
            process_map: dict[subprocess.Popen[str], dict[str, Any]] = {}

            def submit_next() -> None:
                nonlocal next_index
                if next_index >= len(styles):
                    return
                style_no = styles[next_index]
                next_index += 1
                WORKER_RUN_DIR.mkdir(parents=True, exist_ok=True)
                run_key = f"{job_id}_{next_index:05d}_{style_no.replace('/', '_').replace(' ', '_')}"
                result_path = WORKER_RUN_DIR / f"{run_key}.json"
                log_path = WORKER_RUN_DIR / f"{run_key}.log"
                log_handle = log_path.open("w", encoding="utf-8", errors="replace")
                env = os.environ.copy()
                env["STOCKX_STYLE_WORKER"] = "1"
                env["STOCKX_STYLE_DB_PATH"] = str(settings.db_path)
                env["STOCKX_STYLE_NO"] = style_no
                env["STOCKX_STYLE_RESULT_PATH"] = str(result_path)
                proc = subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve())],
                    cwd=str(BASE_DIR),
                    env=env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                )
                process_map[proc] = {
                    "style_no": style_no,
                    "started_at": time.monotonic(),
                    "result_path": result_path,
                    "log_path": log_path,
                    "log_handle": log_handle,
                }
                _update_sync_state(
                    job_id,
                    completed=completed,
                    total=len(styles),
                    current_style=style_no,
                    current_phase="StockX API",
                    current_endpoint="已派发",
                    progress=completed / len(styles) if styles else 1.0,
                    message=f"已派发 {next_index}/{len(styles)}，并发 {worker_count}",
                    recomputed=recomputed,
                )

            def finish_process_style(proc: subprocess.Popen[str], timed_out: bool = False) -> None:
                meta = process_map.pop(proc, None)
                if not meta:
                    return
                style_no = str(meta["style_no"])
                result_path = Path(meta["result_path"])
                log_path = Path(meta["log_path"])
                try:
                    meta["log_handle"].close()
                except Exception:
                    pass
                if timed_out:
                    _kill_process(proc)
                    finish_style_result(style_no, timed_out=True, log_tail=_read_tail(log_path))
                    return
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    _kill_process(proc)
                    finish_style_result(style_no, timed_out=True, log_tail=_read_tail(log_path))
                    return
                result = _parse_child_result(result_path, style_no)
                log_tail = _read_tail(log_path)
                error_message = f"子进程退出码 {proc.returncode}" if proc.returncode not in (0, None) else None
                finish_style_result(style_no, result, error_message=error_message, log_tail=log_tail)

            try:
                for _ in range(worker_count):
                    submit_next()

                while process_map:
                    now_monotonic = time.monotonic()
                    finished: list[subprocess.Popen[str]] = []
                    timed_out: list[subprocess.Popen[str]] = []
                    for proc, meta in list(process_map.items()):
                        if proc.poll() is not None:
                            finished.append(proc)
                        elif now_monotonic - float(meta["started_at"]) >= STYLE_SYNC_HARD_TIMEOUT_SECONDS:
                            timed_out.append(proc)

                    if not finished and not timed_out:
                        time.sleep(2)
                        continue

                    for proc in finished:
                        finish_process_style(proc, timed_out=False)
                        submit_next()
                    for proc in timed_out:
                        if proc in process_map:
                            finish_process_style(proc, timed_out=True)
                            submit_next()
            finally:
                for proc, meta in list(process_map.items()):
                    _kill_process(proc)
                    try:
                        meta["log_handle"].close()
                    except Exception:
                        pass
                process_map.clear()

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
        _update_sync_state(
            job_id,
            status="done",
            progress=1.0,
            completed=len(styles),
            total=len(styles),
            current_style=None,
            current_phase="完成",
            current_endpoint=None,
            message=marker["last_message"],
            recomputed=recomputed,
            finished_at=finished.isoformat(timespec="seconds"),
        )
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
        _update_sync_state(
            job_id,
            status="error",
            message=marker["last_message"],
            error=str(exc),
            current_phase="失败",
            current_endpoint=None,
            finished_at=_now().isoformat(timespec="seconds"),
        )
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
    if os.environ.get("STOCKX_STYLE_WORKER") == "1":
        raise SystemExit(_run_style_child())
    main()
