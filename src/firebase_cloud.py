from __future__ import annotations

import base64
import gzip
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from .config import get_settings
from .db import init_db, utc_now


def _firebase_modules():
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore

        return firebase_admin, credentials, firestore
    except ImportError as exc:
        raise RuntimeError("firebase-admin is not installed. Run pip install -r requirements.txt.") from exc


def _service_account_info(settings) -> dict[str, Any] | None:
    if settings.firebase_service_account_json:
        return json.loads(settings.firebase_service_account_json)
    if settings.firebase_service_account_b64:
        raw = base64.b64decode(settings.firebase_service_account_b64).decode("utf-8")
        return json.loads(raw)
    if settings.firebase_credentials_path:
        path = Path(settings.firebase_credentials_path)
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    return None


def firebase_app():
    settings = get_settings()
    if not settings.firebase_enabled:
        return None

    firebase_admin, credentials, _ = _firebase_modules()
    if firebase_admin._apps:
        return firebase_admin.get_app()

    info = _service_account_info(settings)
    options = {"projectId": settings.firebase_project_id} if settings.firebase_project_id else None
    if info:
        cred = credentials.Certificate(info)
        return firebase_admin.initialize_app(cred, options=options)
    if settings.firebase_project_id:
        return firebase_admin.initialize_app(options=options)
    return firebase_admin.initialize_app()


def firestore_client():
    app = firebase_app()
    if app is None:
        return None
    _, _, firestore = _firebase_modules()
    return firestore.client(app=app)


def firebase_status() -> dict[str, Any]:
    settings = get_settings()
    if not settings.firebase_enabled:
        return {"enabled": False, "ok": False, "message": "Firebase disabled"}
    try:
        db = firestore_client()
        if db is None:
            return {"enabled": True, "ok": False, "message": "Firestore client not initialized"}
        marker_ref = db.collection(settings.firebase_collection_prefix).document("_health")
        marker_ref.set({"checked_at": utc_now(), "app": "stockx-goat-scanner"}, merge=True)
        return {
            "enabled": True,
            "ok": True,
            "project_id": settings.firebase_project_id or "-",
            "collection_prefix": settings.firebase_collection_prefix,
            "message": "Firestore connected",
        }
    except Exception as exc:
        return {"enabled": True, "ok": False, "message": str(exc)}


def write_cloud_event(event_type: str, payload: dict[str, Any]) -> None:
    settings = get_settings()
    if not settings.firebase_enabled:
        return
    db = firestore_client()
    if db is None:
        return
    db.collection(settings.firebase_collection_prefix).document("events").collection("items").add(
        {
            "event_type": event_type,
            "payload": payload,
            "created_at": utc_now(),
        }
    )


def _backup_root(db, settings):
    return db.collection(settings.firebase_collection_prefix).document("sqlite_backup")


def _core_backup_root(db, settings):
    return db.collection(settings.firebase_collection_prefix).document("core_backup")


def _score_watermark_root(db, settings):
    return db.collection(settings.firebase_collection_prefix).document("stockx_score_watermark")


CORE_BACKUP_TABLES = (
    "sku_imports",
    "sku_import_sheets",
    "sku_items",
    "products",
    "stockx_style_sync_status",
    "stockx_import_progress_watermarks",
    "opportunity_scores",
    "opportunity_import_snapshots",
    "opportunity_score_history",
    "goat_consignment_items",
    "goat_consignment_scores",
    "goat_consignment_import_history",
    "goat_consignment_history_items",
    "goat_consignment_history_scores",
    "goat_hidden_styles",
)

STOCKX_CORE_BACKUP_TABLES = (
    "sku_imports",
    "sku_import_sheets",
    "sku_items",
    "products",
    "stockx_style_sync_status",
    "stockx_import_progress_watermarks",
    "opportunity_scores",
    "opportunity_import_snapshots",
    "opportunity_score_history",
)


def _count_table_rows(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def _read_table_rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    return [dict(row) for row in rows]


def _core_row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in CORE_BACKUP_TABLES:
        if _table_exists(conn, table):
            counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
        else:
            counts[table] = 0
    return counts


def _remote_opportunity_score_count(db, settings) -> int:
    counts: list[int] = []
    try:
        watermark = _score_watermark_root(db, settings).get()
        if watermark.exists:
            data = watermark.to_dict() or {}
            counts.append(int(data.get("opportunity_scores") or 0))
            counts.append(int(data.get("scored_sizes") or 0))
    except Exception:
        pass
    for root in (_core_backup_root(db, settings), _backup_root(db, settings)):
        try:
            meta = root.get()
            if not meta.exists:
                continue
            data = meta.to_dict() or {}
            row_counts = data.get("row_counts") or {}
            counts.append(int(row_counts.get("opportunity_scores") or 0))
        except Exception:
            continue
    return max(counts) if counts else 0


def _remote_backup_opportunity_score_count(db, settings) -> int:
    counts: list[int] = []
    for root in (_core_backup_root(db, settings), _backup_root(db, settings)):
        try:
            meta = root.get()
            if not meta.exists:
                continue
            data = meta.to_dict() or {}
            row_counts = data.get("row_counts") or {}
            counts.append(int(row_counts.get("opportunity_scores") or 0))
        except Exception:
            continue
    return max(counts) if counts else 0


def _save_score_watermark(db, settings, row_counts: dict[str, int], reason: str) -> None:
    local_scores = max(
        int(row_counts.get("opportunity_scores") or 0),
        int(row_counts.get("scored_sizes") or 0),
    )
    if local_scores <= 0:
        return
    try:
        doc = _score_watermark_root(db, settings)
        current = doc.get()
        current_scores = 0
        current_styles = 0
        current_pending = None
        if current.exists:
            current_data = current.to_dict() or {}
            current_scores = max(
                int(current_data.get("opportunity_scores") or 0),
                int(current_data.get("scored_sizes") or 0),
            )
            current_styles = int(current_data.get("scored_styles") or 0)
            if current_data.get("pending_styles") is not None:
                current_pending = int(current_data.get("pending_styles") or 0)
        if local_scores > current_scores:
            scored_styles = max(current_styles, int(row_counts.get("scored_styles") or 0))
            pending_value = row_counts.get("pending_styles")
            pending_styles = current_pending
            if pending_value is not None:
                pending_styles = int(pending_value or 0) if pending_styles is None else min(pending_styles, int(pending_value or 0))
            payload: dict[str, Any] = {
                "opportunity_scores": local_scores,
                "scored_sizes": local_scores,
                "scored_styles": scored_styles,
                "reason": reason,
                "updated_at": utc_now(),
            }
            if pending_styles is not None:
                payload["pending_styles"] = pending_styles
            doc.set(payload, merge=True)
    except Exception as exc:
        write_cloud_event(
            "score_watermark_save_failed",
            {"reason": reason, "error": str(exc), "created_at": utc_now()},
        )


def save_stockx_score_watermark(
    db_path: Path | str,
    *,
    import_id: int | None = None,
    scored_styles: int = 0,
    scored_sizes: int = 0,
    pending_styles: int | None = None,
    reason: str = "progress",
) -> dict[str, Any]:
    settings = get_settings()
    if not settings.firebase_enabled:
        return {"ok": False, "message": "Firebase disabled"}
    db = firestore_client()
    if db is None:
        return {"ok": False, "message": "Firestore client not initialized"}

    row_counts: dict[str, int] = {
        "scored_styles": max(0, int(scored_styles or 0)),
        "scored_sizes": max(0, int(scored_sizes or 0)),
    }
    if pending_styles is not None:
        row_counts["pending_styles"] = max(0, int(pending_styles or 0))

    path = Path(db_path)
    if path.exists():
        conn = sqlite3.connect(path, timeout=60)
        conn.row_factory = sqlite3.Row
        try:
            init_db(conn)
            row_counts["opportunity_scores"] = max(
                int(row_counts.get("scored_sizes") or 0),
                _count_table_rows(conn, "opportunity_scores"),
            )
            if _table_exists(conn, "stockx_import_progress_watermarks"):
                params: tuple[Any, ...] = ()
                where = ""
                if import_id is not None:
                    where = "WHERE import_id = ?"
                    params = (import_id,)
                row = conn.execute(
                    f"""
                    SELECT
                        MAX(scored_styles) AS scored_styles,
                        MAX(scored_sizes) AS scored_sizes,
                        MIN(pending_styles) AS pending_styles
                    FROM stockx_import_progress_watermarks
                    {where}
                    """,
                    params,
                ).fetchone()
                if row:
                    row_counts["scored_styles"] = max(row_counts["scored_styles"], int(row["scored_styles"] or 0))
                    row_counts["scored_sizes"] = max(row_counts["scored_sizes"], int(row["scored_sizes"] or 0))
                    row_counts["opportunity_scores"] = max(row_counts["opportunity_scores"], int(row["scored_sizes"] or 0))
                    if row["pending_styles"] is not None:
                        local_pending = int(row["pending_styles"] or 0)
                        if pending_styles is None:
                            row_counts["pending_styles"] = local_pending
                        else:
                            row_counts["pending_styles"] = min(row_counts["pending_styles"], local_pending)
        finally:
            conn.close()

    before = _remote_opportunity_score_count(db, settings)
    _save_score_watermark(db, settings, row_counts, reason)
    after = _remote_opportunity_score_count(db, settings)
    return {"ok": True, "before": before, "after": after, "row_counts": row_counts}


def read_stockx_score_watermark() -> dict[str, int]:
    settings = get_settings()
    if not settings.firebase_enabled:
        return {}
    try:
        db = firestore_client()
        if db is None:
            return {}
        doc = _score_watermark_root(db, settings).get()
        if not doc.exists:
            return {}
        data = doc.to_dict() or {}
        result: dict[str, int] = {}
        for key in ("scored_styles", "scored_sizes", "opportunity_scores", "pending_styles"):
            if data.get(key) is not None:
                result[key] = max(0, int(data.get(key) or 0))
        return result
    except Exception:
        return {}


def _remote_core_opportunity_score_count(db, settings) -> int:
    try:
        meta = _core_backup_root(db, settings).get()
        if not meta.exists:
            return 0
        data = meta.to_dict() or {}
        row_counts = data.get("row_counts") or {}
        return int(row_counts.get("opportunity_scores") or 0)
    except Exception:
        return 0


def _should_skip_regressive_score_backup(db, settings, row_counts: dict[str, int], reason: str) -> bool:
    local_scores = int(row_counts.get("opportunity_scores") or 0)
    remote_scores = _remote_backup_opportunity_score_count(db, settings)
    if local_scores < remote_scores:
        write_cloud_event(
            "backup_skipped_regressive_scores",
            {
                "reason": reason,
                "local_opportunity_scores": local_scores,
                "remote_opportunity_scores": remote_scores,
                "created_at": utc_now(),
            },
        )
        return True
    return False


def _should_skip_regressive_sqlite_restore(sqlite_scores: int, core_scores: int) -> bool:
    return int(sqlite_scores or 0) < int(core_scores or 0)


def _replace_table_rows(conn: sqlite3.Connection, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows or not _table_exists(conn, table):
        return
    column_info = conn.execute(f"PRAGMA table_info({table})").fetchall()
    columns = [str(row[1]) for row in column_info]
    required_defaults: dict[str, Any] = {}
    for cid, name, col_type, not_null, default_value, primary_key in column_info:
        if primary_key or not not_null or default_value is not None:
            continue
        column = str(name)
        lowered = column.lower()
        type_name = str(col_type or "").upper()
        if lowered.endswith("_json"):
            required_defaults[column] = "[]" if lowered in {"raw_table_json"} else "{}"
        elif "INT" in type_name:
            required_defaults[column] = 0
        elif any(token in type_name for token in ("REAL", "FLOA", "DOUB", "NUM")):
            required_defaults[column] = 0.0
        else:
            required_defaults[column] = ""
    valid_rows = []
    for row in rows:
        valid_row = {key: value for key, value in row.items() if key in columns}
        for key, value in required_defaults.items():
            valid_row.setdefault(key, value)
        if valid_row:
            valid_rows.append(valid_row)
    if not valid_rows:
        return
    conn.execute(f"DELETE FROM {table}")
    for row in valid_rows:
        keys = list(row.keys())
        placeholders = ",".join("?" for _ in keys)
        quoted = ",".join(f'"{key}"' for key in keys)
        conn.execute(
            f"INSERT OR REPLACE INTO {table} ({quoted}) VALUES ({placeholders})",
            [row[key] for key in keys],
        )


def _write_chunked_payload(root, db, payload: dict[str, Any], meta_extra: dict[str, Any]) -> dict[str, Any]:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    compressed = gzip.compress(raw, compresslevel=6)
    encoded = base64.b64encode(compressed).decode("ascii")
    chunk_size = 650_000
    chunks = [encoded[index : index + chunk_size] for index in range(0, len(encoded), chunk_size)]

    old_chunks = list(root.collection("chunks").stream())
    for start in range(0, len(old_chunks), 400):
        batch = db.batch()
        for doc in old_chunks[start : start + 400]:
            batch.delete(doc.reference)
        batch.commit()

    for start in range(0, len(chunks), 400):
        batch = db.batch()
        for index, chunk in enumerate(chunks[start : start + 400], start=start):
            batch.set(root.collection("chunks").document(f"{index:06d}"), {"index": index, "data": chunk})
        batch.commit()

    meta = {
        "updated_at": utc_now(),
        "raw_size": len(raw),
        "compressed_size": len(compressed),
        "encoded_size": len(encoded),
        "chunk_count": len(chunks),
        **meta_extra,
    }
    root.set(meta)
    return meta


def _read_chunked_payload(root) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    meta = root.get()
    if not meta.exists:
        return None, {}
    meta_data = meta.to_dict() or {}
    chunk_count = int(meta_data.get("chunk_count") or 0)
    if chunk_count <= 0:
        return None, meta_data
    encoded_parts: list[str] = []
    for doc in root.collection("chunks").order_by("index").stream():
        data = doc.to_dict() or {}
        encoded = data.get("data")
        if encoded:
            encoded_parts.append(str(encoded))
    if len(encoded_parts) != chunk_count:
        return None, meta_data
    compressed = base64.b64decode("".join(encoded_parts))
    raw = gzip.decompress(compressed)
    return json.loads(raw.decode("utf-8")), meta_data


def backup_core_tables_to_firestore(db_path: Path | str, *, reason: str = "manual") -> dict[str, Any]:
    settings = get_settings()
    if not settings.firebase_enabled:
        return {"ok": False, "message": "Firebase disabled"}
    path = Path(db_path)
    if not path.exists():
        return {"ok": False, "message": f"SQLite file not found: {path}"}
    db = firestore_client()
    if db is None:
        return {"ok": False, "message": "Firestore client not initialized"}

    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        tables = {table: _read_table_rows(conn, table) for table in CORE_BACKUP_TABLES}
        row_counts = {table: len(rows) for table, rows in tables.items()}
    finally:
        conn.close()

    if int(row_counts.get("sku_imports") or 0) <= 0 or int(row_counts.get("sku_items") or 0) <= 0:
        return {
            "ok": False,
            "message": "Skipped backup: local StockX source list is empty",
            "row_counts": row_counts,
        }

    if _should_skip_regressive_score_backup(db, settings, row_counts, reason):
        return {
            "ok": False,
            "message": "Skipped backup: local opportunity_scores is lower than remote backup",
            "row_counts": row_counts,
        }

    payload = {"schema": 1, "tables": tables, "row_counts": row_counts}
    meta = _write_chunked_payload(
        _core_backup_root(db, settings),
        db,
        payload,
        {"reason": reason, "row_counts": row_counts},
    )
    _save_score_watermark(db, settings, row_counts, reason)
    write_cloud_event("core_backup_saved", meta)
    return {"ok": True, **meta}


def restore_core_tables_if_needed(db_path: Path | str) -> bool:
    settings = get_settings()
    if not settings.firebase_enabled:
        return False
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        init_db(conn)
        local_counts = _core_row_counts(conn)
        existing_imports = int(local_counts.get("sku_imports") or 0)
        existing_items = int(local_counts.get("sku_items") or 0)
        existing_scores = int(local_counts.get("opportunity_scores") or 0)
        existing_goat = conn.execute("SELECT COUNT(*) FROM goat_consignment_scores").fetchone()[0] or 0
    finally:
        conn.close()

    db = firestore_client()
    if db is None:
        return False
    payload, meta_data = _read_chunked_payload(_core_backup_root(db, settings))
    if not payload:
        return False
    tables = payload.get("tables") or {}
    remote_counts = payload.get("row_counts") or {}
    remote_imports = int(remote_counts.get("sku_imports") or 0)
    remote_items = int(remote_counts.get("sku_items") or 0)
    remote_scores = int(remote_counts.get("opportunity_scores") or 0)
    remote_score_floor = _remote_opportunity_score_count(db, settings)
    if existing_imports > 0 and existing_items > 0 and existing_scores > remote_scores and remote_scores < remote_score_floor:
        write_cloud_event(
            "core_restore_skipped_regressive_scores",
            {
                "local_opportunity_scores": existing_scores,
                "core_opportunity_scores": remote_scores,
                "remote_score_floor": remote_score_floor,
                "backup_updated_at": meta_data.get("updated_at"),
                "created_at": utc_now(),
            },
        )
        return False
    if existing_imports >= remote_imports and existing_items >= remote_items and existing_scores >= remote_scores:
        return False
    tables_to_restore = CORE_BACKUP_TABLES if not existing_goat else STOCKX_CORE_BACKUP_TABLES

    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        init_db(conn)
        conn.execute("BEGIN")
        for table in tables_to_restore:
            rows = tables.get(table) or []
            if isinstance(rows, list):
                _replace_table_rows(conn, table, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    write_cloud_event(
        "core_backup_restored",
        {"db_path": str(path), "backup_updated_at": meta_data.get("updated_at")},
    )
    return True


def restore_packaged_stockx_seed_if_empty(db_path: Path | str, seed_path: Path | str) -> bool:
    """Restore the bundled StockX source/results snapshot only when cloud SQLite is empty."""
    seed = Path(seed_path)
    if not seed.exists():
        return False

    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        init_db(conn)
        existing_imports = _count_table_rows(conn, "sku_imports")
        existing_items = _count_table_rows(conn, "sku_items")
        existing_scores = _count_table_rows(conn, "opportunity_scores")
        if existing_imports or existing_items or existing_scores:
            return False
    finally:
        conn.close()

    with gzip.open(seed, "rt", encoding="utf-8") as file:
        payload = json.load(file)
    tables = payload.get("tables") or {}
    row_counts = payload.get("row_counts") or {}
    if int(row_counts.get("sku_items") or 0) <= 0 or int(row_counts.get("opportunity_scores") or 0) <= 0:
        return False

    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        init_db(conn)
        conn.execute("BEGIN")
        for table in STOCKX_CORE_BACKUP_TABLES:
            rows = tables.get(table) or []
            if isinstance(rows, list):
                _replace_table_rows(conn, table, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    try:
        write_cloud_event(
            "packaged_stockx_seed_restored",
            {
                "db_path": str(path),
                "seed_path": str(seed),
                "row_counts": {table: int(row_counts.get(table) or 0) for table in STOCKX_CORE_BACKUP_TABLES},
                "created_at": utc_now(),
            },
        )
    except Exception:
        pass
    return True


def restore_sqlite_backup_if_needed(db_path: Path | str) -> bool:
    settings = get_settings()
    if not settings.firebase_enabled:
        return False
    path = Path(db_path)
    local_counts: dict[str, int] = {}
    if path.exists():
        try:
            conn = sqlite3.connect(path, timeout=60)
            conn.row_factory = sqlite3.Row
            try:
                init_db(conn)
                local_counts = _core_row_counts(conn)
            finally:
                conn.close()
            if (
                int(local_counts.get("sku_imports") or 0) > 0
                and int(local_counts.get("sku_items") or 0) > 0
                and int(local_counts.get("opportunity_scores") or 0) > 0
            ):
                return False
        except Exception:
            if path.stat().st_size > 32768:
                return False

    db = firestore_client()
    if db is None:
        return False
    root = _backup_root(db, settings)
    meta = root.get()
    if not meta.exists:
        return False
    meta_data = meta.to_dict() or {}
    chunk_count = int(meta_data.get("chunk_count") or 0)
    if chunk_count <= 0:
        return False
    sqlite_counts = meta_data.get("row_counts") or {}
    if int(sqlite_counts.get("sku_imports") or 0) <= 0 or int(sqlite_counts.get("sku_items") or 0) <= 0:
        write_cloud_event(
            "sqlite_restore_skipped_empty_source",
            {
                "sqlite_row_counts": sqlite_counts,
                "local_row_counts": local_counts,
                "backup_updated_at": meta_data.get("updated_at"),
                "created_at": utc_now(),
            },
        )
        return False
    sqlite_scores = int(sqlite_counts.get("opportunity_scores") or 0)
    core_scores = _remote_core_opportunity_score_count(db, settings)
    remote_score_floor = _remote_opportunity_score_count(db, settings)
    local_has_source = (
        int(local_counts.get("sku_imports") or 0) > 0
        and int(local_counts.get("sku_items") or 0) > 0
    )
    if local_has_source and _should_skip_regressive_sqlite_restore(sqlite_scores, max(core_scores, remote_score_floor)):
        write_cloud_event(
            "sqlite_restore_skipped_regressive_scores",
            {
                "sqlite_opportunity_scores": sqlite_scores,
                "core_opportunity_scores": core_scores,
                "remote_score_floor": remote_score_floor,
                "backup_updated_at": meta_data.get("updated_at"),
                "created_at": utc_now(),
            },
        )
        return False

    encoded_parts: list[str] = []
    chunks = root.collection("chunks").order_by("index").stream()
    for doc in chunks:
        data = doc.to_dict() or {}
        encoded = data.get("data")
        if encoded:
            encoded_parts.append(str(encoded))
    if len(encoded_parts) != chunk_count:
        return False

    compressed = base64.b64decode("".join(encoded_parts))
    raw = gzip.decompress(compressed)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".restore.tmp")
    tmp.write_bytes(raw)
    tmp.replace(path)
    write_cloud_event(
        "sqlite_backup_restored",
        {"db_path": str(path), "size": len(raw), "backup_updated_at": meta_data.get("updated_at")},
    )
    return True


def backup_sqlite_to_firestore(db_path: Path | str, *, reason: str = "manual") -> dict[str, Any]:
    settings = get_settings()
    if not settings.firebase_enabled:
        return {"ok": False, "message": "Firebase disabled"}
    path = Path(db_path)
    if not path.exists():
        return {"ok": False, "message": f"SQLite file not found: {path}"}

    max_mb = float(getattr(settings, "firebase_sqlite_backup_max_mb", 200) or 200)
    if path.stat().st_size > max_mb * 1024 * 1024:
        return {"ok": False, "message": f"SQLite file is larger than backup limit: {max_mb} MB"}

    db = firestore_client()
    if db is None:
        return {"ok": False, "message": "Firestore client not initialized"}

    local_counts: dict[str, int]
    count_conn = sqlite3.connect(path, timeout=60)
    count_conn.row_factory = sqlite3.Row
    try:
        init_db(count_conn)
        local_counts = _core_row_counts(count_conn)
    finally:
        count_conn.close()

    if _should_skip_regressive_score_backup(db, settings, local_counts, reason):
        return {
            "ok": False,
            "message": "Skipped SQLite backup: local opportunity_scores is lower than remote backup",
            "row_counts": local_counts,
        }

    with tempfile.TemporaryDirectory() as tmp_dir:
        copy_path = Path(tmp_dir) / "stockx_arbitrage.sqlite"
        source = sqlite3.connect(path, timeout=60)
        try:
            dest = sqlite3.connect(copy_path)
            try:
                source.backup(dest)
            finally:
                dest.close()
        finally:
            source.close()

        raw = copy_path.read_bytes()

    compressed = gzip.compress(raw, compresslevel=6)
    encoded = base64.b64encode(compressed).decode("ascii")
    chunk_size = 650_000
    chunks = [encoded[index : index + chunk_size] for index in range(0, len(encoded), chunk_size)]
    root = _backup_root(db, settings)

    old_chunks = list(root.collection("chunks").stream())
    for start in range(0, len(old_chunks), 400):
        batch = db.batch()
        for doc in old_chunks[start : start + 400]:
            batch.delete(doc.reference)
        batch.commit()

    for start in range(0, len(chunks), 400):
        batch = db.batch()
        for index, chunk in enumerate(chunks[start : start + 400], start=start):
            batch.set(root.collection("chunks").document(f"{index:06d}"), {"index": index, "data": chunk})
        batch.commit()

    meta = {
        "updated_at": utc_now(),
        "reason": reason,
        "raw_size": len(raw),
        "compressed_size": len(compressed),
        "encoded_size": len(encoded),
        "chunk_count": len(chunks),
        "row_counts": local_counts,
    }
    root.set(meta)
    _save_score_watermark(db, settings, local_counts, reason)
    write_cloud_event("sqlite_backup_saved", meta)
    return {"ok": True, **meta}
