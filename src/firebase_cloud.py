from __future__ import annotations

import base64
import gzip
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from .config import get_settings
from .db import utc_now


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


def restore_sqlite_backup_if_needed(db_path: Path | str) -> bool:
    settings = get_settings()
    if not settings.firebase_enabled:
        return False
    path = Path(db_path)
    if path.exists() and path.stat().st_size > 32768:
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
    batch = db.batch()
    for doc in old_chunks:
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
    }
    root.set(meta)
    write_cloud_event("sqlite_backup_saved", meta)
    return {"ok": True, **meta}
