from __future__ import annotations

import base64
import json
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
