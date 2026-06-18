from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def apply_stockx_progress_watermark(
    conn,
    *,
    import_id: int | None,
    scored_styles: int,
    scored_sizes: int,
    pending_styles: int,
    sync_state: dict[str, Any] | None = None,
    auto_status: dict[str, Any] | None = None,
) -> dict[str, int]:
    sync_state = sync_state or {}
    auto_status = auto_status or {}
    runtime_scored_sizes = max(
        int(sync_state.get("recomputed") or 0),
        int(sync_state.get("opportunity_scores") or 0),
        int(auto_status.get("recomputed") or 0),
        int(auto_status.get("opportunity_scores") or 0),
    )
    runtime_processed_styles = max(
        int(sync_state.get("completed") or 0),
        int(auto_status.get("completed") or 0),
    )
    scored_styles = max(int(scored_styles or 0), runtime_processed_styles)
    scored_sizes = max(int(scored_sizes or 0), runtime_scored_sizes)
    if runtime_processed_styles:
        pending_styles = max(0, int(pending_styles or 0) - runtime_processed_styles)
    if import_id is None:
        return {
            "scored_styles": int(scored_styles or 0),
            "scored_sizes": int(scored_sizes or 0),
            "pending_styles": int(pending_styles or 0),
        }
    row = conn.execute(
        """
        SELECT scored_styles, scored_sizes, pending_styles
        FROM stockx_import_progress_watermarks
        WHERE import_id = ?
        """,
        (import_id,),
    ).fetchone()
    previous = dict(row) if row else {}
    display_styles = max(int(previous.get("scored_styles") or 0), int(scored_styles or 0))
    display_sizes = max(int(previous.get("scored_sizes") or 0), int(scored_sizes or 0))
    previous_pending = previous.get("pending_styles")
    if previous_pending is None:
        display_pending = int(pending_styles or 0)
    else:
        display_pending = min(int(previous_pending or 0), int(pending_styles or 0))
    conn.execute(
        """
        INSERT INTO stockx_import_progress_watermarks (
            import_id, scored_styles, scored_sizes, pending_styles, updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(import_id) DO UPDATE SET
            scored_styles=MAX(stockx_import_progress_watermarks.scored_styles, excluded.scored_styles),
            scored_sizes=MAX(stockx_import_progress_watermarks.scored_sizes, excluded.scored_sizes),
            pending_styles=MIN(COALESCE(stockx_import_progress_watermarks.pending_styles, excluded.pending_styles), excluded.pending_styles),
            updated_at=excluded.updated_at
        """,
        (
            import_id,
            display_styles,
            display_sizes,
            display_pending,
            datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        ),
    )
    return {
        "scored_styles": display_styles,
        "scored_sizes": display_sizes,
        "pending_styles": display_pending,
    }
