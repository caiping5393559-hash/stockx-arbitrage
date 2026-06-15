from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
MARKER_PATH = BASE_DIR / "data" / "goat_stockx_worker.json"
PAUSED_STOCKX_TASK_PATH = BASE_DIR / "data" / "paused_stockx_task.json"


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def main() -> int:
    deadline = time.time() + 18 * 60 * 60
    while time.time() < deadline:
        marker = _read_json(MARKER_PATH)
        if marker.get("status") == "done" and PAUSED_STOCKX_TASK_PATH.exists():
            paused = _read_json(PAUSED_STOCKX_TASK_PATH)
            if paused.get("kind") == "stockx_full_refresh":
                try:
                    PAUSED_STOCKX_TASK_PATH.unlink()
                except OSError:
                    pass
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
            return 0
        if marker.get("status") in {"error", "paused_by_new_upload"}:
            return 0
        time.sleep(30)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
