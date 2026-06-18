from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import firebase_cloud


class FakeSnapshot:
    def __init__(self, data: dict | None):
        self._data = data
        self.exists = data is not None

    def to_dict(self) -> dict:
        return self._data or {}


class FakeDoc:
    def __init__(self, data: dict | None = None):
        self.data = data

    def get(self) -> FakeSnapshot:
        return FakeSnapshot(self.data)


class FakeCollection:
    def __init__(self, docs: dict[str, FakeDoc]):
        self.docs = docs

    def document(self, name: str) -> FakeDoc:
        return self.docs.setdefault(name, FakeDoc(None))


class FakeDb:
    def __init__(self):
        self.docs = {
            "core_backup": FakeDoc({"row_counts": {"opportunity_scores": 125}}),
            "sqlite_backup": FakeDoc({"row_counts": {"opportunity_scores": 0}}),
        }

    def collection(self, _prefix: str) -> FakeCollection:
        return FakeCollection(self.docs)


def main() -> int:
    db = FakeDb()
    settings = SimpleNamespace(firebase_collection_prefix="stockx_test")
    firebase_cloud.write_cloud_event = lambda *_args, **_kwargs: None
    blocked = firebase_cloud._should_skip_destructive_zero_score_backup(
        db,
        settings,
        {"opportunity_scores": 0},
        "regression_test",
    )
    if not blocked:
        print("FAILED: zero-score backup was not blocked")
        return 1
    core_scores = firebase_cloud._remote_core_opportunity_score_count(db, settings)
    if core_scores != 125:
        print(f"FAILED: expected core score count 125, got {core_scores}")
        return 1
    print("OK: zero-score backup guard blocks destructive overwrite")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
