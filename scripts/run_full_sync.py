from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics import compute_and_store_opportunities
from src.config import get_settings
from src.db import connect, init_db, query_rows
from src.sync import sync_style


def load_targets(db_path: Path) -> list[dict[str, Any]]:
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = query_rows(
            conn,
            """
            SELECT
                style_no,
                MAX(title_hint) AS title_hint,
                MIN(rank) AS rank
            FROM sku_items
            GROUP BY style_no
            ORDER BY COALESCE(MIN(rank), 999999), style_no
            """,
        )
        return [dict(row) for row in rows]
    finally:
        conn.close()


def run_one(db_path: Path, style_no: str, title_hint: str | None, include_size_endpoints: bool) -> dict[str, Any]:
    conn = connect(db_path)
    init_db(conn)
    try:
        summary = sync_style(
            conn,
            style_no,
            title_hint=title_hint,
            include_sales=True,
            include_depth=True,
            include_size_endpoints=include_size_endpoints,
            reset_snapshot=True,
        )
        return {
            "style_no": summary.style_no,
            "product_id": summary.product_id,
            "sizes": len(summary.sizes),
            "sales_rows": summary.sales_rows,
            "ask_rows": summary.ask_rows,
            "bid_rows": summary.bid_rows,
            "errors": summary.errors or [],
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--include-size-endpoints", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    db_path = Path(settings.db_path)
    targets = load_targets(db_path)
    total = len(targets)
    print(f"targets={total} db={db_path}", flush=True)

    done = 0
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        future_map = {
            executor.submit(
                run_one,
                db_path,
                str(target["style_no"]),
                str(target["title_hint"]) if target.get("title_hint") else None,
                bool(args.include_size_endpoints),
            ): target
            for target in targets
        }
        for future in as_completed(future_map):
            target = future_map[future]
            style_no = str(target["style_no"])
            done += 1
            try:
                result = future.result()
                errors = result["errors"]
                if errors:
                    failures.append(style_no)
                print(
                    f"[{done}/{total}] {style_no} "
                    f"sizes={result['sizes']} sales={result['sales_rows']} ask={result['ask_rows']} bid={result['bid_rows']} "
                    f"errors={len(errors)}",
                    flush=True,
                )
                if errors:
                    print("  " + " | ".join(errors[:3]), flush=True)
            except Exception as exc:  # noqa: BLE001
                failures.append(style_no)
                print(f"[{done}/{total}] {style_no} FAILED {exc}", flush=True)

    main_conn = connect(db_path)
    init_db(main_conn)
    try:
        scored = compute_and_store_opportunities(
            main_conn,
            fee_rate=settings.estimated_seller_fee_rate,
            sales_fraction=settings.buy_depth_sales_fraction,
        )
        main_conn.commit()
        stats = {
            "products": main_conn.execute("SELECT COUNT(DISTINCT style_no) FROM products").fetchone()[0] or 0,
            "product_sizes": main_conn.execute("SELECT COUNT(*) FROM product_sizes").fetchone()[0] or 0,
            "sales_history": main_conn.execute("SELECT COUNT(*) FROM sales_history").fetchone()[0] or 0,
            "ask_depth": main_conn.execute("SELECT COUNT(*) FROM ask_depth").fetchone()[0] or 0,
            "bid_depth": main_conn.execute("SELECT COUNT(*) FROM bid_depth").fetchone()[0] or 0,
            "opportunity_scores": main_conn.execute("SELECT COUNT(*) FROM opportunity_scores").fetchone()[0] or 0,
        }
        print(f"scored={scored}", flush=True)
        print(stats, flush=True)
        print(f"failures={len(set(failures))}", flush=True)
        if failures:
            print("failed_styles=" + ",".join(sorted(set(failures))), flush=True)
    finally:
        main_conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
