from __future__ import annotations

import io
import re
import sqlite3
import zipfile
from dataclasses import dataclass
from typing import Any

import pandas as pd

from .db import json_dumps, upsert_reference_price, utc_now
from .parsing import normalize_style_no


NIKE_NUMERIC_STYLE_RE = re.compile(r"\b(\d{6})[- ](\d{3})\b")
STYLE_TOKEN_RE = re.compile(r"^[A-Z0-9]{2,14}(?:-[A-Z0-9]{2,8}){0,2}$")
HEADER_LIKE_VALUES = {
    "SKU",
    "STYLE",
    "STYLE-NO",
    "STYLE-NUMBER",
    "STYLEID",
    "PRODUCT-SKU",
    "US-RELEASE-DATE",
    "RELEASE-DATE",
    "COLOR",
    "PRODUCT-NAME",
}

STYLE_COLUMN_CANDIDATES = (
    "style no",
    "style-no",
    "style number",
    "styleid",
    "style id",
    "style_no",
    "styleno",
    "货号",
    "款号",
    "sku",
    "style",
    "product sku",
)

STYLE_COLUMN_CANDIDATES = (
    "货号",
    "款号",
    "商品货号",
    "商品款号",
    "货品货号",
    "产品货号",
    "鞋款货号",
    "sku",
    "product sku",
    "product_sku",
    "product-sku",
    "merchant sku",
    "merchantsku",
    "item sku",
    "itemsku",
    "model",
    "model no",
    "model number",
    "style",
    "style no",
    "style-no",
    "style number",
    "style code",
    "stylecode",
    "styleid",
    "style id",
    "style_no",
    "styleno",
    *STYLE_COLUMN_CANDIDATES,
)

REFERENCE_PRICE_HINTS = (
    "goat",
    "market",
    "current",
    "ask",
    "sell",
    "price",
)

RANK_COLUMN_CANDIDATES = (
    "rank",
    "ranking",
    "top",
    "position",
    "no",
    "number",
    "排名",
)

RANK_COLUMN_CANDIDATES = (
    "排名",
    "排行",
    "榜单排名",
    *RANK_COLUMN_CANDIDATES,
)

TITLE_COLUMN_CANDIDATES = (
    "title",
    "name",
    "product",
    "product name",
    "商品名",
    "名称",
)

TITLE_COLUMN_CANDIDATES = (
    "商品名",
    "商品名称",
    "名称",
    "标题",
    *TITLE_COLUMN_CANDIDATES,
)


STOCKX_TOP1000_MARKERS = {
    "productname",
    "styleid",
    "lowestask",
    "last72hrsalecount",
    "annualsalescount",
}


@dataclass
class ImportResult:
    import_id: int
    rows_seen: int
    rows_imported: int
    sheets: list[str]

    @property
    def sheet_names(self) -> list[str]:
        return self.sheets


def normalize_column(name: Any) -> str:
    return re.sub(r"[\s_\-./()（）:：#]+", "", str(name).strip().lower())


def _header_aliases(candidates: tuple[str, ...]) -> set[str]:
    return {normalize_column(candidate) for candidate in candidates}


def _find_column(columns: list[Any], candidates: tuple[str, ...]) -> Any | None:
    normalized_columns = {normalize_column(column): column for column in columns}
    aliases = _header_aliases(candidates)
    for normalized_candidate in aliases:
        if normalized_candidate in normalized_columns:
            return normalized_columns[normalized_candidate]
    for normalized_column, column in normalized_columns.items():
        if any(alias and alias in normalized_column for alias in aliases if len(alias) >= 4):
            return column
    return None


def _make_unique_columns(values: list[Any]) -> list[str]:
    counts: dict[str, int] = {}
    columns: list[str] = []
    for index, value in enumerate(values):
        base = str(value or "").strip() or f"column_{index + 1}"
        count = counts.get(base, 0)
        counts[base] = count + 1
        columns.append(base if count == 0 else f"{base}_{count + 1}")
    return columns


def _sku_header_score(values: list[Any]) -> int:
    aliases = set()
    for candidates in (STYLE_COLUMN_CANDIDATES, RANK_COLUMN_CANDIDATES, TITLE_COLUMN_CANDIDATES):
        aliases.update(_header_aliases(candidates))
    score = 0
    for value in values:
        key = normalize_column(value)
        if key in aliases:
            score += 3
        elif any(alias and alias in key for alias in aliases if len(alias) >= 4):
            score += 1
    return score


def _frame_from_raw_table(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw
    best_idx = 0
    best_score = -1
    for idx in range(min(len(raw), 25)):
        values = [None if pd.isna(v) else v for v in raw.iloc[idx].tolist()]
        score = _sku_header_score(values)
        if score > best_score:
            best_idx = idx
            best_score = score
    if best_score >= 2:
        frame = raw.iloc[best_idx + 1 :].copy()
        frame.columns = _make_unique_columns([None if pd.isna(v) else v for v in raw.iloc[best_idx].tolist()])
        return frame.dropna(how="all").reset_index(drop=True)
    raw = raw.copy()
    raw.columns = _make_unique_columns(raw.columns.tolist())
    return raw.dropna(how="all").reset_index(drop=True)


def _clean_style(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return _extract_style(text)


def _clean_style_from_named_column(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return _extract_style(text)


def _clean_stockx_top1000_style(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return normalize_style_no(text)


def _scan_row_for_style(row: pd.Series) -> str | None:
    for value in row.tolist():
        if value is None or pd.isna(value):
            continue
        style_no = _extract_style(str(value))
        if style_no:
            return style_no
    return None


def _style_content_score(series: pd.Series) -> float:
    values = [None if pd.isna(v) else v for v in series.head(300).tolist()]
    nonempty = [v for v in values if v not in (None, "")]
    if not nonempty:
        return 0.0
    hits = 0
    for value in nonempty:
        if _clean_style(value):
            hits += 1
    return hits / max(len(nonempty), 1)


def _detect_style_column_by_content(frame: pd.DataFrame) -> Any | None:
    best_column = None
    best_score = 0.0
    for column in frame.columns:
        score = _style_content_score(frame[column])
        if score > best_score:
            best_score = score
            best_column = column
    return best_column if best_score >= 0.45 else None


def _extract_style(text: str) -> str | None:
    normalized = normalize_style_no(text)
    if not normalized:
        return None
    return normalized if _looks_like_style_no(normalized) else None


def _looks_like_style_no(value: str) -> bool:
    text = value.strip().upper().replace(" ", "-")
    if text in HEADER_LIKE_VALUES:
        return False
    if len(text) < 4 or len(text) > 24:
        return False
    if not any(char.isdigit() for char in text):
        return False
    if not STYLE_TOKEN_RE.fullmatch(text):
        return False
    if text.replace("-", "").isdigit():
        return bool(NIKE_NUMERIC_STYLE_RE.fullmatch(text))
    return True


def _clean_rank(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(str(value).strip()))
    except ValueError:
        return None


def _clean_price(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        text = str(value).strip().replace("$", "").replace(",", "")
        if not text:
            return None
        price = float(text)
        if price <= 0:
            return None
        return price
    except ValueError:
        return None


def _find_reference_price(row: pd.Series) -> tuple[float | None, str | None]:
    for preferred in ("Lowest Ask", "lowest_ask", "Current Ask", "Ask"):
        value = _row_value(row, preferred)
        price = _clean_price(value)
        if price is not None:
            return price, preferred
    best: tuple[float | None, str | None] = (None, None)
    for column, value in row.items():
        normalized = normalize_column(column)
        if not any(hint in normalized for hint in REFERENCE_PRICE_HINTS):
            continue
        if normalized in {"rank", "ranking", "position", "no", "number"}:
            continue
        price = _clean_price(value)
        if price is None:
            continue
        best = (price, str(column))
        break
    return best


def _stockx_top1000_reference_price(row: pd.Series) -> tuple[float | None, str | None]:
    price = _clean_price(_row_value(row, "Lowest Ask", "lowest_ask"))
    return (price, "Lowest Ask") if price is not None else (None, None)


def _looks_like_stockx_top1000(frame: pd.DataFrame) -> bool:
    columns = {normalize_column(column) for column in frame.columns}
    return len(columns & STOCKX_TOP1000_MARKERS) >= 3


def _stockx_top1000_style_column(frame: pd.DataFrame) -> Any | None:
    normalized = {normalize_column(column): column for column in frame.columns}
    for name in ("StyleID", "Style ID", "style_id", "styleid"):
        column = normalized.get(normalize_column(name))
        if column is not None:
            return column
    return None


def _stockx_category_from_name(file_name: str, sheet_name: str) -> str | None:
    text = f"{file_name} {sheet_name}".lower()
    for category in ("sneakers", "shoes", "apparel", "accessories", "collectibles", "electronics", "trading-cards"):
        if category in text:
            return category
    return None


def _row_value(row: pd.Series, *names: str) -> Any:
    normalized = {normalize_column(column): column for column in row.index}
    for name in names:
        column = normalized.get(normalize_column(name))
        if column is not None:
            return row.get(column)
    return None


def _clean_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _upsert_import_product(conn: sqlite3.Connection, style_no: str, row: pd.Series) -> None:
    product_id = _clean_text(_row_value(row, "Id", "Product Id", "product_id"))
    title = _clean_text(_row_value(row, "Product Name", "Title", "Name"))
    brand = _clean_text(_row_value(row, "Brand"))
    release_date = _clean_text(_row_value(row, "Release Date", "release_date"))
    image_url = _clean_text(_row_value(row, "ImageUrl", "Image URL", "image_url"))
    if not any((product_id, title, brand, release_date, image_url)):
        return
    conn.execute(
        """
        INSERT INTO products (
            product_id, style_no, title, brand, release_date, image_url, raw_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(style_no) DO UPDATE SET
            product_id=COALESCE(excluded.product_id, products.product_id),
            title=COALESCE(excluded.title, products.title),
            brand=COALESCE(excluded.brand, products.brand),
            release_date=COALESCE(excluded.release_date, products.release_date),
            image_url=COALESCE(excluded.image_url, products.image_url),
            raw_json=excluded.raw_json,
            updated_at=excluded.updated_at
        """,
        (
            product_id,
            style_no,
            title,
            brand,
            release_date,
            image_url,
            json_dumps(row.to_dict()),
            utc_now(),
        ),
    )


def _dataframes_from_upload(file_name: str, content: bytes) -> dict[str, pd.DataFrame]:
    lower_name = file_name.lower()
    if lower_name.endswith(".zip"):
        dataframes: dict[str, pd.DataFrame] = {}
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                inner_name = info.filename.replace("\\", "/").split("/")[-1]
                if inner_name.startswith("~$") or not inner_name.lower().endswith((".csv", ".xlsx", ".xls")):
                    continue
                for sheet_name, frame in _dataframes_from_upload(inner_name, archive.read(info)).items():
                    dataframes[f"{inner_name}::{sheet_name}"] = frame
        if not dataframes:
            raise ValueError("ZIP 里没有可导入的 CSV/XLSX 文件")
        return dataframes
    if lower_name.endswith(".csv"):
        last_error: Exception | None = None
        for encoding in ("utf-8-sig", "utf-8", "gb18030", "gbk", None):
            try:
                raw = pd.read_csv(io.BytesIO(content), header=None, encoding=encoding)
                break
            except UnicodeDecodeError as exc:
                last_error = exc
        else:
            raise last_error or ValueError("CSV 文件编码无法识别")
        return {"CSV": _frame_from_raw_table(raw)}
    if lower_name.endswith((".xlsx", ".xls")):
        sheets = pd.read_excel(io.BytesIO(content), sheet_name=None, header=None)
        return {str(name): _frame_from_raw_table(frame) for name, frame in sheets.items()}
    raise ValueError("仅支持 .csv 或 .xlsx 文件")


def import_sku_file(
    conn: sqlite3.Connection,
    *,
    file_name: str,
    content: bytes,
    source_name: str = "manual",
) -> ImportResult:
    dataframes = _dataframes_from_upload(file_name, content)
    imported_at = utc_now()
    cur = conn.execute(
        "INSERT INTO sku_imports (source_name, file_name, imported_at) VALUES (?, ?, ?)",
        (source_name, file_name, imported_at),
    )
    import_id = int(cur.lastrowid)
    rows_seen = 0
    rows_imported = 0
    sheet_names: list[str] = []

    for sheet_name, frame in dataframes.items():
        sheet_names.append(sheet_name)
        clean_frame = frame.where(pd.notnull(frame), None)
        raw_rows = clean_frame.to_dict(orient="records")
        rows_seen += len(raw_rows)
        conn.execute(
            """
            INSERT INTO sku_import_sheets (import_id, sheet_name, row_count, raw_table_json)
            VALUES (?, ?, ?, ?)
            """,
            (import_id, sheet_name, len(raw_rows), json_dumps(raw_rows)),
        )

        rank_col = _find_column(list(clean_frame.columns), RANK_COLUMN_CANDIDATES)
        title_col = _find_column(list(clean_frame.columns), TITLE_COLUMN_CANDIDATES)
        is_stockx_top1000 = _looks_like_stockx_top1000(clean_frame)
        if is_stockx_top1000:
            style_col = _stockx_top1000_style_column(clean_frame)
        else:
            style_col = _find_column(list(clean_frame.columns), STYLE_COLUMN_CANDIDATES)
            if style_col is None:
                style_col = _detect_style_column_by_content(clean_frame)
        stockx_category = _stockx_category_from_name(file_name, sheet_name)

        for row_index, row in clean_frame.iterrows():
            if is_stockx_top1000:
                if style_col is None:
                    continue
                style_no = _clean_stockx_top1000_style(row.get(style_col))
            elif style_col is not None:
                style_no = _clean_style_from_named_column(row.get(style_col))
            else:
                style_no = _scan_row_for_style(row)
            if not style_no:
                continue

            rank = _clean_rank(row.get(rank_col)) if rank_col is not None else None
            if rank is None and is_stockx_top1000:
                rank = int(row_index) + 1
            title_hint = str(row.get(title_col)).strip() if title_col is not None and row.get(title_col) else None
            conn.execute(
                """
                INSERT INTO sku_items (
                    import_id, sheet_name, style_no, sku, rank, title_hint,
                    raw_row_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    import_id,
                    sheet_name,
                    style_no,
                    style_no,
                    rank,
                    title_hint,
                    json_dumps(row.to_dict()),
                    imported_at,
                ),
            )
            if is_stockx_top1000:
                reference_price, reference_column = _stockx_top1000_reference_price(row)
            else:
                reference_price, reference_column = _find_reference_price(row)
            if reference_price is not None:
                upsert_reference_price(
                    conn,
                    style_no=style_no,
                    size=None,
                    source_name=f"stockx_top1000_{stockx_category}" if is_stockx_top1000 and stockx_category else "import",
                    price=reference_price,
                    currency="USD",
                    note=f"{sheet_name}:{reference_column}" if reference_column else sheet_name,
                    raw_json=row.to_dict(),
                )
            if is_stockx_top1000:
                _upsert_import_product(conn, style_no, row)
            rows_imported += 1

    return ImportResult(import_id, rows_seen, rows_imported, sheet_names)


def list_imported_skus(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT style_no
        FROM sku_items
        WHERE style_no IS NOT NULL AND TRIM(style_no) != ''
        ORDER BY style_no
        """
    ).fetchall()
    return [str(row["style_no"]) for row in rows]
