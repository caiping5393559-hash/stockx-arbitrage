from __future__ import annotations

from datetime import datetime
import re
from collections.abc import Iterator
from typing import Any


SIZE_KEY_RE = re.compile(r"^(?:US\s*)?(?:(?:[WYCMS]\s*)?\d{1,2}(?:\.\d)?(?:[WYCMS])?|[XSML]{1,3})$", re.I)
STYLE_NO_RE = re.compile(r"\b(\d{6})[- ](\d{3})\b")
STYLE_TOKEN_RE = re.compile(r"\b[A-Z0-9]{2,8}[- ][A-Z0-9]{2,8}(?:[- ][A-Z0-9]{2,6})?\b", re.I)
APPAREL_SIZE_RE = re.compile(r"^(?:XS|S|M|L|XL|XXL|XXXL|XXXXL|2XL|3XL|4XL|5XL|6XL|7XL|OS|ONE SIZE(?: FITS ALL)?)$", re.I)
NUMERIC_SIZE_RE = re.compile(r"^(?:[WYCMS]\s*)?\d{1,2}(?:\.\d)?(?:[WYCMS])?$", re.I)
ISO_DATE_RE = re.compile(r"\b20\d{2}-\d{1,2}-\d{1,2}\b")
NUMERIC_DATE_RE = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b")
MONTH_DATE_RE = re.compile(
    r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s+20\d{2}\b",
    re.I,
)
RELEASE_HINT_RE = re.compile(r"\b(release|released|launch|drop)\b", re.I)


def first_value(data: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
    return default


def deep_first_value(data: Any, keys: list[str], default: Any = None) -> Any:
    if isinstance(data, dict):
        for key in keys:
            if key in data and data[key] not in (None, ""):
                return data[key]
        for value in data.values():
            if isinstance(value, (dict, list)):
                found = deep_first_value(value, keys, default=None)
                if found not in (None, ""):
                    return found
    elif isinstance(data, list):
        for item in data:
            found = deep_first_value(item, keys, default=None)
            if found not in (None, ""):
                return found
    return default


def extract_release_date(payload: Any) -> str | None:
    payload = unwrap_payload(payload)

    def _parse_date_text(text: str) -> str | None:
        cleaned = re.sub(r"(?<=\d)(st|nd|rd|th)\b", "", str(text).strip(), flags=re.I)
        cleaned = re.sub(r"\s+", " ", cleaned)
        if not cleaned or cleaned in {"0", "0.0", "null", "none"}:
            return None

        if re.fullmatch(r"\d{10}", cleaned):
            timestamp = int(cleaned)
            if 946684800 <= timestamp <= 4102444800:
                return datetime.utcfromtimestamp(timestamp).date().isoformat()

        if re.fullmatch(r"20\d{2}-\d{1,2}-\d{1,2}(?:[ T].*)?", cleaned):
            try:
                return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).date().isoformat()
            except ValueError:
                pass

        if ISO_DATE_RE.search(cleaned):
            try:
                return datetime.strptime(ISO_DATE_RE.search(cleaned).group(0), "%Y-%m-%d").date().isoformat()
            except ValueError:
                pass

        for match in NUMERIC_DATE_RE.finditer(cleaned):
            candidate = match.group(0)
            for fmt in (
                "%m/%d/%Y",
                "%m-%d-%Y",
                "%m/%d/%y",
                "%m-%d-%y",
                "%d/%m/%Y",
                "%d-%m-%Y",
                "%d/%m/%y",
                "%d-%m-%y",
            ):
                try:
                    parsed = datetime.strptime(candidate, fmt)
                    if 1990 <= parsed.year <= 2035:
                        return parsed.date().isoformat()
                except ValueError:
                    continue

        for match in MONTH_DATE_RE.finditer(cleaned):
            candidate = re.sub(r"(?<=\d)(st|nd|rd|th)\b", "", match.group(0), flags=re.I).replace(",", "")
            for fmt in ("%B %d %Y", "%b %d %Y"):
                try:
                    parsed = datetime.strptime(candidate, fmt)
                    if 1990 <= parsed.year <= 2035:
                        return parsed.date().isoformat()
                except ValueError:
                    continue

        if RELEASE_HINT_RE.search(cleaned):
            try:
                from .release_dates import extract_release_date_from_text

                parsed = extract_release_date_from_text(cleaned)
                if parsed:
                    return parsed
            except Exception:
                pass

        return None

    def _walk(value: Any) -> str | None:
        if value in (None, "", [], {}):
            return None
        if isinstance(value, bool):
            return None

        candidate = _parse_date_text(value) if isinstance(value, (str, int, float)) else None
        if candidate:
            return candidate

        if isinstance(value, dict):
            direct_keys = (
                "releaseDate",
                "release_date",
                "releaseAt",
                "releasedAt",
                "releaseTimestamp",
                "releaseTime",
            )
            for key in direct_keys:
                if key in value:
                    candidate = _parse_date_text(value.get(key))
                    if candidate:
                        return candidate

            traits = value.get("traits")
            if isinstance(traits, list):
                for trait in traits:
                    if not isinstance(trait, dict):
                        continue
                    trait_name = str(trait.get("name") or trait.get("label") or "").strip().lower()
                    if "release date" in trait_name or trait_name == "release":
                        candidate = _parse_date_text(trait.get("value"))
                        if candidate:
                            return candidate

            for key in ("description", "shortDescription", "all_text_bool", "title", "name", "model", "colorway"):
                text_value = value.get(key)
                if isinstance(text_value, str):
                    candidate = _parse_date_text(text_value)
                    if candidate:
                        return candidate

            for child in value.values():
                if isinstance(child, (dict, list)):
                    candidate = _walk(child)
                    if candidate:
                        return candidate

        elif isinstance(value, list):
            for item in value:
                candidate = _walk(item)
                if candidate:
                    return candidate

        return None

    return _walk(payload)


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace("$", "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def to_int(value: Any, default: int = 1) -> int:
    if value is None or value == "":
        return default
    try:
        return max(0, int(float(str(value).replace(",", "").strip())))
    except ValueError:
        return default


def looks_like_size(value: Any) -> bool:
    if value is None:
        return False
    return bool(SIZE_KEY_RE.match(str(value).strip()))


def normalize_style_no(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None

    nike_match = STYLE_NO_RE.search(text)
    if nike_match:
        return f"{nike_match.group(1)}-{nike_match.group(2)}"

    compact = text.replace(" ", "").replace("-", "")
    if compact.isdigit() and len(compact) == 9:
        return f"{compact[:6]}-{compact[6:]}"

    style_match = STYLE_TOKEN_RE.search(text)
    if style_match:
        candidate = style_match.group(0).replace(" ", "-")
        if any(ch.isdigit() for ch in candidate):
            return candidate

    return text


def unwrap_payload(payload: Any) -> Any:
    current = payload
    changed = True
    while changed:
        changed = False
        for key in ("data", "result", "payload"):
            if isinstance(current, dict) and key in current and current[key] not in (None, ""):
                current = current[key]
                changed = True
                break
    return current


def iter_records(payload: Any) -> Iterator[Any]:
    payload = unwrap_payload(payload)
    if isinstance(payload, list):
        for item in payload:
            yield item
        return

    if not isinstance(payload, dict):
        return

    preferred_keys = (
        "items",
        "results",
        "records",
        "rows",
        "list",
        "activity",
        "activities",
        "asks",
        "bids",
        "variants",
        "children",
        "sizes",
        "market",
    )
    for key in preferred_keys:
        value = payload.get(key)
        if isinstance(value, list):
            for item in value:
                yield item
            return
        if isinstance(value, dict):
            for item in iter_records(value):
                yield item
            return

    yield payload


def extract_product(payload: Any) -> dict[str, Any]:
    payload = unwrap_payload(payload)
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
        return {}
    if isinstance(payload, dict):
        product = first_value(payload, ["product", "productDetail", "product_detail", "details"], payload)
        if isinstance(product, dict):
            return product
    return {}


def extract_product_uuid(payload: Any) -> str | None:
    payload = unwrap_payload(payload)
    if not isinstance(payload, dict):
        return None
    value = first_value(payload, ["stockx_uuid", "stockxUuid", "product_uuid", "productUuid", "uuid", "id"])
    return str(value) if value not in (None, "") else None


def _normalize_us_size_label(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    upper = text.upper()
    if upper.startswith("US "):
        text = text[3:].strip()
    elif upper.startswith("US-"):
        text = text[3:].strip()
    elif upper.startswith("US"):
        text = text[2:].strip()
    text = text.replace("M ", "").replace("W ", "").strip()
    return text or None


def _normalize_product_size_label(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    upper = text.upper()
    if re.fullmatch(r"[0-9A-F]{8}(?:-[0-9A-F]{4}){3}-[0-9A-F]{12}", upper):
        return None

    normalized = _normalize_us_size_label(text) or text
    normalized = str(normalized).strip()
    if not normalized:
        return None
    upper = normalized.upper()
    if NUMERIC_SIZE_RE.fullmatch(normalized):
        return normalized
    if APPAREL_SIZE_RE.fullmatch(upper):
        return upper
    return None


def extract_size_variants(payload: Any) -> list[dict[str, Any]]:
    payload = unwrap_payload(payload)
    if not isinstance(payload, dict):
        return []
    product = first_value(payload, ["product_size_info", "productSizeInfo", "product"], payload)
    variants = first_value(product, ["variants"], [])
    if not isinstance(variants, list):
        return []
    result: list[dict[str, Any]] = []
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        size = first_value(variant, ["size", "shoeSize", "displaySize", "variantSize"])
        traits = variant.get("traits") if isinstance(variant.get("traits"), dict) else {}
        if not size and isinstance(traits, dict):
            size = first_value(traits, ["size", "shoeSize", "displaySize", "variantSize"])
        if not size:
            size_chart = variant.get("sizeChart") if isinstance(variant.get("sizeChart"), dict) else {}
            display_options = size_chart.get("displayOptions") if isinstance(size_chart, dict) else None
            if isinstance(display_options, list):
                for option in display_options:
                    if not isinstance(option, dict):
                        continue
                    candidate = _normalize_us_size_label(option.get("size"))
                    if candidate:
                        size = candidate
                        break
        size_text = str(size).strip() if size not in (None, "") else ""
        if not size_text:
            continue
        result.append(
            {
                "product_size_uuid": first_value(variant, ["id", "uuid", "productSizeUuid", "product_size_uuid"]),
                "size": size_text,
                "raw": variant,
            }
        )
    return result


def extract_sizes_from_product(product: dict[str, Any]) -> list[str]:
    sizes: set[str] = set()
    size_keys = (
        "allSizes",
        "sizes",
        "shoeSizes",
        "CMShoeSizes",
        "EUShoeSizes",
        "UKShoeSizes",
        "KRShoeSizes",
        "USMenShoeSizes",
        "USWomenShoeSizes",
        "USKidShoeSizes",
        "USMenApparelSizes",
        "USWomenApparelSizes",
        "USKidApparelSizes",
        "children",
        "variants",
    )

    def add_candidate(value: Any) -> None:
        candidate = _normalize_product_size_label(value)
        if candidate:
            sizes.add(candidate)

    def walk(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                walk(item)
            return
        if isinstance(value, dict):
            direct = first_value(value, ["size", "shoeSize", "displaySize", "variantSize"])
            if direct:
                add_candidate(direct)
            display_options = value.get("displayOptions")
            if isinstance(display_options, list):
                for option in display_options:
                    if isinstance(option, dict):
                        add_candidate(option.get("size"))
                    else:
                        add_candidate(option)
            for key, child in value.items():
                lower = key.lower()
                if "uuid" in lower or lower.endswith("id"):
                    continue
                if "size" in lower or lower == "displayoptions":
                    walk(child)
            return
        add_candidate(value)

    for key in size_keys:
        value = product.get(key)
        if value not in (None, ""):
            walk(value)
    return sorted(sizes, key=_size_sort_key)


def _size_sort_key(size: str) -> tuple[int, float | str]:
    text = str(size).upper().replace("US", "").strip()
    try:
        return (0, float(text))
    except ValueError:
        return (1, text)


def extract_next_cursor(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    candidates = [
        payload.get("nextCursor"),
        payload.get("next_cursor"),
        payload.get("next"),
    ]
    page_info = payload.get("pageInfo")
    if isinstance(page_info, dict):
        candidates.extend([page_info.get("nextCursor"), page_info.get("endCursor")])
    pagination = payload.get("pagination")
    if isinstance(pagination, dict):
        candidates.extend([pagination.get("nextCursor"), pagination.get("next_cursor")])
    for candidate in candidates:
        if candidate:
            return str(candidate)
    return None


def normalize_service_level(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    known = {"standard", "expressStandard", "expressExpedited"}
    return text if text in known else text or None
