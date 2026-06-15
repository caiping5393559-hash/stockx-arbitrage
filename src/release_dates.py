from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests


MONTH_RE = (
    r"January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)
NUMERIC_DATE_RE = re.compile(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b")
ISO_DATE_RE = re.compile(r"\b20\d{2}-\d{1,2}-\d{1,2}\b")
MONTH_DATE_RE = re.compile(
    rf"\b(?:{MONTH_RE})\.?\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,)?\s+20\d{{2}}\b",
    re.I,
)
TRUSTED_DOMAINS = (
    "goat.com",
    "soleretriever.com",
    "sneakerfiles.com",
    "nicekicks.com",
    "sneakernews.com",
    "hypebeast.com",
    "sneakerbardetroit.com",
    "kicksonfire.com",
    "justfreshkicks.com",
    "moresneakers.com",
    "thesolesupplier.co.uk",
    "whentocop.com",
    "sneakerjagers.com",
    "solesense.com",
)
DAY_FIRST_DOMAINS = (
    "moresneakers.com",
    "thesolesupplier.co.uk",
    "sneakerjagers.com",
)
GENERIC_TITLE_TOKENS = {
    "nike",
    "jordan",
    "air",
    "retro",
    "low",
    "high",
    "mid",
    "og",
    "sp",
    "gs",
    "td",
    "ps",
    "mens",
    "men",
    "womens",
    "women",
    "white",
    "black",
}


@dataclass
class ReleaseDateResult:
    style_no: str
    release_date: str
    source_name: str
    source_url: str
    confidence: float
    raw_text: str


def compact_style(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


def style_parts(style_no: str) -> list[str]:
    parts = [part.strip() for part in re.split(r"[/,;]", str(style_no)) if part.strip()]
    if str(style_no).strip() and str(style_no).strip() not in parts:
        parts.insert(0, str(style_no).strip())
    return parts or [str(style_no).strip()]


def slugify(value: str) -> str:
    text = re.sub(r"\([^)]*\)", " ", str(value).lower())
    text = text.replace("'", "").replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def remove_brand_from_title(title: str, brand: str | None) -> str:
    text = str(title or "").strip()
    if brand:
        text = re.sub(rf"^{re.escape(str(brand).strip())}\s+", "", text, flags=re.I)
    return text.strip() or str(title or "").strip()


def model_family_slug(title: str, brand: str | None) -> str | None:
    lower = str(title).lower()
    known = [
        ("gel-kayano", "gel-kayano"),
        ("gel-nyc", "gel-nyc"),
        ("gel-1130", "gel-1130"),
        ("dunk low", "dunk-low"),
        ("dunk high", "dunk-high"),
        ("air force 1", "air-force-1"),
        ("air jordan 1", "air-jordan-1"),
        ("jordan 1", "air-jordan-1"),
        ("jordan 3", "air-jordan-3"),
        ("jordan 4", "air-jordan-4"),
        ("jordan 5", "air-jordan-5"),
        ("jordan 6", "air-jordan-6"),
        ("jordan 8", "air-jordan-8"),
        ("jordan 11", "air-jordan-11"),
        ("jordan 12", "air-jordan-12"),
        ("jordan 13", "air-jordan-13"),
        ("jordan 14", "air-jordan-14"),
    ]
    for needle, family in known:
        if needle in lower:
            return family
    title_without_brand = remove_brand_from_title(title, brand)
    tokens = slugify(title_without_brand).split("-")
    if len(tokens) >= 2:
        return "-".join(tokens[:2])
    return tokens[0] if tokens else None


def reader_url(url: str) -> str:
    return "https://r.jina.ai/http://" + url


def source_name_for_url(url: str) -> tuple[str, float]:
    host = urlparse(url).netloc.lower()
    if "goat.com" in host:
        return "GOAT", 0.95
    if "soleretriever.com" in host:
        return "Sole Retriever", 0.9
    if "kicksonfire.com" in host:
        return "KicksOnFire", 0.85
    if "justfreshkicks.com" in host:
        return "JustFreshKicks", 0.82
    if "sneakerfiles.com" in host:
        return "Sneaker Files", 0.82
    if "nicekicks.com" in host:
        return "Nice Kicks", 0.78
    if "sneakernews.com" in host:
        return "Sneaker News", 0.78
    if "hypebeast.com" in host:
        return "Hypebeast", 0.72
    if "sneakerbardetroit.com" in host:
        return "Sneaker Bar Detroit", 0.75
    if "moresneakers.com" in host:
        return "More Sneakers", 0.8
    if "thesolesupplier.co.uk" in host:
        return "The Sole Supplier", 0.8
    if "whentocop.com" in host:
        return "When To Cop", 0.78
    if "sneakerjagers.com" in host:
        return "Sneakerjagers", 0.8
    if "solesense.com" in host:
        return "Solesense", 0.78
    return host or "web", 0.5


def is_trusted_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return any(domain in host for domain in TRUSTED_DOMAINS)


def fetch_text(url: str, *, timeout: int = 12) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    }
    response = requests.get(url, timeout=timeout, headers=headers)
    response.raise_for_status()
    return response.text


def fetch_page_text(url: str, *, timeout: int = 12) -> str:
    try:
        return fetch_text(reader_url(url), timeout=timeout)
    except Exception:
        return fetch_text(url, timeout=timeout)


def parse_date_value(value: str, *, prefer_day_first: bool = False) -> str | None:
    text = html.unescape(str(value)).strip()
    text = re.sub(r"(?<=\d)(st|nd|rd|th)\b", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text)
    candidates = []
    candidates.extend(ISO_DATE_RE.findall(text))
    candidates.extend(NUMERIC_DATE_RE.findall(text))
    candidates.extend(match.group(0) for match in MONTH_DATE_RE.finditer(text))
    numeric_formats = (
        ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y")
        if prefer_day_first
        else ("%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y")
    )
    for candidate in candidates:
        cleaned = candidate.replace(",", "").strip()
        for fmt in ("%Y-%m-%d", *numeric_formats, "%B %d %Y", "%b %d %Y"):
            try:
                parsed = datetime.strptime(cleaned, fmt)
                if parsed.year < 1990 or parsed.year > 2035:
                    continue
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def extract_release_date_from_text(text: str, *, source_url: str | None = None) -> str | None:
    if not text:
        return None
    normalized = html.unescape(text).replace("\r", "\n")
    prefer_day_first = False
    if source_url:
        host = urlparse(source_url).netloc.lower()
        prefer_day_first = any(domain in host for domain in DAY_FIRST_DOMAINS)
    lines = [line.strip() for line in normalized.splitlines() if line.strip()]
    for index, line in enumerate(lines):
        lower = line.lower()
        if "release date" not in lower:
            continue
        after = re.split(r"release date", line, flags=re.I, maxsplit=1)[-1]
        date = parse_date_value(after, prefer_day_first=prefer_day_first)
        if date:
            return date
        for lookahead in lines[index + 1 : index + 4]:
            date = parse_date_value(lookahead, prefer_day_first=prefer_day_first)
            if date:
                return date

    context_patterns = [
        rf"release date\s*(?:\||:|-)?\s*({NUMERIC_DATE_RE.pattern})",
        rf"release date\s*(?:\||:|-)?\s*({MONTH_DATE_RE.pattern})",
        rf"released?\s+(?:on\s+)?({MONTH_DATE_RE.pattern})",
        rf"released?\s+(?:on\s+)?({NUMERIC_DATE_RE.pattern})",
    ]
    for pattern in context_patterns:
        match = re.search(pattern, normalized, flags=re.I | re.S)
        if match:
            date = parse_date_value(match.group(1), prefer_day_first=prefer_day_first)
            if date:
                return date
    return None


def meaningful_title_tokens(title: str) -> set[str]:
    tokens = set(re.findall(r"[a-z0-9]{3,}", str(title).lower()))
    return {token for token in tokens if token not in GENERIC_TITLE_TOKENS}


def title_variants(title: str, brand: str | None) -> list[str]:
    raw_title = str(title or "").strip()
    if not raw_title:
        return []

    def _strip_generic_words(value: str) -> str:
        words = [word for word in re.findall(r"[A-Za-z0-9']+", value) if word]
        keep: list[str] = []
        for word in words:
            lower = word.lower()
            if lower in {"retro", "og", "sp", "qs", "prm", "wmns", "womens", "women", "mens", "men", "td", "ps"}:
                continue
            keep.append(word)
        return " ".join(keep)

    variants = [
        slugify(_strip_generic_words(raw_title)),
        slugify(_strip_generic_words(remove_brand_from_title(raw_title, brand))),
        slugify(raw_title),
        slugify(remove_brand_from_title(raw_title, brand)),
    ]
    clean: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        if variant and variant not in seen:
            seen.add(variant)
            clean.append(variant)
    return clean


def text_matches_product(text: str, url: str, style_no: str, title: str) -> bool:
    haystack = compact_style((text or "") + " " + (url or ""))
    style_ok = any(compact_style(part) and compact_style(part) in haystack for part in style_parts(style_no))
    if not style_ok:
        return False
    tokens = meaningful_title_tokens(title)
    if not tokens:
        return True
    text_lower = ((text or "") + " " + (url or "")).lower()
    overlap = sum(1 for token in tokens if token in text_lower)
    if "goat.com" in urlparse(url).netloc.lower() and overlap >= 1:
        return True
    return overlap >= min(2, len(tokens))


def goat_candidate_urls(style_no: str, title: str, brand: str | None) -> list[str]:
    urls: list[str] = []
    for part in style_parts(style_no):
        style_slug = slugify(part)
        title_options = [
            remove_brand_from_title(title, brand),
            title,
        ]
        for title_option in title_options:
            title_slug = slugify(title_option)
            if title_slug and style_slug:
                urls.append(f"https://www.goat.com/sneakers/{title_slug}-{style_slug}")
                urls.append(f"https://www.goat.com/apparel/{title_slug}-{style_slug}")
    return dedupe(urls)


def soleretriever_candidate_urls(style_no: str, title: str, brand: str | None) -> list[str]:
    family = model_family_slug(title, brand)
    if not family:
        return []
    brand_slug = slugify("jordan" if "jordan" in str(title).lower() else (brand or str(title).split()[0]))
    urls: list[str] = []
    for part in style_parts(style_no):
        page_slug = slugify(f"{brand or ''} {remove_brand_from_title(title, brand)} {part}")
        urls.append(f"https://www.soleretriever.com/sneaker-release-dates/{brand_slug}/{family}/{page_slug}")
        urls.append(f"https://www.soleretriever.com/releases/{page_slug}")
    return dedupe(urls)


def article_candidate_urls(title: str, brand: str | None) -> list[str]:
    urls: list[str] = []
    for slug in title_variants(title, brand):
        if not slug:
            continue
        urls.extend(
            [
                f"https://www.kicksonfire.com/{slug}/",
                f"https://www.justfreshkicks.com/{slug}/",
                f"https://www.sneakerfiles.com/{slug}/",
                f"https://www.sneakerfiles.com/{slug}-release-date-info/",
                f"https://www.sneakernews.com/{slug}/",
                f"https://www.nicekicks.com/{slug}/",
                f"https://www.hypebeast.com/{slug}/",
                f"https://www.sneakerbardetroit.com/{slug}/",
                f"https://www.sneakernews.com/{slug}-release-date/",
                f"https://www.moresneakers.com/releases/{slug}",
                f"https://www.thesolesupplier.co.uk/release-dates/{slug}/",
                f"https://www.whentocop.com/drops/{slug}/",
                f"https://www.sneakerjagers.com/en/s/{slug}/",
                f"https://www.solesense.com/en-us/{slug}",
            ]
        )
    return dedupe(urls)


def dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    clean: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            clean.append(value)
    return clean


def extract_search_links(html_text: str) -> list[str]:
    links: list[str] = []
    text = html.unescape(html_text or "")
    for match in re.finditer(r"uddg=([^&\"']+)", text):
        links.append(unquote(match.group(1)))
    for match in re.finditer(r"<a[^>]+href=[\"'](https?://[^\"']+)[\"']", text, flags=re.I):
        href = match.group(1)
        if "duckduckgo.com/l/?" in href and "uddg=" in href:
            parsed = parse_qs(urlparse(href).query).get("uddg", [])
            links.extend(parsed)
        elif is_trusted_url(href):
            links.append(href)
    return dedupe([link for link in links if is_trusted_url(link)])


def search_candidate_urls(style_no: str, title: str, *, timeout: int = 12, max_links: int = 12) -> list[str]:
    queries = [
        f'site:goat.com "{style_no}" "Release Date"',
        f'"{style_no}" "{title}" "Release Date"',
        f'"{style_no}" "Release Date"',
    ]
    links: list[str] = []
    for query in queries:
        url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
        try:
            links.extend(extract_search_links(fetch_text(url, timeout=timeout)))
        except Exception:
            continue
        if len(links) >= max_links:
            break
    return dedupe(links)[:max_links]


def try_url(style_no: str, title: str, url: str, *, timeout: int = 12) -> ReleaseDateResult | None:
    text = fetch_page_text(url, timeout=timeout)
    if not text_matches_product(text, url, style_no, title):
        return None
    release_date = extract_release_date_from_text(text, source_url=url)
    if not release_date:
        return None
    source_name, confidence = source_name_for_url(url)
    return ReleaseDateResult(
        style_no=style_no,
        release_date=release_date,
        source_name=source_name,
        source_url=url,
        confidence=confidence,
        raw_text=text[:20000],
    )


def lookup_release_date(
    *,
    style_no: str,
    title: str,
    brand: str | None = None,
    timeout: int = 12,
    candidate_limit: int | None = None,
    allow_search: bool = True,
) -> ReleaseDateResult | None:
    brand_text = f"{brand or ''} {title or ''}".lower()
    prefer_article_first = any(token in brand_text for token in ("nike", "jordan"))
    candidates = []
    if prefer_article_first:
        candidates.extend(article_candidate_urls(title, brand))
        candidates.extend(goat_candidate_urls(style_no, title, brand))
        candidates.extend(soleretriever_candidate_urls(style_no, title, brand))
    else:
        candidates.extend(goat_candidate_urls(style_no, title, brand))
        candidates.extend(soleretriever_candidate_urls(style_no, title, brand))
        candidates.extend(article_candidate_urls(title, brand))
    if allow_search:
        candidates.extend(search_candidate_urls(style_no, title, timeout=timeout))
    if candidate_limit is not None:
        candidates = candidates[:candidate_limit]

    errors: list[str] = []
    for url in dedupe(candidates):
        try:
            result = try_url(style_no, title, url, timeout=timeout)
        except Exception as exc:
            errors.append(f"{url}: {exc}")
            continue
        if result:
            return result
    return None
