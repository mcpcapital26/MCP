# acquisitions_alerts.py
# Scrapes 6 acquisition-listing sites and sends ONLY new listings via Telegram (state file per site).
#
# Deps:
#   pip install requests beautifulsoup4 lxml
#
# Env:
#   TELEGRAM_BOT_TOKEN
#   TELEGRAM_CHAT_ID
#
# State file (committed back to repo by workflow):
#   seen_announcements_by_site.json
#
# Notes:
# - Filters apply to ALL sites (accent-insensitive).
# - SPA sites:
#   * WE Transmission: discovers backend/api endpoints from JS bundle, caches working endpoint in state["_meta"].
#   * Bedrijventekoop: tries POST listings endpoint; if it can’t, falls back to probing IDs (no-crash).
# - Filtered/ignored items are still marked as "seen" so they don’t re-trigger every run.

import os
import re
import json
import time
import hashlib
import unicodedata
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Callable, Any
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
# CONFIG
# =============================================================================
USER_AGENT = "Mozilla/5.0 (AcquisitionAnnouncementsBot; +https://github.com/)"
STATE_FILE = "seen_announcements_by_site.json"

REQUEST_TIMEOUT = 35
SLEEP_BETWEEN_PAGES_SEC = 1.0
MAX_NEW_PER_RUN = 40  # cap to avoid spam/long runs

# Any announcement whose title/location/teaser/type/sector contains ANY of these words
# (case-insensitive, substring match, accent-insensitive) will be discarded.
FORBIDDEN_WORDS = [
    "horeca",
    "restaurant",
    "bar",
    "traiteur",
    "alimentaire",
]

# URLS (your 6 sources)
URL1_COFIM = "https://www.cofim.be/fr/entreprises/entreprises-fonds-de-commerce"
URL2_CAR = "https://www.commerce-a-remettre.be/recherche?region=&sector=&id="
URL3_WE = "https://transmission.wallonie-entreprendre.be/"
URL4_OVERNAMEMARKT = "https://www.overnamemarkt.be/fr/acheter?sectors=bouw,diensten,industrie,distributie,vrije-beroepen,andere"
URL5_BEDRIJVENTEKOOP = "https://www.bedrijventekoop.be/te-koop-aangeboden?sectors=2,4,12,2_216,2_217,2_218,2_83,2_222,2_219,2_86,2_94,2_93,2_91,2_220,2_221,2_87,2_92,2_233,2_89,2_85,2_223,2_99,2_224,2_225,2_90,2_84,4_12,4_13,4_15,4_14,4_101,8_51,8_62,8_53,8_59,8_64,8_230,8_242,8_54,8_204,8_67,8_229,8_60,8_215,8_66,8_105,8_52,8_49,8_56,8_234,8_88,8_241,8_65,8_228,8_55,12_208,12_61,12_206,12_108,12_207,12_205&regions=26,27,28,29,30,31,32,33,34,35,36,37,38,39"
URL6_CESSIONPRO = "https://www.cessionpro.be/?secteurs-v4oj=entreprise-de-construction-a-remettre-en-belgique%7Ce-commerce-a-vendre-en-belgique%7Csociete-industrielle-a-vendre-en-belgique%7Csociete-de-service-a-vendre-en-belgique"

# Per-site paging limits
COFIM_MAX_PAGES = 3
CAR_MAX_PAGES = 3
OVERNAMEMARKT_MAX_PAGES = 3
CESSIONPRO_MAX_PAGES = 1

# CessionPro: keep empty to allow all sectors. If non-empty, only these exact sector labels are kept.
CESSIONPRO_ALLOWED_SECTORS = set()

# =============================================================================
# STATE META (for caching endpoints / last ids)
# =============================================================================
META_KEY = "_meta"
STATE_DIRTY = False  # set True whenever we update state meta (endpoint cache, last_id, etc.)


def meta_get(state: Dict[str, Any], site: str, key: str, default=None):
    return (state.get(META_KEY, {}) or {}).get(site, {}).get(key, default)


def meta_set(state: Dict[str, Any], site: str, key: str, value):
    global STATE_DIRTY
    state.setdefault(META_KEY, {})
    state[META_KEY].setdefault(site, {})
    if state[META_KEY][site].get(key) != value:
        state[META_KEY][site][key] = value
        STATE_DIRTY = True


# =============================================================================
# TYPES
# =============================================================================
@dataclass
class Announcement:
    site: str
    title: str
    url: str
    meta: Dict[str, str]

    @property
    def key(self) -> str:
        """
        Stable key for dedup/state:
        - Prefer numeric ID segment if present
        - Else last path segment
        - Else sha256(url+title)
        """
        try:
            p = urlparse(self.url)
            segments = [s for s in p.path.split("/") if s]
            for seg in segments:
                if seg.isdigit():
                    return f"{self.site}:{seg}"
            if segments:
                return f"{self.site}:{segments[-1]}"
        except Exception:
            pass
        raw = (self.url or "") + "||" + (self.title or "")
        return f"{self.site}:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


FetchFn = Callable[[requests.Session, int], List[Announcement]]
FormatFn = Callable[[Announcement], Optional[str]]


# =============================================================================
# HTTP HELPERS
# =============================================================================
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def http_get(session: requests.Session, url: str, extra_headers: Optional[Dict[str, str]] = None) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "fr,en;q=0.8,nl;q=0.6",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }
    if extra_headers:
        headers.update(extra_headers)
    r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text


def http_post_form(
    session: requests.Session,
    url: str,
    data: Dict[str, str],
    referer: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "fr,en;q=0.8,nl;q=0.6",
        "Accept": "text/html,*/*;q=0.8",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    if referer:
        headers["Referer"] = referer
    if extra_headers:
        headers.update(extra_headers)
    r = session.post(url, data=data, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text


def http_post_json(
    session: requests.Session,
    url: str,
    body: Dict[str, Any],
    referer: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "fr,en;q=0.8,nl;q=0.6",
        "Accept": "application/json,text/plain,*/*",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    if referer:
        headers["Referer"] = referer
    if extra_headers:
        headers.update(extra_headers)
    r = session.post(url, json=body, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text


def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def text_clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def absolute_url(base: str, href: str) -> str:
    return urljoin(base, href) if href else ""


def normalize_url(u: str) -> str:
    try:
        p = urlparse(u)
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
        new_query = urlencode(q, doseq=True)
        p2 = p._replace(query=new_query, fragment="")
        return urlunparse(p2)
    except Exception:
        return u


def norm_cmp(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().strip()


# =============================================================================
# STATE I/O (preserves _meta dict)
# =============================================================================
def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {}
            out: Dict[str, Any] = {}
            for k, v in data.items():
                if isinstance(v, list) or isinstance(v, dict):
                    out[k] = v
            return out
    except Exception:
        return {}


def save_state(state: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# =============================================================================
# TELEGRAM
# =============================================================================
def send_telegram(session: requests.Session, bot_token: str, chat_id: str, message: str) -> None:
    if not bot_token or not chat_id:
        print("Telegram not configured; skipping send.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "disable_web_page_preview": True}

    # Small manual handling for 429 retry_after (GitHub runners can hit this occasionally)
    for attempt in range(3):
        rr = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        if rr.ok:
            return
        try:
            j = rr.json()
            retry_after = int(j.get("parameters", {}).get("retry_after", 0))
        except Exception:
            retry_after = 0
        if rr.status_code == 429 and retry_after > 0:
            time.sleep(min(60, retry_after + 1))
            continue
        print("Telegram send failed:", rr.text)
        return


# =============================================================================
# FILTERS (apply to ALL sites, accent-insensitive)
# =============================================================================
def forbidden_hit(ann: Announcement) -> str:
    words = [norm_cmp(w) for w in FORBIDDEN_WORDS if w and w.strip()]
    if not words:
        return ""

    hay = " ".join(
        [
            ann.title or "",
            ann.meta.get("location", ""),
            ann.meta.get("price", ""),
            ann.meta.get("teaser", ""),
            ann.meta.get("type", ""),
            ann.meta.get("sector", ""),
            ann.meta.get("branche", ""),
            ann.meta.get("region", ""),
            ann.meta.get("description", ""),
            ann.meta.get("turnover", ""),
            ann.meta.get("omzet", ""),
        ]
    )
    hay_n = norm_cmp(hay)

    for w in words:
        if w and w in hay_n:
            return w
    return ""


# =============================================================================
# GENERIC RUNNER (HTML/list pages)
# =============================================================================
def run_site(
    session: requests.Session,
    site_name: str,
    fetch_fn: FetchFn,
    format_fn: FormatFn,
    max_pages: int,
    state: Dict[str, Any],
    bot_token: str,
    chat_id: str,
) -> Tuple[int, int, bool]:
    """
    Returns: (new_detected, alerts_sent, changed_state)
    Behavior:
      - Items matching FORBIDDEN_WORDS are marked as seen (so they don't resurface).
      - Items returning None/"" from formatter are marked as seen.
      - MAX_NEW_PER_RUN only limits sent alerts; remaining items stay "new" for next run.
    """
    seen = set(state.get(site_name, [])) if isinstance(state.get(site_name, []), list) else set()
    changed = False

    all_items: List[Announcement] = []
    for page in range(1, max_pages + 1):
        try:
            items = fetch_fn(session, page)
            print(f"[{site_name}] page {page}: {len(items)} items (after parsing)")
            all_items.extend(items)
        except Exception as e:
            print(f"[{site_name}] fetch/parse error page={page}: {e}")
        time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    # Dedup by key
    uniq_by_key: Dict[str, Announcement] = {}
    for it in all_items:
        uniq_by_key[it.key] = it
    items = list(uniq_by_key.values())

    new_items = [a for a in items if a.key not in seen]
    if not new_items:
        print(f"[{site_name}] no new announcements")
        return (0, 0, False)

    new_items.sort(key=lambda x: x.url)

    sent = 0
    for ann in new_items:
        if forbidden_hit(ann):
            seen.add(ann.key)
            changed = True
            continue

        if sent >= MAX_NEW_PER_RUN:
            break

        msg: Optional[str] = None
        try:
            msg = format_fn(ann)
        except Exception as e:
            print(f"[{site_name}] format error: {e}")

        if not msg:
            seen.add(ann.key)
            changed = True
            continue

        try:
            send_telegram(session, bot_token, chat_id, msg)
            sent += 1
            seen.add(ann.key)
            changed = True
        except Exception as e:
            print(f"[{site_name}] telegram send error: {e}")

        time.sleep(0.5)

    state[site_name] = list(seen)
    return (len(new_items), sent, changed)


# =============================================================================
# DIRECT RUNNER (API/SPA/fallback sites that need state meta)
# =============================================================================
def run_site_direct(
    session: requests.Session,
    site_name: str,
    fetch_items_fn: Callable[[requests.Session, Dict[str, Any]], List[Announcement]],
    format_fn: FormatFn,
    state: Dict[str, Any],
    bot_token: str,
    chat_id: str,
) -> Tuple[int, int, bool]:
    seen = set(state.get(site_name, [])) if isinstance(state.get(site_name, []), list) else set()
    changed = False

    try:
        items = fetch_items_fn(session, state)
        print(f"[{site_name}] fetched {len(items)} items (direct)")
    except Exception as e:
        print(f"[{site_name}] fetch error (direct): {e}")
        return (0, 0, False)

    # Dedup by key
    uniq_by_key: Dict[str, Announcement] = {}
    for it in items:
        uniq_by_key[it.key] = it
    items = list(uniq_by_key.values())

    new_items = [a for a in items if a.key not in seen]
    if not new_items:
        print(f"[{site_name}] no new announcements")
        return (0, 0, False)

    new_items.sort(key=lambda x: x.url)

    sent = 0
    for ann in new_items:
        if forbidden_hit(ann):
            seen.add(ann.key)
            changed = True
            continue

        if sent >= MAX_NEW_PER_RUN:
            break

        msg: Optional[str] = None
        try:
            msg = format_fn(ann)
        except Exception as e:
            print(f"[{site_name}] format error: {e}")

        if not msg:
            seen.add(ann.key)
            changed = True
            continue

        try:
            send_telegram(session, bot_token, chat_id, msg)
            sent += 1
            seen.add(ann.key)
            changed = True
        except Exception as e:
            print(f"[{site_name}] telegram send error: {e}")

        time.sleep(0.5)

    state[site_name] = list(seen)
    return (len(new_items), sent, changed)


# =============================================================================
# SITE 1 — COFIM
# =============================================================================
COFIM_SITE = "cofim"
COFIM_BASE = "https://www.cofim.be"


def build_cofim_page_url(page: int) -> str:
    if page <= 1:
        return URL1_COFIM
    p = urlparse(URL1_COFIM)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q["page"] = str(page)
    return urlunparse(p._replace(query=urlencode(q, doseq=True), fragment=""))


def parse_cofim_listing(html: str) -> List[Announcement]:
    soup = soupify(html)
    listing = soup.select_one("#biens-listing")
    if not listing:
        return []

    out: List[Announcement] = []
    for box in listing.select(".biens-box"):
        a = box.select_one("a.propertylink[href]")
        if not a:
            continue

        url = normalize_url(absolute_url(COFIM_BASE, a.get("href", "")))
        if not url:
            continue

        # sold flag
        flag = box.select_one("img.flag")
        flag_src = (flag.get("src", "") if flag else "").lower()
        if "banner-sold" in flag_src:
            continue

        title = ""
        location = ""
        h3 = box.select_one(".biens-title h3")
        if h3:
            raw = h3.get_text("\n", strip=True)
            parts = [p.strip() for p in raw.split("\n") if p.strip()]
            if parts:
                title = parts[0]
            if len(parts) > 1:
                location = parts[1]

        title = text_clean(title)
        location = text_clean(location)
        if not title or len(title) < 3:
            continue

        price = ""
        price_el = box.select_one(".biens-prix span")
        if price_el:
            price = text_clean(price_el.get_text(" "))

        teaser = ""
        teaser_el = box.select_one(".uk-overlay-primary p")
        if teaser_el:
            teaser = text_clean(teaser_el.get_text(" "))

        meta: Dict[str, str] = {}
        if location:
            meta["location"] = location
        if price:
            meta["price"] = price
        if teaser:
            meta["teaser"] = teaser

        out.append(Announcement(COFIM_SITE, title, url, meta))

    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def fetch_cofim(session: requests.Session, page: int) -> List[Announcement]:
    url = build_cofim_page_url(page)
    html = http_get(
        session,
        url,
        extra_headers={"Upgrade-Insecure-Requests": "1", "Referer": COFIM_BASE},
    )
    return parse_cofim_listing(html)


def format_cofim(ann: Announcement) -> str:
    lines = [f"[COFIM] {ann.title}"]
    if ann.meta.get("location"):
        lines.append(f"Localisation: {ann.meta['location']}")
    if ann.meta.get("price"):
        lines.append(f"Prix: {ann.meta['price']}")
    if ann.meta.get("teaser"):
        t = ann.meta["teaser"]
        if len(t) > 280:
            t = t[:280] + "…"
        lines.append(f"Résumé: {t}")
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# SITE 2 — Commerce-a-remettre
# =============================================================================
CAR_SITE = "commerce-a-remettre"
CAR_BASE = "https://www.commerce-a-remettre.be"


def build_car_page_url(page: int) -> str:
    # 0-based paging
    p0 = max(0, page - 1)
    p = urlparse(URL2_CAR)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q["page"] = str(p0)
    return urlunparse(p._replace(query=urlencode(q, doseq=True), fragment=""))


def parse_car_listing(html: str) -> List[Announcement]:
    soup = soupify(html)
    out: List[Announcement] = []

    for item in soup.select(".search-result-item"):
        a = item.select_one("h3.result-title a[href]")
        href = a.get("href", "") if a else ""
        if not href:
            meta_url = item.select_one("meta[itemprop='url']")
            href = meta_url.get("content", "") if meta_url else ""

        url = normalize_url(absolute_url(CAR_BASE, href))
        if not url:
            continue

        title = text_clean(a.get_text(" ")) if a else ""
        if not title:
            meta_name = item.select_one("meta[itemprop='name']")
            title = text_clean(meta_name.get("content", "")) if meta_name else ""

        if not title or len(title) < 3:
            continue

        price = ""
        meta_price = item.select_one("meta[itemprop='price']")
        if meta_price and meta_price.get("content"):
            price = text_clean(meta_price.get("content", ""))

        location = ""
        typ = ""
        for li in item.select("ul.result-info li"):
            txt = text_clean(li.get_text(" "))
            if "icon-map-marker" in li.decode().lower():
                loc_a = li.select_one("a")
                location = text_clean(loc_a.get_text(" ")) if loc_a else location
            if "type" in txt.lower():
                strong = li.select_one("strong")
                typ = text_clean(strong.get_text(" ")) if strong else typ
                if not typ:
                    m = re.search(r"type\s*:\s*(.+)$", txt, flags=re.I)
                    if m:
                        typ = text_clean(m.group(1))

        teaser = ""
        desc = item.select_one(".result-description p")
        if desc:
            teaser = text_clean(desc.get_text(" "))

        meta: Dict[str, str] = {}
        if location:
            meta["location"] = location
        if typ:
            meta["type"] = typ
        if price:
            meta["price"] = price
        if teaser:
            meta["teaser"] = teaser

        out.append(Announcement(CAR_SITE, title, url, meta))

    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def fetch_car(session: requests.Session, page: int) -> List[Announcement]:
    url = build_car_page_url(page)
    html = http_get(session, url, extra_headers={"Referer": CAR_BASE})
    return parse_car_listing(html)


def format_car(ann: Announcement) -> str:
    lines = [f"[Commerce-à-remettre] {ann.title}"]
    if ann.meta.get("location"):
        lines.append(f"Région: {ann.meta['location']}")
    if ann.meta.get("type"):
        lines.append(f"Type: {ann.meta['type']}")
    if ann.meta.get("price"):
        lines.append(f"Prix: {ann.meta['price']}")
    if ann.meta.get("teaser"):
        t = ann.meta["teaser"]
        if len(t) > 280:
            t = t[:280] + "…"
        lines.append(f"Résumé: {t}")
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# SITE 4 — overnamemarkt.be
# =============================================================================
OV_SITE = "overnamemarkt"
OV_BASE = "https://www.overnamemarkt.be"


def build_overnamemarkt_page_url(page: int) -> str:
    p = urlparse(URL4_OVERNAMEMARKT)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q["page"] = str(page)
    return urlunparse(p._replace(query=urlencode(q, doseq=True), fragment=""))


def _find_value_by_label_in_anchor(a_tag: BeautifulSoup, label: str) -> str:
    target = norm_cmp(label)
    for sp in a_tag.find_all("span"):
        if norm_cmp(sp.get_text(" ", strip=True)) == target:
            parent = sp.parent
            if parent:
                spans = parent.find_all("span")
                if spans:
                    val = spans[-1].get_text(" ", strip=True)
                    if norm_cmp(val) != target:
                        return text_clean(val)
    return ""


def parse_overnamemarkt_listing(html: str) -> List[Announcement]:
    soup = soupify(html)
    out: List[Announcement] = []

    for a in soup.select('a[href*="/fr/kopen/"]'):
        href = a.get("href", "")
        if not href or "/fr/kopen/" not in href:
            continue

        url = normalize_url(absolute_url(OV_BASE, href))

        title_el = a.select_one('span[class*="text-h4"]')
        title = text_clean(title_el.get_text(" ", strip=True)) if title_el else ""
        if not title:
            sp = a.find("span")
            title = text_clean(sp.get_text(" ", strip=True)) if sp else ""
        if not title:
            title = "(sans titre)"

        location = ""
        icon = a.select_one('i[class*="fa-map-marker"]')
        if icon and icon.parent:
            spans = icon.parent.find_all("span")
            if spans:
                location = text_clean(spans[-1].get_text(" ", strip=True))

        price = _find_value_by_label_in_anchor(a, "Prix")
        turnover = _find_value_by_label_in_anchor(a, "Chiffre d'affaires") or _find_value_by_label_in_anchor(a, "Chiffre d’affaires")

        sector = ""
        for sp in a.find_all("span"):
            cls = " ".join(sp.get("class", [])).lower()
            if "inline-flex" in cls and sp.find("i"):
                sector = text_clean(sp.get_text(" ", strip=True))
                break

        meta: Dict[str, str] = {}
        if location:
            meta["location"] = location
        if price:
            meta["price"] = price
        if turnover:
            meta["turnover"] = turnover
        if sector:
            meta["sector"] = sector

        out.append(Announcement(OV_SITE, title, url, meta))

    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def fetch_overnamemarkt(session: requests.Session, page: int) -> List[Announcement]:
    url = build_overnamemarkt_page_url(page)
    html = http_get(session, url, extra_headers={"Referer": OV_BASE})
    return parse_overnamemarkt_listing(html)


def format_overnamemarkt(ann: Announcement) -> str:
    lines = [f"[Overnamemarkt] {ann.title}"]
    if ann.meta.get("location"):
        lines.append(f"Localisation: {ann.meta['location']}")
    if ann.meta.get("sector"):
        lines.append(f"Secteur: {ann.meta['sector']}")
    if ann.meta.get("price"):
        lines.append(f"Prix: {ann.meta['price']}")
    if ann.meta.get("turnover"):
        lines.append(f"CA: {ann.meta['turnover']}")
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# SITE 6 — cessionpro.be
# =============================================================================
C6_SITE = "cessionpro"
C6_BASE = "https://www.cessionpro.be"


def is_new_c6_item(item: BeautifulSoup) -> bool:
    for el in item.select(".text-block-162, .badge, .label"):
        if norm_cmp(el.get_text(" ", strip=True)) == "nouveau":
            return True
    return "nouveau" in norm_cmp(item.get_text(" ", strip=True))


def parse_c6_item(item: BeautifulSoup) -> Optional[Announcement]:
    a = item.select_one('a[href^="/annonces/"]')
    href = a.get("href", "").strip() if a else ""
    if not href:
        return None
    url = normalize_url(absolute_url(C6_BASE, href))

    def pick(sel: str) -> str:
        el = item.select_one(sel)
        return text_clean(el.get_text(" ", strip=True)) if el else ""

    title = pick(".jobcard1 .poste-3 .text-block-119") or pick(".poste-3 .text-block-119") or "(sans titre)"
    location = pick(".jobcard1 .localisation .text-block-117") or pick(".localisation .text-block-117")
    location = re.sub(r"^📍\s*", "", location).strip()

    blocks = [text_clean(x.get_text(" ", strip=True)) for x in item.select(".jobcard1 .date-4 .text-block-116")]
    price = blocks[0] if blocks else ""
    turnover = blocks[1] if len(blocks) >= 2 else ""

    sector = pick(".jobcard1 .div-block-196 .text-block-145") or pick(".div-block-196 .text-block-145")

    reference = pick(".secondcard .text-block-163")
    description = ""
    rich = item.select_one(".secondcard .rich-text-block-annonce")
    if rich:
        description = text_clean(rich.get_text(" ", strip=True))

    email = ""
    phone = ""
    mail = item.select_one('.secondcard a[href^="mailto:"]')
    tel = item.select_one('.secondcard a[href^="tel:"]')
    if mail and mail.get("href"):
        email = mail["href"].replace("mailto:", "").strip()
    if tel and tel.get("href"):
        phone = tel["href"].replace("tel:", "").strip()

    meta: Dict[str, str] = {}
    if location:
        meta["location"] = location
    if sector:
        meta["sector"] = sector
    if price:
        meta["price"] = price
    if turnover:
        meta["turnover"] = turnover
    if reference:
        meta["reference"] = reference
    if description:
        meta["description"] = description
    if email:
        meta["email"] = email
    if phone:
        meta["phone"] = phone

    if CESSIONPRO_ALLOWED_SECTORS and sector and sector not in CESSIONPRO_ALLOWED_SECTORS:
        return None

    return Announcement(C6_SITE, title, url, meta)


def parse_cessionpro_listing(html: str, only_new: bool = True) -> List[Announcement]:
    soup = soupify(html)
    items = soup.select('div[role="listitem"].collectionitem.w-dyn-item') or soup.select("div.w-dyn-item")

    out: List[Announcement] = []
    for el in items:
        if only_new and not is_new_c6_item(el):
            continue
        ann = parse_c6_item(el)
        if ann:
            out.append(ann)

    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def fetch_cessionpro(session: requests.Session, page: int) -> List[Announcement]:
    _ = page
    html = http_get(session, URL6_CESSIONPRO, extra_headers={"Referer": C6_BASE})
    return parse_cessionpro_listing(html, only_new=True)


def format_cessionpro(ann: Announcement) -> str:
    lines = [f"[CessionPro] {ann.title}"]
    if ann.meta.get("location"):
        lines.append(f"Localisation: {ann.meta['location']}")
    if ann.meta.get("sector"):
        lines.append(f"Secteur: {ann.meta['sector']}")
    if ann.meta.get("price"):
        lines.append(f"Prix: {ann.meta['price']}")
    if ann.meta.get("turnover"):
        lines.append(f"CA: {ann.meta['turnover']}")
    if ann.meta.get("reference"):
        lines.append(f"Réf: {ann.meta['reference']}")
    if ann.meta.get("email") or ann.meta.get("phone"):
        lines.append(
            f"Contact: {ann.meta.get('email','')}{' ' if ann.meta.get('email') and ann.meta.get('phone') else ''}{ann.meta.get('phone','')}".strip()
        )
    if ann.meta.get("description"):
        d = ann.meta["description"]
        if len(d) > 280:
            d = d[:280] + "…"
        lines.append(f"Résumé: {d}")
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# SITE 3 — WE Transmission (Wallonie Entreprendre) — SPA via backend API discovery
# =============================================================================
WET_SITE = "we-transmission"
WET_BASE = "https://transmission.wallonie-entreprendre.be"
WET_CATALOGUE_URL = f"{WET_BASE}/catalogue"
WET_PAGES_TO_TRY = 3


def safe_json_loads(text: str):
    try:
        return json.loads(text)
    except Exception:
        return None


def find_best_list_in_json(obj):
    best = []

    def walk(x):
        nonlocal best
        if isinstance(x, list):
            if x and all(isinstance(i, dict) for i in x):
                score = 0
                for k in ("title", "titre", "name", "nom", "headline", "libelle"):
                    if k in x[0]:
                        score += 1
                if score > 0 and len(x) > len(best):
                    best = x
            for i in x:
                walk(i)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)

    walk(obj)
    return best


def pick_first(d: dict, keys):
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return None


def discover_wet_api_endpoints(session: requests.Session) -> List[str]:
    html = http_get(session, WET_CATALOGUE_URL)

    script_srcs = set(re.findall(r'src=["\'](/assets/[^"\']+\.js)["\']', html))
    if not script_srcs:
        script_srcs = set(re.findall(r'(/assets/[^"\']+\.js)', html))

    bundles = list(script_srcs)[:8]
    candidates = set()

    for src in bundles:
        js_url = absolute_url(WET_BASE, src)
        try:
            js = http_get(session, js_url)
        except Exception:
            continue

        # Absolute URLs
        for m in re.findall(r'https?://[^"\']+?/backend/api/v1/[^"\']+', js):
            candidates.add(m)

        # Relative paths
        for m in re.findall(r'(/backend/api/v1/[^"\']+)', js):
            candidates.add(absolute_url(WET_BASE, m))

        # Also capture plain base if present
        for m in re.findall(r'https?://[^"\']+?/backend/api/v1\b', js):
            candidates.add(m)

    expanded = set(candidates)

    # If we only have the base ".../backend/api/v1", expand with common suffixes
    for u in list(candidates):
        if re.search(r"/backend/api/v1/?$", u):
            base = u.rstrip("/")
            for suf in (
                "/catalogue",
                "/catalog",
                "/offres",
                "/offers",
                "/annonces",
                "/listings",
                "/public/offres",
                "/public/offers",
                "/public/annonces",
                "/public/listings",
                "/search",
            ):
                expanded.add(base + suf)

    def score(u: str) -> int:
        lu = u.lower()
        s = 0
        for kw in ("catalog", "offre", "offer", "listing", "annonce", "search", "public"):
            if kw in lu:
                s += 10
        if lu.endswith(".js"):
            s -= 50
        return s

    ranked = sorted(expanded, key=lambda x: (-score(x), x))
    return ranked[:60]


def wet_items_to_announcements(items: list) -> List[Announcement]:
    out: List[Announcement] = []
    for it in items:
        if not isinstance(it, dict):
            continue

        title = text_clean(str(pick_first(it, ["title", "titre", "name", "nom", "headline"]) or ""))
        if len(title) < 3:
            continue

        url = pick_first(it, ["url", "detailUrl", "permalink", "href", "link"])
        if url:
            url = normalize_url(absolute_url(WET_BASE, str(url)))
        else:
            _id = pick_first(it, ["id", "uuid", "reference", "ref"])
            slug = pick_first(it, ["slug"])
            if _id and slug:
                url = f"{WET_BASE}/offres/{_id}-{slug}"
            elif _id:
                url = f"{WET_BASE}/offres/{_id}"
            else:
                url = WET_CATALOGUE_URL

        meta: Dict[str, str] = {}
        loc = pick_first(it, ["location", "localisation", "ville", "city", "region", "province"])
        if loc:
            meta["location"] = text_clean(str(loc))

        sector = pick_first(it, ["sector", "secteur", "activity", "domaine"])
        if sector:
            meta["sector"] = text_clean(str(sector))

        teaser = pick_first(it, ["teaser", "summary", "resume", "descriptionShort", "shortDescription"])
        if teaser:
            meta["teaser"] = text_clean(str(teaser))

        ann = Announcement(WET_SITE, title, url, meta)
        if forbidden_hit(ann):
            continue
        out.append(ann)

    uniq: Dict[str, Announcement] = {}
    for a in out:
        uniq[a.url] = a
    return list(uniq.values())


def try_fetch_wet_listings_from_endpoint(session: requests.Session, url: str, page: int) -> List[Announcement]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "fr,en;q=0.8,nl;q=0.6",
        "Referer": WET_CATALOGUE_URL,
    }

    # GET param conventions
    param_sets = [
        {"page": page, "size": 20},
        {"page": page - 1, "size": 20},  # 0-based
        {"pageNumber": page, "pageSize": 20},
        {"p": page, "limit": 20},
        {"offset": (page - 1) * 20, "limit": 20},
    ]

    for params in param_sets:
        try:
            r = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code in (401, 403):
                return []
            if not r.ok:
                continue
            data = safe_json_loads(r.text)
            if not data:
                continue
            items = find_best_list_in_json(data)
            if items:
                return wet_items_to_announcements(items)
        except Exception:
            pass

    # POST body conventions
    bodies = [
        {"page": page, "size": 20},
        {"page": page - 1, "size": 20},
        {"pagination": {"page": page, "size": 20}},
        {"offset": (page - 1) * 20, "limit": 20},
        {"pageNumber": page, "pageSize": 20},
    ]
    for body in bodies:
        try:
            r = session.post(url, headers={**headers, "Content-Type": "application/json"}, json=body, timeout=REQUEST_TIMEOUT)
            if r.status_code in (401, 403):
                return []
            if not r.ok:
                continue
            data = safe_json_loads(r.text)
            if not data:
                continue
            items = find_best_list_in_json(data)
            if items:
                return wet_items_to_announcements(items)
        except Exception:
            pass

    return []


def fetch_wet_listings(session: requests.Session, state: Dict[str, Any]) -> List[Announcement]:
    cached = meta_get(state, WET_SITE, "api_url", None)
    endpoints = [cached] if cached else []
    if not cached:
        endpoints = discover_wet_api_endpoints(session)

    for api_url in endpoints:
        if not api_url:
            continue

        all_items: List[Announcement] = []
        for page in range(1, WET_PAGES_TO_TRY + 1):
            items = try_fetch_wet_listings_from_endpoint(session, api_url, page)
            if page > 1 and not items:
                break
            all_items.extend(items)
            time.sleep(0.6)

        if all_items:
            meta_set(state, WET_SITE, "api_url", api_url)
            return all_items

    # Keep useful debug for Actions logs
    if not cached:
        print("[we-transmission] no working endpoint found after discovery.")
    return []


def format_wet_message(ann: Announcement) -> str:
    lines = [f"[WE Transmission] {ann.title}"]
    if ann.meta.get("location"):
        lines.append(f"Localisation: {ann.meta['location']}")
    if ann.meta.get("sector"):
        lines.append(f"Secteur: {ann.meta['sector']}")
    if ann.meta.get("teaser"):
        t = ann.meta["teaser"]
        if len(t) > 280:
            t = t[:280] + "…"
        lines.append(f"Résumé: {t}")
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# SITE 5 — bedrijventekoop.be (POST listings + fallback ID probing)
# =============================================================================
BTK_SITE = "bedrijventekoop"
BTK_BASE = "https://www.bedrijventekoop.be"
BTK_HOME_URL = BTK_BASE + "/"
BTK_MAX_PROBE_PER_RUN = 40


def btk_extract_token(html: str) -> str:
    soup = soupify(html)
    meta = soup.select_one('meta[name="csrf-token"], meta[name="csrf_token"], meta[name="xsrf-token"]')
    if meta and meta.get("content"):
        return meta["content"].strip()
    return ""


def btk_guess_post_url(html: str) -> str:
    if re.search(r'(["\'])\/listings-post\1', html):
        return "/listings-post"
    return "/listings-post"


def btk_guess_webspace_locale(html: str) -> Tuple[str, str]:
    webspace = re.search(r'"webspace"\s*:\s*"([^"]+)"', html)
    locale = re.search(r'"locale"\s*:\s*"([^"]+)"', html)
    return (webspace.group(1) if webspace else "btkbe", locale.group(1) if locale else "nl_be")


def parse_btk_list_fragment(html_fragment: str) -> List[Announcement]:
    soup = soupify(html_fragment)
    out: List[Announcement] = []

    for a in soup.select('a[href*="/te-koop-aangeboden/"], a[href*="/for-sale/"]'):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "/te-koop-aangeboden/" not in href and "/for-sale/" not in href:
            continue
        url = normalize_url(absolute_url(BTK_BASE, href))
        title = text_clean(a.get_text(" ", strip=True)) or "(title pending)"
        out.append(Announcement(BTK_SITE, title, url, {}))

    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def parse_btk_detail(html: str) -> Dict[str, str]:
    soup = soupify(html)
    meta: Dict[str, str] = {}

    h1 = soup.find("h1")
    if h1:
        meta["title"] = text_clean(h1.get_text(" ", strip=True))

    page_text = norm_cmp(soup.get_text(" ", strip=True))
    if "verkocht" in page_text or "inactief" in page_text:
        meta["inactive"] = "true"

    # dl/dt/dd blocks often exist
    def dl_value(label: str) -> str:
        for dt in soup.find_all("dt"):
            if norm_cmp(dt.get_text(" ", strip=True)) == norm_cmp(label):
                dd = dt.find_next_sibling("dd")
                if dd:
                    return text_clean(dd.get_text(" ", strip=True))
        return ""

    meta["region"] = dl_value("Regio") or dl_value("Region") or ""
    meta["branche"] = dl_value("Branche") or dl_value("Sector") or ""
    meta["omzet"] = dl_value("Omzet") or dl_value("Omzet indicatie") or ""
    meta["overname"] = dl_value("Overname") or dl_value("Indicatie overnamebedrag") or ""
    meta["resultaat"] = dl_value("Resultaat voor belasting") or ""

    m = re.search(r"Aangeboden sinds\s+([0-9]{1,2}\s+\w+\s+[0-9]{4})", soup.get_text(" ", strip=True), flags=re.I)
    if m:
        meta["listed_since"] = m.group(1)

    return {k: v for k, v in meta.items() if v}


def format_bedrijventekoop_enriched(session: requests.Session, ann: Announcement) -> Optional[str]:
    # Enrich by fetching detail page
    meta = dict(ann.meta or {})
    title = ann.title

    try:
        html = http_get(session, ann.url, extra_headers={"Referer": BTK_BASE})
        d = parse_btk_detail(html)

        if d.get("inactive") == "true":
            return None

        if d.get("title"):
            title = d["title"]
        meta.update(d)
    except Exception as e:
        print(f"[bedrijventekoop] detail enrichment failed: {e}")

    enriched = Announcement(ann.site, title, ann.url, meta)
    if forbidden_hit(enriched):
        return None

    lines = [f"[Bedrijventekoop] {title}"]
    if meta.get("region"):
        lines.append(f"Région: {meta['region']}")
    if meta.get("branche"):
        lines.append(f"Branche: {meta['branche']}")
    if meta.get("omzet"):
        lines.append(f"Omzet: {meta['omzet']}")
    if meta.get("overname"):
        lines.append(f"Overname: {meta['overname']}")
    if meta.get("resultaat"):
        lines.append(f"Résultat (avant impôt): {meta['resultaat']}")
    if meta.get("listed_since"):
        lines.append(f"Publié: {meta['listed_since']}")
    lines.append(ann.url)
    return "\n".join(lines)


def extract_btk_ids_from_html(html: str) -> List[int]:
    """
    Try to extract listing IDs from HTML/inline JSON.
    Supports:
      - /te-koop-aangeboden/<id>
      - "listingId": 12345, "id":12345 near te-koop-aangeboden
    """
    ids = set()

    for m in re.findall(r"/te-koop-aangeboden/(\d{3,})", html):
        try:
            ids.add(int(m))
        except Exception:
            pass

    # inline JSON-ish IDs (best effort)
    for m in re.findall(r'"listingId"\s*:\s*(\d{3,})', html):
        try:
            ids.add(int(m))
        except Exception:
            pass

    # cautious: "id":12345 but only if page mentions te-koop-aangeboden somewhere
    if "/te-koop-aangeboden" in html:
        for m in re.findall(r'"id"\s*:\s*(\d{3,})', html):
            try:
                ids.add(int(m))
            except Exception:
                pass

    return sorted(ids)


def fetch_btk_listings_via_post(session: requests.Session) -> Optional[List[Announcement]]:
    landing_url = URL5_BEDRIJVENTEKOOP
    landing_html = http_get(session, landing_url, extra_headers={"Referer": BTK_BASE})

    token = btk_extract_token(landing_html)
    webspace, locale = btk_guess_webspace_locale(landing_html)
    post_url = absolute_url(BTK_BASE, btk_guess_post_url(landing_html))

    p = urlparse(URL5_BEDRIJVENTEKOOP)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    sectors_csv = q.get("sectors", "")
    regions_csv = q.get("regions", "")

    extra_headers = {}
    if token:
        extra_headers["X-CSRF-TOKEN"] = token

    # Try multiple payload styles (FORM + JSON)
    for page in range(1, 3):  # keep it light; we only need newest
        candidates_form: List[Dict[str, str]] = []

        candidates_form.append({"page": str(page), "webspace": webspace, "locale": locale})
        candidates_form.append({"p": str(page), "webspace": webspace, "locale": locale})

        filter_obj = {
            "q": "",
            "sectors": sectors_csv.split(",") if sectors_csv else [],
            "regions": [int(x) for x in regions_csv.split(",") if x.isdigit()] if regions_csv else [],
            "archived": False,
            "purposeType": 1,
            "page": page,
        }
        candidates_form.append({"webspace": webspace, "locale": locale, "payload": json.dumps(filter_obj)})
        candidates_form.append({"webspace": webspace, "locale": locale, "offset": str((page - 1) * 20), "limit": "20"})

        for data in candidates_form:
            try:
                resp = http_post_form(session, post_url, data=data, referer=landing_url, extra_headers=extra_headers)
            except Exception:
                continue

            # Sometimes JSON with HTML inside
            if "/te-koop-aangeboden/" in resp or "/for-sale/" in resp:
                return parse_btk_list_fragment(resp)

            j = safe_json_loads(resp)
            if isinstance(j, dict):
                for v in j.values():
                    if isinstance(v, str) and ("/te-koop-aangeboden/" in v or "/for-sale/" in v):
                        return parse_btk_list_fragment(v)

        # JSON post attempt
        try:
            resp2 = http_post_json(session, post_url, body=filter_obj, referer=landing_url, extra_headers=extra_headers)
            if "/te-koop-aangeboden/" in resp2 or "/for-sale/" in resp2:
                return parse_btk_list_fragment(resp2)
            j2 = safe_json_loads(resp2)
            if isinstance(j2, dict):
                for v in j2.values():
                    if isinstance(v, str) and ("/te-koop-aangeboden/" in v or "/for-sale/" in v):
                        return parse_btk_list_fragment(v)
        except Exception:
            pass

    return None


def fetch_btk_detail_by_id(session: requests.Session, listing_id: int) -> Optional[Announcement]:
    url_try = f"{BTK_BASE}/te-koop-aangeboden/{listing_id}"
    r = session.get(
        url_try,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "nl,fr;q=0.8,en;q=0.6"},
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
    )
    if r.status_code == 404 or not r.ok:
        return None

    final_url = normalize_url(r.url)
    soup = soupify(r.text)

    h1 = soup.find("h1")
    title = text_clean(h1.get_text(" ")) if h1 else ""
    if len(title) < 3:
        return None

    txt = soup.get_text("\n", strip=True)
    meta = {}

    for label in ("Regio", "Provincie", "Plaats", "Locatie"):
        m = re.search(rf"{label}\s*[:\-]\s*(.+)", txt, flags=re.I)
        if m:
            meta["location"] = text_clean(m.group(1))
            break

    for label in ("Branche", "Sector", "Categorie"):
        m = re.search(rf"{label}\s*[:\-]\s*(.+)", txt, flags=re.I)
        if m:
            meta["sector"] = text_clean(m.group(1))
            break

    ann = Announcement(BTK_SITE, title, final_url, meta)
    if forbidden_hit(ann):
        return None
    return ann


def fetch_btk_listings_fallback(session: requests.Session, state: Dict[str, Any]) -> List[Announcement]:
    """
    Fallback strategy:
      - scrape IDs from homepage + listing page HTML (if any)
      - probe IDs newer than last_id
    """
    home_html = ""
    list_html = ""
    try:
        home_html = http_get(session, BTK_HOME_URL)
    except Exception:
        pass
    try:
        list_html = http_get(session, URL5_BEDRIJVENTEKOOP, extra_headers={"Referer": BTK_BASE})
    except Exception:
        pass

    ids = sorted(set(extract_btk_ids_from_html(home_html) + extract_btk_ids_from_html(list_html)))
    if not ids:
        return []

    max_id = max(ids)
    last_id = int(meta_get(state, BTK_SITE, "last_id", 0) or 0)

    # first run: initialize without spamming
    if last_id == 0:
        meta_set(state, BTK_SITE, "last_id", max_id)
        return []

    if max_id <= last_id:
        return []

    new_items: List[Announcement] = []
    to_probe = list(range(last_id + 1, max_id + 1))[:BTK_MAX_PROBE_PER_RUN]

    highest_seen = last_id
    for listing_id in to_probe:
        try:
            ann = fetch_btk_detail_by_id(session, listing_id)
            if ann:
                new_items.append(ann)
            highest_seen = max(highest_seen, listing_id)
        except Exception:
            highest_seen = max(highest_seen, listing_id)
        time.sleep(0.6)

    meta_set(state, BTK_SITE, "last_id", highest_seen)
    return new_items


def fetch_btk_listings(session: requests.Session, state: Dict[str, Any]) -> List[Announcement]:
    # Try POST listings first (best quality). If it fails, fallback.
    items = fetch_btk_listings_via_post(session)
    if items is not None and len(items) > 0:
        return items
    return fetch_btk_listings_fallback(session, state)


def format_btk_message(ann: Announcement) -> Optional[str]:
    # placeholder; actual formatting uses enrichment inside runner via the enriched formatter below
    # (we keep this for compatibility, but not used)
    lines = [f"[Bedrijventekoop] {ann.title}", ann.url]
    return "\n".join(lines)


# =============================================================================
# MAIN
# =============================================================================
_GLOBAL_SESSION = make_session()


def main() -> None:
    global STATE_DIRTY

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    state = load_state()
    any_changed = False

    # 4 HTML sites
    html_sites = [
        (COFIM_SITE, fetch_cofim, format_cofim, COFIM_MAX_PAGES),
        (CAR_SITE, fetch_car, format_car, CAR_MAX_PAGES),
        (OV_SITE, fetch_overnamemarkt, format_overnamemarkt, OVERNAMEMARKT_MAX_PAGES),
        (C6_SITE, fetch_cessionpro, format_cessionpro, CESSIONPRO_MAX_PAGES),
    ]

    for (site_name, fetch_fn, format_fn, max_pages) in html_sites:
        new_detected, sent, changed = run_site(
            session=_GLOBAL_SESSION,
            site_name=site_name,
            fetch_fn=fetch_fn,
            format_fn=format_fn,
            max_pages=max_pages,
            state=state,
            bot_token=bot_token,
            chat_id=chat_id,
        )
        any_changed = any_changed or changed
        print(f"[{site_name}] new_detected={new_detected} sent={sent}")

    # WE Transmission (SPA) — direct
    new_detected, sent, changed = run_site_direct(
        session=_GLOBAL_SESSION,
        site_name=WET_SITE,
        fetch_items_fn=fetch_wet_listings,
        format_fn=format_wet_message,
        state=state,
        bot_token=bot_token,
        chat_id=chat_id,
    )
    any_changed = any_changed or changed
    print(f"[{WET_SITE}] new_detected={new_detected} sent={sent}")

    # Bedrijventekoop — direct fetch + detail enrichment at formatting time
    # We wrap format so it enriches and also re-checks filters after enrichment.
    def _btk_formatter(ann: Announcement) -> Optional[str]:
        return format_bedrijventekoop_enriched(_GLOBAL_SESSION, ann)

    new_detected, sent, changed = run_site_direct(
        session=_GLOBAL_SESSION,
        site_name=BTK_SITE,
        fetch_items_fn=fetch_btk_listings,
        format_fn=_btk_formatter,
        state=state,
        bot_token=bot_token,
        chat_id=chat_id,
    )
    any_changed = any_changed or changed
    print(f"[{BTK_SITE}] new_detected={new_detected} sent={sent}")

    if any_changed or STATE_DIRTY:
        save_state(state)


if __name__ == "__main__":
    main()
