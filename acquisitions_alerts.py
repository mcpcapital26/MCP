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

import os
import re
import json
import time
import hashlib
import unicodedata
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Callable
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


USER_AGENT = "Mozilla/5.0 (AcquisitionAnnouncementsBot; +https://github.com/)"
STATE_FILE = "seen_announcements_by_site.json"

REQUEST_TIMEOUT = 35
SLEEP_BETWEEN_PAGES_SEC = 1.0
MAX_NEW_PER_RUN = 40  # cap to avoid spam/long runs


# ----------------------------
# USER FILTER (editable)
# ----------------------------
# Any announcement whose title/location/teaser/type/sector/etc contains ANY of these words
# (case-insensitive, substring match) will be discarded.
FORBIDDEN_WORDS = [
    "horeca",
    "restaurant",
    "bar",
    "traiteur",
    "alimentaire",
]


# ----------------------------
# URLS (your 6 sources)
# ----------------------------
URL1_COFIM = "https://www.cofim.be/fr/entreprises/entreprises-fonds-de-commerce"
URL2_CAR = "https://www.commerce-a-remettre.be/recherche?region=&sector=&id="
URL3_WE = "https://transmission.wallonie-entreprendre.be/"
URL4_OVERNAMEMARKT = "https://www.overnamemarkt.be/fr/acheter?sectors=bouw,diensten,industrie,distributie,vrije-beroepen,andere"
URL5_BEDRIJVENTEKOOP = "https://www.bedrijventekoop.be/te-koop-aangeboden?sectors=2,4,12,2_216,2_217,2_218,2_83,2_222,2_219,2_86,2_94,2_93,2_91,2_220,2_221,2_87,2_92,2_233,2_89,2_85,2_223,2_99,2_224,2_225,2_90,2_84,4_12,4_13,4_15,4_14,4_101,8_51,8_62,8_53,8_59,8_64,8_230,8_242,8_54,8_204,8_67,8_229,8_60,8_215,8_66,8_105,8_52,8_49,8_56,8_234,8_88,8_241,8_65,8_228,8_55,12_208,12_61,12_206,12_108,12_207,12_205&regions=26,27,28,29,30,31,32,33,34,35,36,37,38,39"
URL6_CESSIONPRO = "https://www.cessionpro.be/?secteurs-v4oj=entreprise-de-construction-a-remettre-en-belgique%7Ce-commerce-a-vendre-en-belgique%7Csociete-industrielle-a-vendre-en-belgique%7Csociete-de-service-a-vendre-en-belgique"


# ----------------------------
# Per-site paging limits
# ----------------------------
COFIM_MAX_PAGES = 3
CAR_MAX_PAGES = 3
WE_MAX_PAGES = 3
OVERNAMEMARKT_MAX_PAGES = 3
BEDRIJVENTEKOOP_MAX_PAGES = 3
CESSIONPRO_MAX_PAGES = 1


# ----------------------------
# Model
# ----------------------------
@dataclass
class Announcement:
    site: str
    title: str
    url: str
    meta: Dict[str, str]
    uid: Optional[str] = None  # stable id when URL is not stable/known

    @property
    def key(self) -> str:
        """
        Stable key for dedup/state:
        - If uid provided: site:uid
        - Else: prefer numeric ID segment in URL path
        - Else: last path segment
        - Else: sha256(url+title)
        """
        if self.uid:
            return f"{self.site}:{self.uid}"

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


# ----------------------------
# HTTP helpers
# ----------------------------
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


def http_post_json(
    session: requests.Session,
    url: str,
    payload: dict,
    extra_headers: Optional[Dict[str, str]] = None,
) -> requests.Response:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "fr,en;q=0.8,nl;q=0.6",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Connection": "keep-alive",
    }
    if extra_headers:
        headers.update(extra_headers)

    r = session.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r


def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def text_clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def absolute_url(base: str, href: str) -> str:
    return urljoin(base, href) if href else ""


def normalize_url(u: str) -> str:
    """
    Remove tracking params (utm_*) and fragments to stabilize dedup.
    Keep functional query params.
    """
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


# ----------------------------
# State
# ----------------------------
def load_state() -> Dict[str, List[str]]:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return {k: list(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def save_state(state: Dict[str, List[str]]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# ----------------------------
# Telegram
# ----------------------------
def send_telegram(bot_token: str, chat_id: str, message: str) -> None:
    if not bot_token or not chat_id:
        print("Telegram not configured; skipping send.")
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "disable_web_page_preview": True}
    rr = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    if not rr.ok:
        print("Telegram send failed:", rr.text)


# ----------------------------
# Filters (applies to ALL sites)
# ----------------------------
def forbidden_hit(ann: Announcement) -> str:
    words = [norm_cmp(w) for w in FORBIDDEN_WORDS if w and w.strip()]
    if not words:
        return ""
    hay = norm_cmp(
        " ".join(
            [
                ann.title or "",
                ann.meta.get("location", ""),
                ann.meta.get("location_details", ""),
                ann.meta.get("price", ""),
                ann.meta.get("teaser", ""),
                ann.meta.get("type", ""),
                ann.meta.get("sector", ""),
                ann.meta.get("sectors", ""),
                ann.meta.get("branche", ""),
                ann.meta.get("region", ""),
                ann.meta.get("description", ""),
            ]
        )
    )
    for w in words:
        if w and w in hay:
            return w
    return ""


# ----------------------------
# Generic runner (HTML sites + API sites with pagination)
# ----------------------------
FetchFn = Callable[[requests.Session, int], List[Announcement]]
FormatFn = Callable[[Announcement], Optional[str]]


def run_site(
    session: requests.Session,
    site_name: str,
    fetch_fn: FetchFn,
    format_fn: FormatFn,
    max_pages: int,
    state: Dict[str, List[str]],
    bot_token: str,
    chat_id: str,
) -> Tuple[int, int, bool]:
    """
    Returns: (new_detected, alerts_sent, changed_state)

    Notes:
    - Forbidden items are marked seen immediately (so they don't resurface).
    - Items returning None/"" from formatter are marked seen (inactive / filtered after enrichment).
    - MAX_NEW_PER_RUN limits only sends; remaining "new" items stay for next run.
    """
    seen = set(state.get(site_name, []))
    changed = False

    all_items: List[Announcement] = []
    for page in range(1, max_pages + 1):
        try:
            items = fetch_fn(session, page)
            print(f"[{site_name}] page {page}: {len(items)} items (after parsing)")
            if page > 1 and not items:
                # stop early if page is empty
                break
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
        # filter (and mark seen) for all sites
        if forbidden_hit(ann):
            seen.add(ann.key)
            changed = True
            continue

        if sent >= MAX_NEW_PER_RUN:
            break

        msg = None
        try:
            msg = format_fn(ann)
        except Exception as e:
            print(f"[{site_name}] format error: {e}")

        if not msg:
            seen.add(ann.key)
            changed = True
            continue

        try:
            send_telegram(bot_token, chat_id, msg)
            sent += 1
            seen.add(ann.key)
            changed = True
        except Exception as e:
            print(f"[{site_name}] telegram send error: {e}")

        time.sleep(0.5)

    state[site_name] = list(seen)
    return (len(new_items), sent, changed)


# =============================================================================
# SITE 1 — COFIM (HTML)
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

        # Filter sold
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

    # Dedup by URL
    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def fetch_cofim(session: requests.Session, page: int) -> List[Announcement]:
    url = build_cofim_page_url(page)
    html = http_get(session, url, extra_headers={"Referer": COFIM_BASE})
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
# SITE 2 — Commerce-a-remettre (HTML)
# =============================================================================
CAR_SITE = "commerce-a-remettre"
CAR_BASE = "https://www.commerce-a-remettre.be"


def build_car_page_url(page: int) -> str:
    # Pagination is 0-based: page=0 is first
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

        title = ""
        if a:
            title = text_clean(a.get_text(" "))
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
# SITE 3 — WE Transmission (API) — uses YOUR cURL exactly
# =============================================================================
WE_SITE = "we-transmission"
WE_BASE = "https://transmission.wallonie-entreprendre.be"
WE_API_VISITOR = f"{WE_BASE}/backend/api/v1/company/visitor"
WE_CATALOGUE = f"{WE_BASE}/catalogue"


def fetch_we(session: requests.Session, page: int) -> List[Announcement]:
    """
    Matches your provided cURL:
      POST https://transmission.wallonie-entreprendre.be/backend/api/v1/company/visitor
      body: {"page":1,"pageSize":"9","keyword":"","turnover":"","sector":[],"localisation":"","price":"","is_published":"1"}
    """
    # Make sure session has site cookies (not strictly required, but safe)
    try:
        session.get(WE_BASE + "/", headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    except Exception:
        pass

    payload = {
        "page": page,
        "pageSize": "9",
        "keyword": "",
        "turnover": "",
        "sector": [],
        "localisation": "",
        "price": "",
        "is_published": "1",
    }

    r = http_post_json(
        session,
        WE_API_VISITOR,
        payload=payload,
        extra_headers={
            "Origin": WE_BASE,
            "Referer": WE_BASE + "/",
        },
    )

    data = r.json()
    items = data.get("data", []) if isinstance(data, dict) else []
    if not isinstance(items, list):
        return []

    out: List[Announcement] = []
    for it in items:
        if not isinstance(it, dict):
            continue

        _id = str(it.get("id") or "").strip()
        title = text_clean(str(it.get("title") or ""))
        if not _id or len(title) < 3:
            continue

        desc = text_clean(str(it.get("description") or ""))
        location = text_clean(str(it.get("location") or ""))
        location_details = text_clean(str(it.get("location_details") or ""))
        created = text_clean(str(it.get("createdAt") or ""))

        # sectors: list of {value: "..."}
        sectors = []
        for s in (it.get("sectors") or []):
            if isinstance(s, dict) and s.get("value"):
                sectors.append(text_clean(str(s["value"])))
        sectors_str = ", ".join([x for x in sectors if x])

        # price: transfer_details.price
        price = ""
        td = it.get("transfer_details") or it.get("transfer_detail_filter") or {}
        if isinstance(td, dict) and td.get("price"):
            price = text_clean(str(td.get("price")))

        # turnover: take most recent year in turnovers list if present
        turnover = ""
        turnovers = it.get("turnovers") or it.get("turnover_filter") or []
        if isinstance(turnovers, list) and turnovers:
            # pick first element (often newest), else best-effort
            t0 = turnovers[0]
            if isinstance(t0, dict) and t0.get("turnover"):
                turnover = text_clean(str(t0.get("turnover")))

        meta: Dict[str, str] = {}
        if location:
            meta["location"] = location
        if location_details:
            meta["location_details"] = location_details
        if created:
            meta["createdAt"] = created
        if sectors_str:
            meta["sectors"] = sectors_str
        if price:
            meta["price"] = price
        if turnover:
            meta["turnover"] = turnover
        if desc:
            meta["description"] = desc

        # We do NOT guess a detail URL pattern. We provide catalogue + ID for retrieval.
        out.append(
            Announcement(
                WE_SITE,
                title,
                WE_CATALOGUE,
                meta,
                uid=_id,
            )
        )

    return out


def format_we(ann: Announcement) -> str:
    lines = [f"[WE Transmission] {ann.title}"]
    lines.append(f"ID: {ann.uid or ''}".strip())
    if ann.meta.get("location"):
        lines.append(f"Localisation: {ann.meta['location']}")
    if ann.meta.get("location_details"):
        lines.append(f"Détails: {ann.meta['location_details']}")
    if ann.meta.get("sectors"):
        lines.append(f"Secteurs: {ann.meta['sectors']}")
    if ann.meta.get("turnover"):
        lines.append(f"CA: {ann.meta['turnover']}")
    if ann.meta.get("price"):
        lines.append(f"Prix: {ann.meta['price']}")
    if ann.meta.get("createdAt"):
        lines.append(f"Publié: {ann.meta['createdAt']}")
    if ann.meta.get("description"):
        d = ann.meta["description"]
        if len(d) > 280:
            d = d[:280] + "…"
        lines.append(f"Résumé: {d}")
    lines.append(ann.url)  # catalogue URL
    return "\n".join(lines)


# =============================================================================
# SITE 4 — Overnamemarkt (HTML)
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
            title = text_clean(sp.get_text(" ", strip=True)) if sp else "(sans titre)"

        location = ""
        icon = a.select_one('i[class*="fa-map-marker"]')
        if icon and icon.parent:
            spans = icon.parent.find_all("span")
            if spans:
                location = text_clean(spans[-1].get_text(" ", strip=True))

        price = _find_value_by_label_in_anchor(a, "Prix")
        turnover = (
            _find_value_by_label_in_anchor(a, "Chiffre d'affaires")
            or _find_value_by_label_in_anchor(a, "Chiffre d’affaires")
        )

        sector = ""
        for sp in a.find_all("span"):
            cls = " ".join(sp.get("class", [])).lower()
            if "inline-flex" in cls and sp.find("i"):
                sector = text_clean(sp.get_text(" ", strip=True))
                if sector:
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
# SITE 5 — Bedrijventekoop (API POST) — uses YOUR cURL exactly
# =============================================================================
BTK_SITE = "bedrijventekoop"
BTK_BASE = "https://www.bedrijventekoop.be"
BTK_POST = f"{BTK_BASE}/listings-post"


def _btk_filters_from_url() -> Tuple[List[str], List[str]]:
    p = urlparse(URL5_BEDRIJVENTEKOOP)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    sectors = [s.strip() for s in (q.get("sectors") or "").split(",") if s.strip()]
    regions = [r.strip() for r in (q.get("regions") or "").split(",") if r.strip()]
    return sectors, regions


def _btk_extract_html_blob(resp: requests.Response) -> str:
    """
    listings-post may return:
    - HTML fragment (text/html)
    - JSON containing an HTML fragment somewhere
    We handle both without guessing keys too aggressively.
    """
    ct = (resp.headers.get("Content-Type") or "").lower()
    text = resp.text or ""

    if "application/json" in ct:
        try:
            j = resp.json()
        except Exception:
            return text

        # Try common patterns: {html: "..."} or {data: "..."} etc
        if isinstance(j, dict):
            for k, v in j.items():
                if isinstance(v, str) and ("te-koop-aangeboden" in v or "for-sale" in v or "<a" in v):
                    return v
            # sometimes nested
            for v in j.values():
                if isinstance(v, dict):
                    for vv in v.values():
                        if isinstance(vv, str) and ("te-koop-aangeboden" in vv or "for-sale" in vv or "<a" in vv):
                            return vv

        return text

    return text


def parse_btk_fragment(html_fragment: str) -> List[Announcement]:
    soup = soupify(html_fragment)
    out: List[Announcement] = []

    # Detail URLs typically include /te-koop-aangeboden/...
    for a in soup.select('a[href*="/te-koop-aangeboden/"], a[href*="/for-sale/"]'):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "/te-koop-aangeboden/" not in href and "/for-sale/" not in href:
            continue

        url = normalize_url(absolute_url(BTK_BASE, href))

        # Title: try nearest headings, then link text
        title = ""
        # climb a bit: parent card might contain h2/h3
        parent = a
        for _ in range(0, 4):
            if parent and parent.parent:
                parent = parent.parent
        if parent:
            h = parent.find(["h1", "h2", "h3"])
            if h:
                title = text_clean(h.get_text(" ", strip=True))
        if not title:
            title = text_clean(a.get_text(" ", strip=True))
        if not title or len(title) < 3:
            title = "(title not parsed yet)"

        out.append(Announcement(BTK_SITE, title, url, {}))

    # Dedup by URL
    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def fetch_btk(session: requests.Session, page: int) -> List[Announcement]:
    """
    Matches your provided cURL to /listings-post (JSON body).
    """
    # prime cookies/session
    try:
        session.get(
            URL5_BEDRIJVENTEKOOP,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"},
            timeout=REQUEST_TIMEOUT,
        )
    except Exception:
        pass

    sectors, regions = _btk_filters_from_url()

    payload = {
        "type": 1,
        "sectors": sectors,
        "regions": regions if regions else [],  # derived from your URL; set [] if you don't want region filtering
        "turnovers": [],
        "askingprice": [],
        "typeofacquisition": [],
        "legalentity": [],
        "typeoftransaction": [],
        "employees": [],
        "lifephaseenterprise": [],
        "platform": [],
        "search": "",
        "movable": False,
        "page": page,
        "limit": 15,
        "archive": False,
    }

    resp = http_post_json(
        session,
        BTK_POST,
        payload=payload,
        extra_headers={
            "Origin": BTK_BASE,
            "Referer": URL5_BEDRIJVENTEKOOP,
            "Accept": "*/*",
        },
    )

    blob = _btk_extract_html_blob(resp)
    return parse_btk_fragment(blob)


def format_btk(ann: Announcement) -> str:
    lines = [f"[Bedrijventekoop] {ann.title}"]
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# SITE 6 — CessionPro (HTML)
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
    description = pick(".secondcard .rich-text-block-annonce")

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

    return Announcement(C6_SITE, title, url, meta)


def parse_cessionpro_listing(html: str, only_new: bool = True) -> List[Announcement]:
    soup = soupify(html)

    items = soup.select('div[role="listitem"].collectionitem.w-dyn-item')
    if not items:
        items = soup.select("div.w-dyn-item")

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
            ("Contact: " + ann.meta.get("email", "") + (" " if ann.meta.get("email") and ann.meta.get("phone") else "") + ann.meta.get("phone", "")).strip()
        )
    if ann.meta.get("description"):
        d = ann.meta["description"]
        if len(d) > 280:
            d = d[:280] + "…"
        lines.append(f"Résumé: {d}")
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    session = make_session()

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    state = load_state()
    any_changed = False

    sites = [
        (COFIM_SITE, fetch_cofim, format_cofim, COFIM_MAX_PAGES),
        (CAR_SITE, fetch_car, format_car, CAR_MAX_PAGES),
        (WE_SITE, fetch_we, format_we, WE_MAX_PAGES),
        (OV_SITE, fetch_overnamemarkt, format_overnamemarkt, OVERNAMEMARKT_MAX_PAGES),
        (BTK_SITE, fetch_btk, format_btk, BEDRIJVENTEKOOP_MAX_PAGES),
        (C6_SITE, fetch_cessionpro, format_cessionpro, CESSIONPRO_MAX_PAGES),
    ]

    for (site_name, fetch_fn, format_fn, max_pages) in sites:
        new_detected, sent, changed = run_site(
            session=session,
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

    if any_changed:
        save_state(state)


if __name__ == "__main__":
    main()
