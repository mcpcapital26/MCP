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
from typing import Optional
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
# Any announcement whose title/location/teaser/type/sector contains ANY of these words
# (case-insensitive, substring match) will be discarded.
FORBIDDEN_WORDS = [
     "horeca",
     "restaurant",
    "bar",
    "traiteur",
    "alimentaire"
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
OVERNAMEMARKT_MAX_PAGES = 3
BEDRIJVENTEKOOP_MAX_PAGES = 2
WE_MAX_PAGES = 1
CESSIONPRO_MAX_PAGES = 1

# Optional: focus CessionPro sectors (matches your URL6 intent)
CESSIONPRO_ALLOWED_SECTORS = {
    # Keep empty to allow all sectors
    "Construction et achèvement",
    "E-commerce / Web",
    "Industrie",
    "Service",
}


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
        - Prefer numeric ID segment if present (ex: /.../104308/...)
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
# Filters
# ----------------------------
def forbidden_hit(ann: Announcement) -> str:
    words = [w.strip().lower() for w in FORBIDDEN_WORDS if w and w.strip()]
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
        ]
    ).lower()
    for w in words:
        if w in hay:
            return w
    return ""


# ----------------------------
# Generic runner
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
    Behavior:
      - Items matching FORBIDDEN_WORDS are marked as seen (so they don't resurface).
      - Items returning None/"" from formatter are marked as seen (inactive / filtered after enrichment).
      - MAX_NEW_PER_RUN only limits sent alerts; non-sent non-filtered items remain unsent for next run.
    """
    seen = set(state.get(site_name, []))
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
    processed = 0

    for ann in new_items:
        # 1) If forbidden -> mark seen and skip (does not count against MAX_NEW_PER_RUN)
        if forbidden_hit(ann):
            seen.add(ann.key)
            changed = True
            processed += 1
            continue

        # 2) If we already hit the send cap, stop here (leave remaining as "new" for next run)
        if sent >= MAX_NEW_PER_RUN:
            break

        # 3) Format (may enrich and decide to skip -> returns None/"")
        msg = None
        try:
            msg = format_fn(ann)
        except Exception as e:
            print(f"[{site_name}] format error: {e}")

        if not msg:
            # Mark as seen so it doesn't re-trigger (inactive, filtered-after-enrichment, etc.)
            seen.add(ann.key)
            changed = True
            processed += 1
            continue

        # 4) Send
        try:
            send_telegram(bot_token, chat_id, msg)
            sent += 1
            # Mark as seen only after a successful send (so it retries if Telegram fails)
            seen.add(ann.key)
            changed = True
            processed += 1
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
    # cofim uses ?page=N
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

        # Filter sold (banner-sold image)
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
    # try “browser-ish” headers if needed
    html = http_get(
        session,
        url,
        extra_headers={
            "Upgrade-Insecure-Requests": "1",
            "Referer": COFIM_BASE,
        },
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
    """
    Their pagination is 0-based: ?page=0 for first page.
    We'll map page(1)->0, page(2)->1, ...
    """
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
# SITE 4 — overnamemarkt.be
# =============================================================================
SITE4 = "overnamemarkt"
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

    # cards are links to /fr/kopen/...
    for a in soup.select('a[href*="/fr/kopen/"]'):
        href = a.get("href", "")
        if not href:
            continue
        if "/fr/kopen/" not in href:
            continue

        url = normalize_url(absolute_url(OV_BASE, href))

        # title
        title_el = a.select_one('span[class*="text-h4"]')
        title = text_clean(title_el.get_text(" ", strip=True)) if title_el else ""
        if not title:
            # fallback: first span
            sp = a.find("span")
            title = text_clean(sp.get_text(" ", strip=True)) if sp else ""
        if not title:
            title = "(sans titre)"

        # location (via map-marker icon)
        location = ""
        icon = a.select_one('i[class*="fa-map-marker"]')
        if icon and icon.parent:
            spans = icon.parent.find_all("span")
            if spans:
                location = text_clean(spans[-1].get_text(" ", strip=True))

        price = _find_value_by_label_in_anchor(a, "Prix")
        turnover = _find_value_by_label_in_anchor(a, "Chiffre d'affaires") or _find_value_by_label_in_anchor(a, "Chiffre d’affaires")

        # sector badge (best effort)
        sector = ""
        for sp in a.find_all("span"):
            cls = " ".join(sp.get("class", [])).lower()
            if "inline-flex" in cls and sp.find("i") and "fa" in " ".join(sp.find("i").get("class", [])).lower():
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

        out.append(Announcement(SITE4, title, url, meta))

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
# SITE 6 — cessionpro.be (Webflow/Jetboost-ish)
# =============================================================================
SITE6 = "cessionpro"
C6_BASE = "https://www.cessionpro.be"


def is_new_c6_item(item: BeautifulSoup) -> bool:
    # Prefer explicit badge class (as in your TS)
    for el in item.select(".text-block-162, .badge, .label"):
        if norm_cmp(el.get_text(" ", strip=True)) == "nouveau":
            return True
    # fallback: any text containing "nouveau"
    return "nouveau" in norm_cmp(item.get_text(" ", strip=True))


def parse_c6_item(item: BeautifulSoup) -> Optional[Announcement]:
    # URL (button to detail)
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

    # price + turnover appear in multiple text-block-116
    blocks = [text_clean(x.get_text(" ", strip=True)) for x in item.select(".jobcard1 .date-4 .text-block-116")]
    price = blocks[0] if blocks else ""
    turnover = blocks[1] if len(blocks) >= 2 else ""

    sector = pick(".jobcard1 .div-block-196 .text-block-145") or pick(".div-block-196 .text-block-145")

    # Optional deeper fields
    reference = pick(".secondcard .text-block-163")
    description = pick(".secondcard .rich-text-block-annonce") or text_clean(item.select_one(".secondcard").get_text(" ", strip=True)) if item.select_one(".secondcard") else ""

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

    # Optional sector focus
    if CESSIONPRO_ALLOWED_SECTORS:
        if sector and sector not in CESSIONPRO_ALLOWED_SECTORS:
            return None

    return Announcement(SITE6, title, url, meta)


def parse_cessionpro_listing(html: str, only_new: bool = True) -> List[Announcement]:
    soup = soupify(html)

    items = soup.select('div[role="listitem"].collectionitem.w-dyn-item')
    if not items:
        # fallback: webflow dyn item class only
        items = soup.select("div.w-dyn-item")

    out: List[Announcement] = []
    for el in items:
        if only_new and not is_new_c6_item(el):
            continue
        ann = parse_c6_item(el)
        if ann:
            out.append(ann)

    # De-dupe by URL
    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def fetch_cessionpro(session: requests.Session, page: int) -> List[Announcement]:
    # No pagination used here; page ignored
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
        lines.append(f"Contact: {ann.meta.get('email','')}{' ' if ann.meta.get('email') and ann.meta.get('phone') else ''}{ann.meta.get('phone','')}".strip())
    if ann.meta.get("description"):
        d = ann.meta["description"]
        if len(d) > 280:
            d = d[:280] + "…"
        lines.append(f"Résumé: {d}")
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# SITE 5 — bedrijventekoop.be (POST /listings-post + detail enrichment)
# =============================================================================
SITE5 = "bedrijventekoop"
BTK_BASE = "https://www.bedrijventekoop.be"


def btk_extract_token(html: str) -> str:
    soup = soupify(html)
    meta = soup.select_one('meta[name="csrf-token"], meta[name="csrf_token"], meta[name="xsrf-token"]')
    if meta and meta.get("content"):
        return meta["content"].strip()
    return ""


def btk_guess_webspace_locale(html: str) -> Tuple[str, str]:
    webspace = re.search(r'"webspace"\s*:\s*"([^"]+)"', html)
    locale = re.search(r'"locale"\s*:\s*"([^"]+)"', html)
    return (webspace.group(1) if webspace else "btkbe", locale.group(1) if locale else "nl_be")


def btk_guess_post_url(html: str) -> str:
    # try to find "/listings-post" in HTML/inline scripts
    m = re.search(r'(["\'])\/listings-post\1', html)
    if m:
        return "/listings-post"
    # default
    return "/listings-post"


def parse_btk_list_fragment(html_fragment: str) -> List[Announcement]:
    soup = soupify(html_fragment)
    out: List[Announcement] = []

    # known detail patterns:
    # /te-koop-aangeboden/<id>/<slug>
    # /for-sale/<id>/<slug>
    for a in soup.select('a[href*="/te-koop-aangeboden/"], a[href*="/for-sale/"]'):
        href = a.get("href", "").strip()
        if not href:
            continue
        if "/te-koop-aangeboden/" not in href and "/for-sale/" not in href:
            continue

        url = normalize_url(absolute_url(BTK_BASE, href))
        title = text_clean(a.get_text(" ", strip=True)) or "(title pending)"
        out.append(Announcement(SITE5, title, url, {}))

    # De-dupe by URL
    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def parse_btk_detail(html: str) -> Dict[str, str]:
    soup = soupify(html)
    meta: Dict[str, str] = {}

    # title
    h1 = soup.find("h1")
    if h1:
        meta["title"] = text_clean(h1.get_text(" ", strip=True))

    # sold/inactive detection
    page_text = norm_cmp(soup.get_text(" ", strip=True))
    if "verkocht" in page_text or "inactief" in page_text:
        meta["inactive"] = "true"

    # Try dl/dt/dd extraction
    def dl_value(label: str) -> str:
        for dt in soup.find_all("dt"):
            if norm_cmp(dt.get_text(" ", strip=True)) == norm_cmp(label):
                dd = dt.find_next_sibling("dd")
                if dd:
                    return text_clean(dd.get_text(" ", strip=True))
        return ""

    # Fallback: headings + next text blocks
    def loose_value(label: str) -> str:
        # find exact label node and take next non-empty text
        for node in soup.find_all(string=True):
            if norm_cmp(text_clean(node)) == norm_cmp(label):
                cur = node.parent
                nxt = cur.find_next(string=True)
                # skip same label
                while nxt and norm_cmp(text_clean(nxt)) in ("", norm_cmp(label)):
                    nxt = BeautifulSoup(str(nxt), "lxml").string  # safe no-op
                    break
        return ""

    meta["region"] = dl_value("Regio") or ""
    meta["branche"] = dl_value("Branche") or ""
    meta["omzet"] = dl_value("Omzet") or dl_value("Omzet indicatie") or ""
    meta["overname"] = dl_value("Overname") or dl_value("Indicatie overnamebedrag") or ""
    meta["resultaat"] = dl_value("Resultaat voor belasting") or ""

    # Best-effort “Aangeboden sinds …”
    m = re.search(r"Aangeboden sinds\s+([0-9]{1,2}\s+\w+\s+[0-9]{4})", soup.get_text(" ", strip=True), flags=re.I)
    if m:
        meta["listed_since"] = m.group(1)

    # Clean empties
    return {k: v for k, v in meta.items() if v}


def fetch_bedrijventekoop(session: requests.Session, page: int) -> List[Announcement]:
    # 1) GET the filtered listing page (your URL5)
    landing_url = URL5_BEDRIJVENTEKOOP
    landing_html = http_get(session, landing_url, extra_headers={"Referer": BTK_BASE})

    token = btk_extract_token(landing_html)
    webspace, locale = btk_guess_webspace_locale(landing_html)
    post_path = btk_guess_post_url(landing_html)
    post_url = absolute_url(BTK_BASE, post_path)

    # filters from URL
    p = urlparse(URL5_BEDRIJVENTEKOOP)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    sectors_csv = q.get("sectors", "")
    regions_csv = q.get("regions", "")

    # 2) Try multiple payloads (form-encoded)
    candidates: List[Dict[str, str]] = []

    # A: basic page
    candidates.append({"page": str(page), "webspace": webspace, "locale": locale})

    # B: alternative pagination key
    candidates.append({"p": str(page), "webspace": webspace, "locale": locale})

    # C: JSON payload style
    filter_obj = {
        "q": "",
        "sectors": sectors_csv.split(",") if sectors_csv else [],
        "regions": [int(x) for x in regions_csv.split(",") if x.isdigit()] if regions_csv else [],
        "archived": False,
        "purposeType": 1,
        "page": page,
    }
    candidates.append({"webspace": webspace, "locale": locale, "payload": json.dumps(filter_obj)})

    # D: offset/limit
    candidates.append({"webspace": webspace, "locale": locale, "offset": str((page - 1) * 20), "limit": "20"})

    extra_headers = {}
    if token:
        extra_headers["X-CSRF-TOKEN"] = token

    body = None
    for data in candidates:
        try:
            resp = http_post_form(session, post_url, data=data, referer=landing_url, extra_headers=extra_headers)
        except Exception:
            continue

        # accept if it looks like it contains detail links
        if "/te-koop-aangeboden/" in resp or "/for-sale/" in resp:
            body = resp
            break

        # sometimes JSON with html inside
        try:
            j = json.loads(resp)
            if isinstance(j, dict):
                blob = ""
                for v in j.values():
                    if isinstance(v, str) and ("/te-koop-aangeboden/" in v or "/for-sale/" in v):
                        blob = v
                        break
                if blob:
                    body = blob
                    break
        except Exception:
            pass

    if body is None:
        raise RuntimeError(
            "site5: POST /listings-post accepted none of the candidate payloads. "
            "Open DevTools > Network on the listing page and copy the exact XHR payload to lock it in."
        )

    return parse_btk_list_fragment(body)


def format_bedrijventekoop(ann: Announcement) -> Optional[str]:
    # Enrich by fetching detail page (only when sending)
    meta = dict(ann.meta or {})
    title = ann.title

    try:
        html = http_get(_GLOBAL_SESSION, ann.url, extra_headers={"Referer": BTK_BASE})
        d = parse_btk_detail(html)

        # Skip inactive/sold listings entirely (and mark seen in runner because we return None)
        if d.get("inactive") == "true":
            return None

        if d.get("title"):
            title = d["title"]
        meta.update(d)
    except Exception as e:
        print(f"[site5] detail enrichment failed: {e}")

    # IMPORTANT: re-run forbidden filter AFTER enrichment (branche/region/etc now available)
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



# =============================================================================
# SITE 3 — WE Transmission (SPA best-effort discovery)
# =============================================================================
SITE3 = "we-transmission"
WE_BASE = "https://transmission.wallonie-entreprendre.be"


def we_discover_bundle_urls(html: str) -> List[str]:
    soup = soupify(html)
    urls: List[str] = []
    # Vite often uses <script type="module" src="/assets/index-....js">
    for s in soup.select('script[type="module"][src]'):
        src = s.get("src", "")
        if src and src.endswith(".js"):
            urls.append(absolute_url(WE_BASE, src))
    # fallback: any script /assets/*.js
    for s in soup.select("script[src]"):
        src = s.get("src", "")
        if src and "/assets/" in src and src.endswith(".js"):
            urls.append(absolute_url(WE_BASE, src))
    # de-dupe
    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def we_extract_candidate_endpoints(js: str) -> List[str]:
    endpoints = set()

    # absolute URLs in bundle
    for m in re.finditer(r"https?://[a-z0-9\.\-]+\.[a-z]{2,}(?:/[^\s\"'`<]*)?", js, flags=re.I):
        u = m.group(0)
        if "wallonie" in u or "entreprendre" in u or "transmission" in u:
            endpoints.add(u)

    # relative endpoints
    for m in re.finditer(r'(["\'`])/(api|graphql|actions)/[a-z0-9/_\-]+(?:\?[^"\'`]*)?\1', js, flags=re.I):
        endpoints.add(m.group(0)[1:-1])

    for m in re.finditer(r'fetch\(\s*(["\'`])([^"\'`]+)\1', js, flags=re.I):
        p = m.group(2)
        if p.startswith("/api/") or p.startswith("/graphql") or p.startswith("/actions/"):
            endpoints.add(p)

    # rank with keywords
    kws = ["catalogue", "annonce", "offre", "cession", "listing", "search", "filter"]
    ranked = sorted(
        endpoints,
        key=lambda e: sum(1 for k in kws if k in norm_cmp(e)),
        reverse=True,
    )
    return ranked


def we_json_probe(session: requests.Session, endpoint: str) -> Optional[List[Dict]]:
    url = endpoint if endpoint.startswith("http") else absolute_url(WE_BASE, endpoint)
    try:
        r = session.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json,text/plain,*/*",
                "Referer": WE_BASE,
            },
            timeout=REQUEST_TIMEOUT,
        )
        if not r.ok:
            return None
        data = r.json()
    except Exception:
        return None

    # Normalize into list of dict objects
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data

    if isinstance(data, dict):
        # pick first list value that looks like listings
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return None


def fetch_we_transmission(session: requests.Session, page: int) -> List[Announcement]:
    # page ignored; discovery + probe
    html = http_get(session, URL3_WE, extra_headers={"Referer": WE_BASE})
    bundle_urls = we_discover_bundle_urls(html)
    if not bundle_urls:
        print("[site3] no JS bundle URLs discovered (SPA).")
        return []

    endpoints: List[str] = []
    for b in bundle_urls[:3]:  # don’t over-fetch
        try:
            js = http_get(session, b, extra_headers={"Referer": WE_BASE})
        except Exception:
            continue
        endpoints.extend(we_extract_candidate_endpoints(js))

    # de-dupe
    seen = set()
    endpoints = [e for e in endpoints if not (e in seen or seen.add(e))]

    # probe candidates
    for ep in endpoints[:40]:
        if "graphql" in norm_cmp(ep):
            continue  # skip GraphQL without schema knowledge
        listings = we_json_probe(session, ep)
        if not listings:
            continue

        out: List[Announcement] = []
        for it in listings[:200]:
            title = text_clean(str(it.get("title") or it.get("name") or it.get("titre") or "")) or "(sans titre)"
            u = str(it.get("url") or it.get("link") or "")
            if u:
                url = normalize_url(u if u.startswith("http") else absolute_url(WE_BASE, u))
            else:
                # last resort: try slug/id
                slug = it.get("slug") or it.get("id")
                if slug:
                    url = normalize_url(absolute_url(WE_BASE, f"/catalogue/{slug}"))
                else:
                    continue

            meta = {}
            for k in ["sector", "secteur", "location", "region", "price", "prix"]:
                if it.get(k):
                    meta[k] = text_clean(str(it.get(k)))
            out.append(Announcement(SITE3, title, url, meta))

        if out:
            print(f"[site3] using endpoint: {ep}")
            return out

    # Nothing usable found
    print("[site3] SPA detected; no usable JSON endpoint found. Endpoints discovered (first 20):")
    for ep in endpoints[:20]:
        print("  -", ep)
    return []


def format_we(ann: Announcement) -> str:
    lines = [f"[WE Transmission] {ann.title}"]
    # best-effort meta
    loc = ann.meta.get("location") or ann.meta.get("region") or ""
    if loc:
        lines.append(f"Localisation: {loc}")
    sec = ann.meta.get("sector") or ann.meta.get("secteur") or ""
    if sec:
        lines.append(f"Secteur: {sec}")
    pr = ann.meta.get("price") or ann.meta.get("prix") or ""
    if pr:
        lines.append(f"Prix: {pr}")
    lines.append(ann.url)
    return "\n".join(lines)


# =============================================================================
# Main
# =============================================================================
_GLOBAL_SESSION = make_session()


def main() -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    state = load_state()
    any_changed = False

    sites = [
        (COFIM_SITE, fetch_cofim, format_cofim, COFIM_MAX_PAGES),
        (CAR_SITE, fetch_car, format_car, CAR_MAX_PAGES),
        (SITE3, fetch_we_transmission, format_we, WE_MAX_PAGES),
        (SITE4, fetch_overnamemarkt, format_overnamemarkt, OVERNAMEMARKT_MAX_PAGES),
        (SITE5, fetch_bedrijventekoop, format_bedrijventekoop, BEDRIJVENTEKOOP_MAX_PAGES),
        (SITE6, fetch_cessionpro, format_cessionpro, CESSIONPRO_MAX_PAGES),
    ]

    for (site_name, fetch_fn, format_fn, max_pages) in sites:
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

    if any_changed:
        save_state(state)


if __name__ == "__main__":
    main()
