# acquisitions_alerts.py
# COFIM + commerce-a-remettre: scrapes listings, sends ONLY new announcements since last run (state file), via Telegram.
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
from dataclasses import dataclass
from typing import List, Dict, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (AcquisitionAnnouncementsBot; +https://github.com/)"
STATE_FILE = "seen_announcements_by_site.json"
REQUEST_TIMEOUT = 35
SLEEP_BETWEEN_PAGES_SEC = 1.0
MAX_NEW_PER_RUN = 40  # cap to avoid spam/long runs

# ----------------------------
# USER FILTER (editable)
# ----------------------------
# Any announcement whose title/location/teaser/type contains ANY of these words
# (case-insensitive, substring match) will be discarded.
FORBIDDEN_WORDS = [
    # examples:
    # "horeca",
    # "restaurant",
]


# ----------------------------
# COFIM
# ----------------------------
COFIM_SITE = "cofim"
COFIM_BASE = "https://www.cofim.be"
COFIM_LISTING_URL = "https://www.cofim.be/fr/entreprises/entreprises-fonds-de-commerce"
COFIM_MAX_PAGES = 3  # scan first N pages


# ----------------------------
# Commerce-a-remettre
# ----------------------------
CAR_SITE = "commerce-a-remettre"
CAR_BASE = "https://www.commerce-a-remettre.be"
CAR_LISTING_URL = "https://www.commerce-a-remettre.be/recherche?region=&sector=&id="
CAR_MAX_PAGES = 3  # scan first N pages (page param is 0-based in their UI)


@dataclass
class Announcement:
    site: str
    title: str
    url: str
    meta: Dict[str, str]

    @property
    def key(self) -> str:
        # Prefer stable slug/id from URL when possible
        path = urlparse(self.url).path.rstrip("/")
        if path:
            tail = path.split("/")[-1]
            if tail:
                return f"{self.site}:{tail}"
        raw = (self.url or "") + "||" + (self.title or "")
        return f"{self.site}:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def http_get(url: str) -> str:
    r = requests.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "fr,en;q=0.8,nl;q=0.6",
        },
        timeout=REQUEST_TIMEOUT,
    )
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
    Keep functional query params (page, region, sector, id).
    """
    try:
        p = urlparse(u)
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
        new_query = urlencode(q, doseq=True)
        p2 = p._replace(query=new_query, fragment="")
        return urlunparse(p2)
    except Exception:
        return u


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


def send_telegram(bot_token: str, chat_id: str, message: str) -> None:
    if not bot_token or not chat_id:
        print("Telegram not configured; skipping send.")
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "disable_web_page_preview": True}
    rr = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    if not rr.ok:
        print("Telegram send failed:", rr.text)


def forbidden_hit(ann: Announcement) -> str:
    """
    Returns the forbidden word that matched, or "" if none.
    Checks title + selected meta fields.
    """
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
        ]
    ).lower()
    for w in words:
        if w in hay:
            return w
    return ""


# ----------------------------
# COFIM parsing
# ----------------------------
def build_cofim_page_url(page: int) -> str:
    if page <= 1:
        return COFIM_LISTING_URL
    return f"{COFIM_LISTING_URL}?page={page}"


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

        # Title + location
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

        ann = Announcement(COFIM_SITE, title, url, meta)
        if forbidden_hit(ann):
            continue

        out.append(ann)

    # Dedup by URL
    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def format_cofim_message(ann: Announcement) -> str:
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


# ----------------------------
# Commerce-a-remettre parsing
# ----------------------------
def build_car_page_url(page: int) -> str:
    """
    Their pagination in HTML uses ?page=0 for first page.
    We'll append/override ?page=page while keeping base filters (region/sector/id).
    """
    base = CAR_LISTING_URL
    p = urlparse(base)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q["page"] = str(page)
    new_query = urlencode(q, doseq=True)
    return urlunparse(p._replace(query=new_query, fragment=""))


def parse_car_listing(html: str) -> List[Announcement]:
    """
    From provided HTML:
      each result has .search-result-item-container
      inside: .search-result-item
        - title: h3.result-title a
        - url: the same a[href] or meta[itemprop=url]
        - location: ul.result-info li with .icon-map-marker
        - type/category: ul.result-info li containing "Type :"
        - price: meta[itemprop=price] (optional, but present)
        - teaser: .result-description p
    """
    soup = soupify(html)
    out: List[Announcement] = []

    for item in soup.select(".search-result-item"):
        # URL
        a = item.select_one("h3.result-title a[href]")
        href = a.get("href", "") if a else ""
        if not href:
            meta_url = item.select_one("meta[itemprop='url']")
            href = meta_url.get("content", "") if meta_url else ""
        url = normalize_url(absolute_url(CAR_BASE, href))
        if not url:
            continue

        # Title
        title = ""
        if a:
            title = text_clean(a.get_text(" "))
        if not title:
            meta_name = item.select_one("meta[itemprop='name']")
            title = text_clean(meta_name.get("content", "")) if meta_name else ""
        if not title or len(title) < 3:
            continue

        # Price
        price = ""
        meta_price = item.select_one("meta[itemprop='price']")
        if meta_price and meta_price.get("content"):
            try:
                # keep raw numeric as EUR
                price = f"{int(float(meta_price['content']))} EUR"
            except Exception:
                price = text_clean(meta_price.get("content", ""))

        # Location + Type
        location = ""
        typ = ""
        info_lis = item.select("ul.result-info li")
        for li in info_lis:
            txt = text_clean(li.get_text(" "))
            if "icon-map-marker" in (li.decode().lower()):
                # best effort: pick the <a> text if available
                loc_a = li.select_one("a")
                location = text_clean(loc_a.get_text(" ")) if loc_a else ""
            if "type" in txt.lower():
                # often: "Type : Vente & commerce"
                strong = li.select_one("strong")
                typ = text_clean(strong.get_text(" ")) if strong else ""
                if not typ:
                    # fallback: take after colon
                    m = re.search(r"type\s*:\s*(.+)$", txt, flags=re.I)
                    if m:
                        typ = text_clean(m.group(1))

        # Teaser/description
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

        ann = Announcement(CAR_SITE, title, url, meta)
        if forbidden_hit(ann):
            continue

        out.append(ann)

    # Dedup by URL
    uniq: Dict[str, Announcement] = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())


def format_car_message(ann: Announcement) -> str:
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


# ----------------------------
# Runner per site
# ----------------------------
def run_site(
    site_name: str,
    build_page_url_fn,
    parse_listing_fn,
    format_message_fn,
    max_pages: int,
    state: Dict[str, List[str]],
    bot_token: str,
    chat_id: str,
) -> Tuple[int, int, bool]:
    """
    Returns: (new_detected, alerts_sent, changed_state)
    """
    seen = set(state.get(site_name, []))
    changed = False

    all_items: List[Announcement] = []
    for page in range(0, max_pages):
        # COFIM uses 1-based pages; CAR uses 0-based. We pass whatever builder expects.
        url = build_page_url_fn(page if site_name == CAR_SITE else page + 1)
        try:
            html = http_get(url)
        except Exception as e:
            print(f"[{site_name}] fetch error page={page}:", e)
            time.sleep(SLEEP_BETWEEN_PAGES_SEC)
            continue

        try:
            items = parse_listing_fn(html)
            print(f"[{site_name}] page {page}: {len(items)} items kept after filters")
            all_items.extend(items)
        except Exception as e:
            print(f"[{site_name}] parse error page={page}:", e)

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

    # Deterministic order
    new_items.sort(key=lambda x: x.url)

    sent = 0
    for ann in new_items[:MAX_NEW_PER_RUN]:
        msg = format_message_fn(ann)
        try:
            send_telegram(bot_token, chat_id, msg)
            sent += 1
        except Exception as e:
            print(f"[{site_name}] telegram send error:", e)

        seen.add(ann.key)
        changed = True
        time.sleep(0.5)

    state[site_name] = list(seen)
    return (len(new_items), sent, changed)


def main() -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    state = load_state()
    any_changed = False

    # COFIM
    new_detected, sent, changed = run_site(
        site_name=COFIM_SITE,
        build_page_url_fn=build_cofim_page_url,
        parse_listing_fn=parse_cofim_listing,
        format_message_fn=format_cofim_message,
        max_pages=COFIM_MAX_PAGES,
        state=state,
        bot_token=bot_token,
        chat_id=chat_id,
    )
    any_changed = any_changed or changed
    print(f"[cofim] new_detected={new_detected} sent={sent}")

    # Commerce-a-remettre
    new_detected, sent, changed = run_site(
        site_name=CAR_SITE,
        build_page_url_fn=build_car_page_url,
        parse_listing_fn=parse_car_listing,
        format_message_fn=format_car_message,
        max_pages=CAR_MAX_PAGES,
        state=state,
        bot_token=bot_token,
        chat_id=chat_id,
    )
    any_changed = any_changed or changed
    print(f"[commerce-a-remettre] new_detected={new_detected} sent={sent}")

    if any_changed:
        save_state(state)


if __name__ == "__main__":
    main()
