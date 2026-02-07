# acquisitions_alerts.py
# COFIM-only: scrapes the COFIM "Entreprises / fonds de commerce" listing,
# sends ONLY new announcements since last run (state file), via Telegram.
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
from typing import List, Dict
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (AcquisitionAnnouncementsBot; +https://github.com/)"
STATE_FILE = "seen_announcements_by_site.json"
REQUEST_TIMEOUT = 35
SLEEP_BETWEEN_PAGES_SEC = 1.0
MAX_NEW_PER_RUN = 30  # cap to avoid spam/long runs

# COFIM
COFIM_SITE = "cofim"
COFIM_BASE = "https://www.cofim.be"
COFIM_LISTING_URL = "https://www.cofim.be/fr/entreprises/entreprises-fonds-de-commerce"
COFIM_MAX_PAGES = 3  # scan first N pages


@dataclass
class Announcement:
    site: str
    title: str
    url: str
    meta: Dict[str, str]

    @property
    def key(self) -> str:
        # Prefer a stable ID from URL when possible; fallback to hash(url||title)
        # COFIM URLs often end with "-<id>-<id>" or "-ENT###"
        m = re.search(r"-([A-Za-z0-9]+(?:-[A-Za-z0-9]+)?)$", self.url.rstrip("/"))
        if m:
            return f"{self.site}:{m.group(1)}"
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


def build_cofim_page_url(page: int) -> str:
    # COFIM listing supports ?page=X (as seen in HTML: hidden input name="page")
    # Keep URL stable:
    if page <= 1:
        return COFIM_LISTING_URL
    return f"{COFIM_LISTING_URL}?page={page}"


def parse_cofim_listing(html: str) -> List[Announcement]:
    """
    COFIM listing is server-rendered. Cards exist under #biens-listing.
    We extract:
      - url: a.propertylink[href]
      - title + location: .biens-title h3 (br + span)
      - price: .biens-prix span (optional)
      - teaser: overlay snippet (optional)
      - exclude sold items (img.flag banner-sold-*.png)
    """
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

        # Price (optional)
        price = ""
        price_el = box.select_one(".biens-prix span")
        if price_el:
            price = text_clean(price_el.get_text(" "))

        # Teaser (optional)
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


def format_cofim_message(ann: Announcement) -> str:
    lines = [ann.title]
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


def main() -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    state = load_state()
    seen = set(state.get(COFIM_SITE, []))
    changed = False

    all_items: List[Announcement] = []

    for page in range(1, COFIM_MAX_PAGES + 1):
        url = build_cofim_page_url(page)
        try:
            html = http_get(url)
        except Exception as e:
            print(f"[cofim] fetch error page={page}:", e)
            time.sleep(SLEEP_BETWEEN_PAGES_SEC)
            continue

        try:
            items = parse_cofim_listing(html)
            print(f"[cofim] page {page}: {len(items)} items parsed")
            all_items.extend(items)
        except Exception as e:
            print(f"[cofim] parse error page={page}:", e)

        time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    # Dedup again (multi-page)
    uniq_by_key: Dict[str, Announcement] = {}
    for it in all_items:
        uniq_by_key[it.key] = it
    items = list(uniq_by_key.values())

    new_items = [a for a in items if a.key not in seen]
    if not new_items:
        print("[cofim] no new announcements")
        return

    # Sort new items by URL (stable). If you prefer "most recent first", we need a date/ordering field.
    new_items.sort(key=lambda x: x.url)

    sent = 0
    for ann in new_items[:MAX_NEW_PER_RUN]:
        msg = format_cofim_message(ann)
        try:
            send_telegram(bot_token, chat_id, msg)
            sent += 1
        except Exception as e:
            print("[cofim] telegram send error:", e)

        seen.add(ann.key)
        changed = True
        time.sleep(0.5)

    state[COFIM_SITE] = list(seen)

    if changed:
        save_state(state)

    print(f"[cofim] alerts sent: {sent} (new detected: {len(new_items)})")


if __name__ == "__main__":
    main()
