# acquisitions_alerts.py
# Scrapes multiple “business for sale / acquisition” websites and sends ONLY new announcements via Telegram.
# Remembers last seen announcements per website in a local JSON state file.
#
# Install deps:
#   pip install requests beautifulsoup4 lxml
#
# Env vars (same pattern as your other bot):
#   TELEGRAM_BOT_TOKEN=...
#   TELEGRAM_CHAT_ID=...
#
# Run:
#   python acquisitions_alerts.py
#
# Notes:
# - This is a best-effort scraper. These sites can change HTML; when they do, update the CSS selectors below.
# - commerce-a-remettre.be: will IGNORE announcements where "type" == "horeca" (case-insensitive).

import os
import re
import json
import time
import hashlib
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (AcquisitionAnnouncementsBot; +https://github.com/)"
STATE_FILE = "seen_announcements_by_site.json"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_SITES_SEC = 1.0

# ----------------------------
# Sites (as provided)
# ----------------------------
URL_COFIM = "https://www.cofim.be/fr/entreprises/entreprises-fonds-de-commerce"
URL_COMMERCE_A_REMETTRE = "https://www.commerce-a-remettre.be/recherche?region=&sector=&id="
URL_WALLONIE_ENTREPRENDRE = (
    "https://transmission.wallonie-entreprendre.be/catalogue?"
    "sectors=%5B%22Agriculture%2C+horticulture%2C+aquaculture%22%2C%22Industrie+automobile%22%2C%22Construction%22%2C"
    "%22ICT%22%2C%22Edition+et+Imprimerie%22%2C%22Industrie+extractive%22%2C%22Industrie+manufacturi%C3%A8re%22%2C"
    "%22M%C3%A9tallurgie%22%2C%22Commerce+de+gros%22%2C%22Services%22%5D"
)
URL_OVERNAMEMARKT = "https://www.overnamemarkt.be/fr/acheter?sectors=andere,bouw,diensten,industrie,distributie,vrije-beroepen"
URL_BEDRIJVENTEKOOP = (
    "https://www.bedrijventekoop.be/te-koop-aangeboden?"
    "sectors=2,4,12,2_216,2_217,2_218,2_83,2_222,2_219,2_86,2_94,2_93,2_91,2_220,2_221,2_87,2_92,2_233,2_89,"
    "2_85,2_223,2_99,2_224,2_225,2_90,2_84,4_12,4_13,4_15,4_14,4_101,8_51,8_62,8_53,8_59,8_64,8_230,8_242,8_54,"
    "8_204,8_67,8_229,8_60,8_215,8_66,8_105,8_52,8_49,8_56,8_234,8_88,8_241,8_65,8_228,8_55,12_208,12_61,12_206,"
    "12_108,12_207,12_205&regions=26,27,28,29,30,31,32,33,34,35,36,37,38,39"
)
URL_CESSIONPRO = (
    "https://www.cessionpro.be/?utm_source=chatgpt.com&secteurs-v4oj=entreprise-de-construction-a-remettre-en-belgique%7C"
    "e-commerce-a-vendre-en-belgique%7Csociete-industrielle-a-vendre-en-belgique%7Crecherche%7Csociete-de-service-a-vendre-"
    "en-belgique&regions-be=belgique"
)

# ----------------------------
# Data model
# ----------------------------
@dataclass
class Announcement:
    site: str
    title: str
    url: str
    meta: Dict[str, str]  # arbitrary fields, e.g. {"type": "...", "location": "...", "price": "..."}

    @property
    def key(self) -> str:
        # Stable-ish unique key per announcement (prefer URL; fallback to hash(title+url)).
        raw = (self.url or "") + "||" + (self.title or "")
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ----------------------------
# Helpers
# ----------------------------
def http_get(url: str) -> str:
    r = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "fr,en;q=0.8,nl;q=0.6"},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.text


def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def text_clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def first_text(el) -> str:
    return text_clean(el.get_text(" ")) if el else ""


def absolute_url(base: str, href: str) -> str:
    return urljoin(base, href) if href else ""


def load_state() -> Dict[str, List[str]]:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                # ensure list values
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
    payload = {"chat_id": chat_id, "text": message}
    try:
        rr = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        if not rr.ok:
            print("Telegram send failed:", rr.text)
    except Exception as e:
        print("Telegram send exception:", e)


# ----------------------------
# Parsers (best-effort CSS selectors with fallbacks)
# ----------------------------
def parse_cofim(html: str) -> List[Announcement]:
    base = "https://www.cofim.be"
    soup = soupify(html)

    # Try common patterns: cards with links to company pages
    links = soup.select("a[href]")
    out = []
    for a in links:
        href = a.get("href", "")
        # heuristic: keep links under /fr/entreprises/ and not the listing page itself
        if "/fr/entreprises/" in href and "entreprises-fonds-de-commerce" not in href:
            title = first_text(a)
            if not title or len(title) < 5:
                continue
            url = absolute_url(base, href)
            out.append(Announcement("cofim", title, url, {}))

    # De-duplicate by URL
    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:100]


def parse_commerce_a_remettre(html: str) -> List[Announcement]:
    base = "https://www.commerce-a-remettre.be"
    soup = soupify(html)

    out: List[Announcement] = []

    # Typical result cards; fallback: any link that looks like an announcement/details page
    cards = soup.select("[class*='result'], [class*='card'], article, .annonce, .listing")
    if not cards:
        cards = soup.select("a[href*='annonce'], a[href*='detail'], a[href*='reprise'], a[href*='commerce']")
        cards = [a.parent for a in cards if a.parent]

    for card in cards:
        # find main link
        a = card.select_one("a[href]")
        if not a:
            continue
        href = a.get("href", "")
        url = absolute_url(base, href)
        title = first_text(a) or first_text(card.select_one("h2, h3, .title, [class*='title']"))

        if not title or len(title) < 5:
            continue

        # find "type" field somewhere in the card (labelled "Type" or similar)
        card_text = text_clean(card.get_text(" "))
        type_val = ""
        m = re.search(r"\btype\b\s*[:\-]?\s*([A-Za-zÀ-ÿ0-9\/\s]+)", card_text, flags=re.I)
        if m:
            type_val = text_clean(m.group(1))[:60]

        # Exclude horeca
        if type_val and type_val.lower().strip() == "horeca":
            continue

        meta = {}
        if type_val:
            meta["type"] = type_val

        out.append(Announcement("commerce-a-remettre", title, url, meta))

    # De-duplicate by URL
    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:200]


def parse_wallonie_entreprendre(html: str) -> List[Announcement]:
    base = "https://transmission.wallonie-entreprendre.be"
    soup = soupify(html)

    out = []
    # Try catalogue cards
    cards = soup.select("a[href*='/annonce'], a[href*='/catalogue/'], article, [class*='card'], [class*='listing']")
    for el in cards:
        a = el if el.name == "a" else el.select_one("a[href]")
        if not a:
            continue
        href = a.get("href", "")
        if "/catalogue" not in href and "/annonce" not in href and "/offre" not in href:
            continue
        url = absolute_url(base, href)
        title = first_text(el.select_one("h2, h3, .title, [class*='title']")) or first_text(a)
        if not title or len(title) < 5:
            continue
        out.append(Announcement("wallonie-entreprendre", title, url, {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:200]


def parse_overnamemarkt(html: str) -> List[Announcement]:
    base = "https://www.overnamemarkt.be"
    soup = soupify(html)

    out = []
    cards = soup.select("a[href*='/fr/acheter/'], a[href*='/acheter/'], article, [class*='card'], [class*='listing']")
    for el in cards:
        a = el if el.name == "a" else el.select_one("a[href]")
        if not a:
            continue
        href = a.get("href", "")
        if "/acheter" not in href:
            continue
        url = absolute_url(base, href)
        title = first_text(el.select_one("h2, h3, .title, [class*='title']")) or first_text(a)
        if not title or len(title) < 5:
            continue
        out.append(Announcement("overnamemarkt", title, url, {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:200]


def parse_bedrijventekoop(html: str) -> List[Announcement]:
    base = "https://www.bedrijventekoop.be"
    soup = soupify(html)

    out = []
    cards = soup.select("a[href*='/te-koop-aangeboden/'], a[href*='/te-koop/'], article, [class*='card'], [class*='listing']")
    for el in cards:
        a = el if el.name == "a" else el.select_one("a[href]")
        if not a:
            continue
        href = a.get("href", "")
        if "/te-koop" not in href:
            continue
        url = absolute_url(base, href)
        title = first_text(el.select_one("h2, h3, .title, [class*='title']")) or first_text(a)
        if not title or len(title) < 5:
            continue
        out.append(Announcement("bedrijventekoop", title, url, {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:200]


def parse_cessionpro(html: str) -> List[Announcement]:
    base = "https://www.cessionpro.be"
    soup = soupify(html)

    out = []
    cards = soup.select("a[href*='cession'], a[href*='annonce'], a[href*='offre'], article, [class*='card'], [class*='listing']")
    for el in cards:
        a = el if el.name == "a" else el.select_one("a[href]")
        if not a:
            continue
        href = a.get("href", "")
        # keep internal links only
        if href.startswith("http") and "cessionpro.be" not in href:
            continue
        url = absolute_url(base, href)
        title = first_text(el.select_one("h2, h3, .title, [class*='title']")) or first_text(a)
        if not title or len(title) < 5:
            continue
        out.append(Announcement("cessionpro", title, url, {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:200]


# ----------------------------
# Site registry
# ----------------------------
SiteParser = Callable[[str], List[Announcement]]

SITES: List[Tuple[str, str, SiteParser]] = [
    ("cofim", URL_COFIM, parse_cofim),
    ("commerce-a-remettre", URL_COMMERCE_A_REMETTRE, parse_commerce_a_remettre),
    ("wallonie-entreprendre", URL_WALLONIE_ENTREPRENDRE, parse_wallonie_entreprendre),
    ("overnamemarkt", URL_OVERNAMEMARKT, parse_overnamemarkt),
    ("bedrijventekoop", URL_BEDRIJVENTEKOOP, parse_bedrijventekoop),
    ("cessionpro", URL_CESSIONPRO, parse_cessionpro),
]


# ----------------------------
# Main
# ----------------------------
def format_message(items: List[Announcement]) -> str:
    # Compact Telegram message (one site at a time)
    lines = []
    for it in items:
        line = f"{it.title}\n{it.url}"
        if it.meta.get("type"):
            line = f"{it.title} (type: {it.meta['type']})\n{it.url}"
        lines.append(line)
    return "\n\n".join(lines)


def main():
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    state = load_state()  # {site_name: [keys]}
    changed = False

    for site_name, url, parser in SITES:
        try:
            html = http_get(url)
        except Exception as e:
            print(f"[{site_name}] fetch error:", e)
            time.sleep(SLEEP_BETWEEN_SITES_SEC)
            continue

        try:
            announcements = parser(html)
        except Exception as e:
            print(f"[{site_name}] parse error:", e)
            time.sleep(SLEEP_BETWEEN_SITES_SEC)
            continue

        seen_keys = set(state.get(site_name, []))
        new_items = [a for a in announcements if a.key not in seen_keys]

        if new_items:
            # Send only the new ones
            msg = format_message(new_items[:20])  # cap to avoid huge telegram payloads
            send_telegram(bot_token, chat_id, msg)

            # Update state
            for a in new_items:
                seen_keys.add(a.key)
            state[site_name] = list(seen_keys)
            changed = True

            print(f"[{site_name}] new announcements: {len(new_items)}")
        else:
            print(f"[{site_name}] no new announcements")

        time.sleep(SLEEP_BETWEEN_SITES_SEC)

    if changed:
        save_state(state)


if __name__ == "__main__":
    main()
