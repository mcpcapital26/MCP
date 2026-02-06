# acquisitions_alerts.py
# Scrapes acquisition/business-for-sale websites, sends ONLY new announcements via Telegram,
# and enriches alerts by fetching EACH new item's detail page to extract characteristics.
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
from typing import List, Dict, Callable, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (AcquisitionAnnouncementsBot; +https://github.com/)"
STATE_FILE = "seen_announcements_by_site.json"
REQUEST_TIMEOUT = 35
SLEEP_BETWEEN_SITES_SEC = 1.0
SLEEP_BETWEEN_DETAILS_SEC = 0.7
MAX_NEW_PER_SITE_PER_RUN = 15  # cap to avoid spam / long runs

# ----------------------------
# URLs (as provided)
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
    meta: Dict[str, str]  # can include pre-parsed fields from listing page

    @property
    def key(self) -> str:
        # Stable key: URL is usually unique. Include title to reduce collision on some sites.
        raw = (self.url or "") + "||" + (self.title or "")
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ----------------------------
# HTTP / parsing helpers
# ----------------------------
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
    rr = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    if not rr.ok:
        print("Telegram send failed:", rr.text)


# ----------------------------
# Generic field extraction from detail pages
# ----------------------------
def extract_kv_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Best-effort extractor:
      - <dl><dt>Label</dt><dd>Value</dd>
      - tables: <tr><th/td>Label</th/td><td>Value</td></tr>
      - 'Label: Value' patterns from text blocks
    Returns raw label->value pairs.
    """
    kv: Dict[str, str] = {}

    # 1) Definition lists
    for dl in soup.select("dl"):
        dts = dl.select("dt")
        for dt in dts:
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            k = first_text(dt)
            v = first_text(dd)
            if k and v and len(k) <= 80 and len(v) <= 400:
                kv[k] = v

    # 2) Tables (two columns)
    for tr in soup.select("table tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        k = first_text(cells[0])
        v = first_text(cells[1])
        if k and v and len(k) <= 80 and len(v) <= 400:
            kv[k] = v

    # 3) Label-value patterns in text
    text_blocks = []
    for sel in ["main", "article", ".content", ".container", "body"]:
        node = soup.select_one(sel)
        if node:
            text_blocks.append(first_text(node))
            break
    full_text = "\n".join([t for t in text_blocks if t])

    # Match lines like "Type : Horeca" / "Secteur - Services"
    for line in full_text.splitlines():
        line = text_clean(line)
        if not line or len(line) > 250:
            continue
        m = re.match(r"^([A-Za-zÀ-ÿ0-9 \/\-\(\)\.]{2,50})\s*[:\-]\s*(.{2,160})$", line)
        if m:
            k = text_clean(m.group(1))
            v = text_clean(m.group(2))
            if k and v:
                # avoid trash values that are just punctuation
                if re.fullmatch(r"[\-\:\.\/ ]+", v):
                    continue
                kv.setdefault(k, v)

    return kv


CANONICAL_KEYS = {
    "type": [
        "type", "categorie", "catégorie", "category", "activiteiten", "activiteit", "activité",
        "branche", "branch", "soort"
    ],
    "sector": [
        "secteur", "sector", "activité", "activiteiten", "branche", "industrie", "industry"
    ],
    "location": [
        "lieu", "localisation", "ville", "city", "gemeente", "plaats", "région", "region",
        "province", "plaatsnaam", "code postal", "postcode"
    ],
    "price": [
        "prix", "price", "vraagprijs", "asking price", "overnameprijs", "cession", "prijs"
    ],
    "turnover": [
        "chiffre d'affaires", "ca", "omzet", "turnover", "revenue"
    ],
    "ebitda": [
        "ebitda", "résultat", "resultaat", "profit", "bénéfice", "marge"
    ],
    "employees": [
        "employés", "employes", "employees", "personeel", "fte", "staff", "nombre d'employés"
    ],
    "reference": [
        "référence", "reference", "ref", "code", "dossier", "nummer", "id"
    ],
    "date": [
        "date", "publication", "publié", "posted", "geplaatst", "ajouté", "added"
    ],
}

def canonicalize(kv: Dict[str, str]) -> Dict[str, str]:
    """
    Map raw label->value pairs into a canonical set, choosing the first best match per key.
    """
    out: Dict[str, str] = {}

    for canon, needles in CANONICAL_KEYS.items():
        for raw_k, raw_v in kv.items():
            lk = raw_k.lower()
            if any(n in lk for n in needles):
                out.setdefault(canon, raw_v)

    return out


def extract_description(soup: BeautifulSoup) -> str:
    """
    Try to extract a short description snippet.
    """
    candidates = []
    for sel in ["meta[name='description']", "meta[property='og:description']"]:
        m = soup.select_one(sel)
        if m and m.get("content"):
            candidates.append(text_clean(m["content"]))

    for sel in ["article p", "main p", ".description", "[class*='description'] p"]:
        p = soup.select_one(sel)
        if p:
            candidates.append(first_text(p))

    for c in candidates:
        c = text_clean(c)
        if c and len(c) >= 40:
            return c[:280] + ("…" if len(c) > 280 else "")
    return ""


def enrich_from_detail(url: str) -> Dict[str, str]:
    """
    Fetch detail page, return canonicalized fields + description snippet.
    """
    html = http_get(url)
    soup = soupify(html)
    kv_raw = extract_kv_pairs(soup)
    canon = canonicalize(kv_raw)
    desc = extract_description(soup)
    if desc:
        canon["description"] = desc
    return canon


# ----------------------------
# Listing parsers (best-effort)
# ----------------------------
def parse_cofim(html: str) -> List[Announcement]:
    base = "https://www.cofim.be"
    soup = soupify(html)
    out: List[Announcement] = []

    # Heuristic: links to individual company pages under /fr/entreprises/
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/fr/entreprises/" in href and "entreprises-fonds-de-commerce" not in href:
            title = first_text(a)
            if not title or len(title) < 5:
                continue
            out.append(Announcement("cofim", title, absolute_url(base, href), {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:200]


def parse_commerce_a_remettre(html: str) -> List[Announcement]:
    base = "https://www.commerce-a-remettre.be"
    soup = soupify(html)
    out: List[Announcement] = []

    # Look for result cards or blocks containing links
    cards = soup.select("[class*='result'], [class*='card'], article, .annonce, .listing")
    if not cards:
        cards = [a.parent for a in soup.select("a[href]") if a.parent]

    for card in cards:
        a = card.select_one("a[href]")
        if not a:
            continue
        href = a.get("href", "")
        url = absolute_url(base, href)
        title = first_text(a) or first_text(card.select_one("h2, h3, .title, [class*='title']"))
        if not title or len(title) < 5:
            continue

        # Try to extract "type" from listing card text
        meta = {}
        card_text = text_clean(card.get_text(" "))
        m = re.search(r"\btype\b\s*[:\-]?\s*([A-Za-zÀ-ÿ0-9\/\s]+)", card_text, flags=re.I)
        if m:
            t = text_clean(m.group(1))[:60]
            if t:
                meta["type"] = t

        out.append(Announcement("commerce-a-remettre", title, url, meta))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:250]


def parse_wallonie_entreprendre(html: str) -> List[Announcement]:
    base = "https://transmission.wallonie-entreprendre.be"
    soup = soupify(html)
    out: List[Announcement] = []

    # Cards / links to catalogue items
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/catalogue" in href or "/annonce" in href or "/offre" in href:
            url = absolute_url(base, href)
            title = first_text(a.select_one("h2, h3, .title, [class*='title']")) or first_text(a)
            if title and len(title) >= 5:
                out.append(Announcement("wallonie-entreprendre", title, url, {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:250]


def parse_overnamemarkt(html: str) -> List[Announcement]:
    base = "https://www.overnamemarkt.be"
    soup = soupify(html)
    out: List[Announcement] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/acheter" in href:
            url = absolute_url(base, href)
            title = first_text(a.select_one("h2, h3, .title, [class*='title']")) or first_text(a)
            if title and len(title) >= 5:
                out.append(Announcement("overnamemarkt", title, url, {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:250]


def parse_bedrijventekoop(html: str) -> List[Announcement]:
    base = "https://www.bedrijventekoop.be"
    soup = soupify(html)
    out: List[Announcement] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/te-koop" in href:
            url = absolute_url(base, href)
            title = first_text(a.select_one("h2, h3, .title, [class*='title']")) or first_text(a)
            if title and len(title) >= 5:
                out.append(Announcement("bedrijventekoop", title, url, {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:250]


def parse_cessionpro(html: str) -> List[Announcement]:
    base = "https://www.cessionpro.be"
    soup = soupify(html)
    out: List[Announcement] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("http") and "cessionpro.be" not in href:
            continue
        # heuristic: detail pages tend to include "cession" or "annonce"
        if any(x in href.lower() for x in ["cession", "annonce", "offre", "a-vendre", "vendre"]):
            url = absolute_url(base, href)
            title = first_text(a.select_one("h2, h3, .title, [class*='title']")) or first_text(a)
            if title and len(title) >= 5:
                out.append(Announcement("cessionpro", title, url, {}))

    uniq = {}
    for x in out:
        uniq[x.url] = x
    return list(uniq.values())[:250]


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
# Message formatting
# ----------------------------
def format_item(ann: Announcement, fields: Dict[str, str]) -> str:
    # Prefer extracted fields; fall back to listing meta where useful (e.g. type)
    merged = dict(fields)
    for k, v in (ann.meta or {}).items():
        merged.setdefault(k, v)

    lines = [ann.title]

    def add(label: str, key: str):
        v = merged.get(key, "")
        if v:
            lines.append(f"{label}: {v}")

    add("Type", "type")
    add("Secteur", "sector")
    add("Localisation", "location")
    add("Prix", "price")
    add("Chiffre d'affaires", "turnover")
    add("EBITDA / Résultat", "ebitda")
    add("Employés", "employees")
    add("Référence", "reference")
    add("Date", "date")
    if merged.get("description"):
        lines.append(f"Description: {merged['description']}")

    lines.append(ann.url)
    return "\n".join(lines)


def is_horeca(type_value: str) -> bool:
    return text_clean(type_value).lower() == "horeca"


# ----------------------------
# Main
# ----------------------------
def main():
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    state = load_state()  # {site_name: [keys]}
    changed = False

    for site_name, list_url, parser in SITES:
        try:
            html = http_get(list_url)
        except Exception as e:
            print(f"[{site_name}] listing fetch error:", e)
            time.sleep(SLEEP_BETWEEN_SITES_SEC)
            continue

        try:
            announcements = parser(html)
        except Exception as e:
            print(f"[{site_name}] listing parse error:", e)
            time.sleep(SLEEP_BETWEEN_SITES_SEC)
            continue

        seen_keys = set(state.get(site_name, []))
        new_items = [a for a in announcements if a.key not in seen_keys]

        if not new_items:
            print(f"[{site_name}] no new announcements")
            time.sleep(SLEEP_BETWEEN_SITES_SEC)
            continue

        sent_count = 0
        print(f"[{site_name}] new announcements detected: {len(new_items)}")

        for ann in new_items[:MAX_NEW_PER_SITE_PER_RUN]:
            # Commerce-a-remettre special rule: skip type horeca
            if site_name == "commerce-a-remettre":
                t_meta = ann.meta.get("type", "")
                if t_meta and is_horeca(t_meta):
                    seen_keys.add(ann.key)
                    changed = True
                    continue

            # Enrich from detail page
            fields = {}
            try:
                fields = enrich_from_detail(ann.url)
            except Exception as e:
                # If detail fetch fails, still send minimal info but include URL
                print(f"[{site_name}] detail fetch/parse error for {ann.url}:", e)
                fields = {}

            # Commerce-a-remettre: also apply horeca filter from detail fields
            if site_name == "commerce-a-remettre":
                t_detail = fields.get("type", "")
                t_meta = ann.meta.get("type", "")
                if (t_detail and is_horeca(t_detail)) or (t_meta and is_horeca(t_meta)):
                    seen_keys.add(ann.key)
                    changed = True
                    continue

            msg = format_item(ann, fields)
            try:
                send_telegram(bot_token, chat_id, msg)
                sent_count += 1
            except Exception as e:
                print(f"[{site_name}] telegram send error:", e)

            seen_keys.add(ann.key)
            changed = True
            time.sleep(SLEEP_BETWEEN_DETAILS_SEC)

        state[site_name] = list(seen_keys)
        if sent_count == 0:
            print(f"[{site_name}] no alerts sent after filters")
        else:
            print(f"[{site_name}] alerts sent: {sent_count}")

        time.sleep(SLEEP_BETWEEN_SITES_SEC)

    if changed:
        save_state(state)


if __name__ == "__main__":
    main()
