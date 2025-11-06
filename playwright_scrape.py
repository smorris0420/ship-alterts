#!/usr/bin/env python3
# Hybrid scraper:
# - VesselFinder "Recent Port Calls" (JS-rendered via Playwright)
# - PLUS geofencing for private islands using live coordinates parsed from CruiseMapper
#
# pip install playwright beautifulsoup4
# python -m playwright install --with-deps chromium

import os, json, hashlib, sys, math, traceback, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urljoin, urlparse, urlunparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup, Tag, NavigableString

REPO_ROOT  = os.path.dirname(__file__)
DOCS_DIR   = os.path.join(REPO_ROOT, "docs")
STATE_PATH = os.path.join(REPO_ROOT, "state.json")
SHIPS_PATH = os.path.join(REPO_ROOT, "ships.json")

# ---- History settings ----
HIST_DIR      = os.path.join(REPO_ROOT, "history")
PER_SHIP_CAP  = 50
ALL_CAP       = 100

# ---- Special geofences (center lat/lon + radius_km)
SPECIAL_GEOFENCES = {
    "Disney's Castaway Cay": {
        "aliases": ["gorda cay", "castaway cay"],
        "center": (26.0817, -77.5460),
        "radius_km": 4.0
    },
    "Disney's Lookout Cay at Lighthouse Point": {
        "aliases": ["lighthouse point", "lookout cay", "lighthouse pt"],
        "center": (24.8350, -76.2800),
        "radius_km": 5.0
    }
}

# ---- Port timezone mapping (substring match, case-insensitive) - fallback
PORT_TZ_MAP = [
    ("canaveral", "America/New_York"),
    ("everglades", "America/New_York"),
    ("fort lauderdale", "America/New_York"),
    ("castaway", "America/Nassau"),
    ("gorda cay", "America/Nassau"),
    ("lookout cay", "America/Nassau"),
    ("lighthouse point", "America/Nassau"),
    ("nassau", "America/Nassau"),
    ("cozumel", "America/Cancun"),
    ("progreso", "America/Merida"),
    ("galveston", "America/Chicago"),
    ("san juan", "America/Puerto_Rico"),
    ("tortola", "America/Tortola"),
    ("st. maarten", "America/Lower_Princes"),
    ("st maarten", "America/Lower_Princes"),
    ("basseterre", "America/St_Kitts"),
    ("antigua", "America/Antigua"),
    ("falmouth", "America/Jamaica"),
    ("castries", "America/St_Lucia"),
    ("st. lucia", "America/St_Lucia"),
    ("curaçao", "America/Curacao"),
    ("willemstad", "America/Curacao"),
    ("aruba", "America/Aruba"),
    ("cayman", "America/Cayman"),
    ("roseau", "America/Dominica"),
    ("dominica", "America/Dominica"),
    ("cabo", "America/Mazatlan"),
    ("ensenada", "America/Tijuana"),
    ("vallarta", "America/Bahia_Banderas"),
    ("juneau", "America/Juneau"),
    ("skagway", "America/Juneau"),
    ("ketchikan", "America/Sitka"),
    ("icy strait", "America/Juneau"),
    ("glacier viewing", "America/Juneau"),
    ("honolulu", "Pacific/Honolulu"),
    ("kahului", "Pacific/Honolulu"),
    ("nawiliwili", "Pacific/Honolulu"),
    ("hilo", "Pacific/Honolulu"),
    ("auckland", "Pacific/Auckland"),
    ("wellington", "Pacific/Auckland"),
    ("tauranga", "Pacific/Auckland"),
    ("christchurch", "Pacific/Auckland"),
    ("lyttelton", "Pacific/Auckland"),
    ("eden", "Australia/Sydney"),
    ("hobart", "Australia/Hobart"),
    ("melbourne", "Australia/Melbourne"),
    ("sydney", "Australia/Sydney"),
    ("noumea", "Pacific/Noumea"),
    ("suva", "Pacific/Fiji"),
    ("pago pago", "Pacific/Pago_Pago"),
    ("southampton", "Europe/London"),
    ("liverpool", "Europe/London"),
    ("portland", "Europe/London"),
    ("greenock", "Europe/London"),
    ("amsterdam", "Europe/Amsterdam"),
    ("rotterdam", "Europe/Amsterdam"),
    ("zeebrugge", "Europe/Brussels"),
    ("vigo", "Europe/Madrid"),
    ("bilbao", "Europe/Madrid"),
    ("malaga", "Europe/Madrid"),
    ("barcelona", "Europe/Madrid"),
    ("cadiz", "Europe/Madrid"),
    ("cartagena", "Europe/Madrid"),
    ("alesund", "Europe/Oslo"),
    ("bergen", "Europe/Oslo"),
    ("olden", "Europe/Oslo"),
    ("haugesund", "Europe/Oslo"),
    ("stavanger", "Europe/Oslo"),
    ("mekjarvik", "Europe/Oslo"),
    ("messina", "Europe/Rome"),
    ("civitavecchia", "Europe/Rome"),
    ("rome", "Europe/Rome"),
    ("naples", "Europe/Rome"),
    ("livorno", "Europe/Rome"),
    ("ajaccio", "Europe/Paris"),
    ("la coruna", "Europe/Madrid"),
    ("coruna", "Europe/Madrid"),
    ("chania", "Europe/Athens"),
    ("corfu", "Europe/Athens"),
    ("argostoli", "Europe/Athens"),
    ("santorini", "Europe/Athens"),
    ("mykonos", "Europe/Athens"),
    ("dubrovnik", "Europe/Zagreb"),
    ("athens", "Europe/Athens"),
    ("piraeus", "Europe/Athens"),
    ("valetta", "Europe/Malta"),
    ("malta", "Europe/Malta"),
    ("funchal", "Atlantic/Madeira"),
    ("vancouver", "America/Vancouver"),
    ("victoria", "America/Vancouver"),
]

# ---- VF port link country prefix → IANA tz (primary)
TZ_BY_PORT_PREFIX = {
    # Americas
    "US": "America/New_York",
    "CA": "America/Vancouver",
    "MX": "America/Cancun",
    "PR": "America/Puerto_Rico",
    "JM": "America/Jamaica",
    "BS": "America/Nassau",
    "KY": "America/Cayman",
    "AW": "America/Aruba",
    "CW": "America/Curacao",
    "VG": "America/Tortola",
    # Europe
    "GB": "Europe/London",
    "IE": "Europe/Dublin",
    "ES": "Europe/Madrid",
    "PT": "Europe/Lisbon",   # use Atlantic/Madeira if you want precise Madeira
    "FR": "Europe/Paris",
    "IT": "Europe/Rome",
    "MT": "Europe/Malta",
    "GR": "Europe/Athens",
    "HR": "Europe/Zagreb",
    "NL": "Europe/Amsterdam",
    "BE": "Europe/Brussels",
    "DE": "Europe/Berlin",
    "NO": "Europe/Oslo",
    "DK": "Europe/Copenhagen",
    "SE": "Europe/Stockholm",
    "FI": "Europe/Helsinki",
    "IS": "Atlantic/Reykjavik",
    # Pacific / Oceania
    "AU": "Australia/Sydney",
    "NZ": "Pacific/Auckland",
    "NC": "Pacific/Noumea",
    "FJ": "Pacific/Fiji",
    "AS": "Pacific/Pago_Pago",
    # Latin America
    "PA": "America/Panama",
    "CO": "America/Bogota",
}

# ========== IO helpers ==========
def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_history(slug: str):
    os.makedirs(HIST_DIR, exist_ok=True)
    path = os.path.join(HIST_DIR, f"{slug}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_history(slug: str, items: list):
    os.makedirs(HIST_DIR, exist_ok=True)
    path = os.path.join(HIST_DIR, f"{slug}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)

# ---- Sorted merge (by eventUtc DESC)
def _event_key(it):
    try:
        return datetime.fromisoformat(it.get("eventUtc","")).timestamp()
    except Exception:
        return 0.0

def merge_items(existing: list, new_items: list, cap: int):
    by_guid = {}
    for it in existing:
        by_guid[it["guid"]] = it
    for it in new_items:
        by_guid[it["guid"]] = it
    merged = sorted(by_guid.values(), key=_event_key, reverse=True)
    return merged[:cap]

def rss_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def to_rfc2822(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def make_id(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def build_rss(channel_title: str, channel_link: str, items: list) -> str:
    xml_items = []
    for it in items:
        xml_items.append(f"""
  <item>
    <title>{rss_escape(it["title"])}</title>
    <link>{rss_escape(it.get("link",""))}</link>
    <guid isPermaLink="false">{rss_escape(it["guid"])}</guid>
    <pubDate>{rss_escape(it["pubDate"])}</pubDate>
    <description>{rss_escape(it.get("description",""))}</description>
  </item>""")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
  <title>{rss_escape(channel_title)}</title>
  <link>{rss_escape(channel_link)}</link>
  <description>{rss_escape(channel_title)} - Auto-generated</description>
  <lastBuildDate>{to_rfc2822(datetime.utcnow())}</lastBuildDate>
  {''.join(xml_items)}
</channel>
</rss>
"""

# ========== Time handling ==========
def _parse_vf_time_utc(raw_time: str):
    if not raw_time: return None
    raw = raw_time.strip()
    fmts = ["%b %d, %H:%M", "%b %d, %I:%M %p", "%b %d, %H:%M:%S"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(raw, fmt)
            dt = dt.replace(year=datetime.utcnow().year, tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None

def _port_zoneinfo_from_link(port_link: str):
    try:
        m = re.search(r"/ports/([A-Z]{2})", port_link or "")
        if not m: return None
        cc = m.group(1)
        tz = TZ_BY_PORT_PREFIX.get(cc)
        return ZoneInfo(tz) if tz else None
    except Exception:
        return None

def _port_zoneinfo_from_name(port_name: str):
    if not port_name:
        return ZoneInfo("America/New_York")
    name = port_name.lower()
    for needle, tz in PORT_TZ_MAP:
        if needle in name:
            try:
                return ZoneInfo(tz)
            except Exception:
                break
    return ZoneInfo("America/New_York")

def format_times_for_notification(port_name: str, port_link: str, when_raw: str):
    dt_utc = _parse_vf_time_utc(when_raw)
    if not dt_utc:
        return None, None, None  # (est_str, local_str, eventUtc_iso)

    eastern = ZoneInfo("America/New_York")
    est_dt = dt_utc.astimezone(eastern)
    est_str = est_dt.strftime("%b %d, %I:%M %p %Z")   # DST-aware EST/EDT

    tz_local = _port_zoneinfo_from_link(port_link) or _port_zoneinfo_from_name(port_name)
    local_dt = dt_utc.astimezone(tz_local)
    local_str = local_dt.strftime("%b %d, %I:%M %p %Z")

    return est_str, local_str, dt_utc.isoformat()

# ========== VesselFinder scraping (Recent Port Calls) ==========
def _find_root(soup: BeautifulSoup):
    for tag in soup.find_all(lambda t: isinstance(t, Tag) and t.name in ("h1","h2","h3","h4","div")):
        txt = (tag.get_text(strip=True) or "").lower()
        if "recent port calls" in txt:
            nxt = tag.find_next_sibling()
            hops = 0
            while nxt and hops < 6 and (isinstance(nxt, NavigableString) or (isinstance(nxt, Tag) and nxt.get_text(strip=True) == "")):
                nxt = nxt.next_sibling; hops += 1
            if isinstance(nxt, Tag):
                return nxt
    lab = soup.find(string=lambda s: isinstance(s, str) and "arrival (utc)" in s.lower())
    if lab:
        node = lab
        for _ in range(6):
            node = node.parent
            if not isinstance(node, Tag): break
            labels = node.find_all(string=lambda s: isinstance(s, str) and "arrival (utc)" in s.lower())
            if len(labels) >= 2:
                return node
    return None

def _parse_vf(html: str):
    soup = BeautifulSoup(html, "html.parser")
    root = _find_root(soup)
    results = []
    if not root:
        return results

    def block_has_labels(block: Tag) -> bool:
        txt = (block.get_text(" ", strip=True) or "").lower()
        return ("arrival (utc)" in txt) or ("departure (utc)" in txt)

    blocks = [c for c in root.find_all(recursive=False) if isinstance(c, Tag)]
    for block in blocks:
        candidates = [block] + [c for c in block.find_all(recursive=False) if isinstance(c, Tag)]
        matched = next((c for c in candidates if block_has_labels(c)), None)
        if not matched:
            continue
        a = matched.find("a")
        port_name = a.get_text(strip=True) if a else "Unknown Port"
        port_link = a["href"] if (a and a.has_attr("href")) else ""
        def value_after(label_substr: str) -> str:
            lab = matched.find(string=lambda s: isinstance(s, str) and label_substr in s.lower())
            if not lab: return ""
            try:
                lab_div = lab.parent if isinstance(lab.parent, Tag) else None
                if lab_div:
                    nxt = lab_div.find_next_sibling()
                    hops = 0
                    while nxt and hops < 6 and (not isinstance(nxt, Tag) or nxt.get_text(strip=True) == ""):
                        nxt = nxt.next_sibling; hops += 1
                    if isinstance(nxt, Tag):
                        return nxt.get_text(strip=True)
            except Exception:
                pass
            return ""
        arr = value_after("arrival (utc)")
        dep = value_after("departure (utc)")
        if arr:
            results.append({"event":"Arrived","port":port_name,"when_raw":arr,"link":port_link,
                            "detail":f"{port_name} Arrival (UTC) {arr}"})
        if dep:
            results.append({"event":"Departed","port":port_name,"when_raw":dep,"link":port_link,
                            "detail":f"{port_name} Departure (UTC) {dep}"})
    return results

def _rendered_html(url: str, p, mobile: bool):
    ua = ("Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120 Mobile Safari/537.36") if mobile else \
         ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120 Safari/537.36")
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(user_agent=ua, viewport={"width": 1280, "height": 2000})
    page = ctx.new_page()
    try:
        page.goto(url, timeout=45000, wait_until="domcontentloaded")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6);")
        try:
            page.wait_for_selector("text=Recent Port Calls", timeout=8000)
        except PWTimeout:
            pass
        return page.content()
    finally:
        ctx.close(); browser.close()

def _vf_events_for_ship(p, ship):
    base_url = ship["url"]
    try:
        html = _rendered_html(base_url, p, mobile=False)
        rows = _parse_vf(html)
        if rows: return rows, base_url
    except Exception:
        pass
    try:
        parsed = urlparse(base_url)
        mobile_url = urlunparse(parsed._replace(netloc="m.vesselfinder.com"))
        html = _rendered_html(mobile_url, p, mobile=True)
        rows = _parse_vf(html)
        if rows: return rows, mobile_url
    except Exception:
        pass
    return [], base_url

# ========== CruiseMapper coordinate scrape ==========
COORD_RE = re.compile(
    r'([+-]?\d+(?:\.\d+)?)\s*[°]?\s*([NS])?\s*[,/ ]\s*([+-]?\d+(?:\.\d+)?)\s*[°]?\s*([EW])?',
    re.IGNORECASE
)

def _cm_slug(name: str) -> str:
    return "-".join(part for part in name.split())

def _parse_coords(text: str):
    m = COORD_RE.search(text or "")
    if not m: return None
    lat, ns, lon, ew = m.groups()
    lat = float(lat); lon = float(lon)
    if ns and ns.upper() == "S": lat = -abs(lat)
    if ew and ew.upper() == "W": lon = -abs(lon)
    return (lat, lon)

def _cm_fetch_coords(p, cm_url: str):
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120 Safari/537.36")
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(user_agent=ua, viewport={"width": 1280, "height": 1600})
    page = ctx.new_page()
    try:
        page.goto(cm_url, timeout=45000, wait_until="domcontentloaded")
        html = page.content()
    finally:
        ctx.close(); browser.close()
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text(" ", strip=True)
    coords = _parse_coords(txt)
    return coords  # (lat, lon) or None

def haversine_km(a, b):
    R = 6371.0
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 2*R*math.asin(math.sqrt(h))

def geofence_events_from_coords(ship_name: str, slug: str, coords, state_seen):
    items = []
    if coords is None:
        return items

    geo_state = state_seen.setdefault("geo", {}).setdefault(slug, {})
    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

    for fence_name, info in SPECIAL_GEOFENCES.items():
        center = info["center"]
        radius = info["radius_km"]
        dist = haversine_km(coords, center)
        inside = dist <= radius
        key = fence_name
        prev = geo_state.get(key)

        if prev is None:
            geo_state[key] = inside
            continue

        if inside and not prev: