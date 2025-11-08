#!/usr/bin/env python3
# Hybrid scraper:
# - VesselFinder "Recent Port Calls" (JS-rendered via Playwright)
# - PLUS geofencing for private islands using live coordinates parsed from CruiseMapper
# - PLUS port-page fallback when ship pages are stale (tries last port from ship page
#   and any optional 'home_ports' links from ships.json; checks both Arrivals/Departures tabs)
#
# Requirements:
#   pip install playwright beautifulsoup4
#   python -m playwright install --with-deps chromium

import os, json, hashlib, sys, math, traceback, re, time, random
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup, Tag, NavigableString

REPO_ROOT  = os.path.dirname(__file__)
DOCS_DIR   = os.path.join(REPO_ROOT, "docs")
STATE_PATH = os.path.join(REPO_ROOT, "state.json")
SHIPS_PATH = os.path.join(REPO_ROOT, "ships.json")

# ---- History settings ----
HIST_DIR      = os.path.join(REPO_ROOT, "history")
PER_SHIP_CAP  = 250
ALL_CAP       = 500

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
    },
    # CT8/CT10 and approach
    "Port Canaveral, Florida": {
        "aliases": ["port canaveral", "cape canaveral", "canaveral"],
        "center": (28.4105, -80.6190),
        "radius_km": 6.0
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
    "PT": "Europe/Lisbon",
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

# ---- Default port pages to try when ship rows + home_ports are empty
DEFAULT_PORTS_BY_SHIP = {
    "Disney Wish":       [ { "link": "/ports/USCPV?name=Port-Canaveral", "label": "Port Canaveral" } ],
    "Disney Treasure":   [ { "link": "/ports/USCPV?name=Port-Canaveral", "label": "Port Canaveral" } ],
    "Disney Fantasy":    [ { "link": "/ports/USCPV?name=Port-Canaveral", "label": "Port Canaveral" } ],
    "Disney Dream":      [ { "link": "/ports/USFLL?name=Port-Everglades", "label": "Port Everglades" } ],
    "Disney Magic":      [ { "link": "/ports/USFLL?name=Port-Everglades", "label": "Port Everglades" },
                           { "link": "/ports/PRSJU?name=San-Juan", "label": "San Juan" } ],
    "Disney Wonder":     [ { "link": "/ports/USSEA?name=Seattle", "label": "Seattle" },
                           { "link": "/ports/CAVAN?name=Vancouver", "label": "Vancouver" } ],
    "Disney Adventure":  [ { "link": "/ports/USLAX?name=Los-Angeles", "label": "Los Angeles" } ],
    "Disney Destiny":    [ { "link": "/ports/USCPV?name=Port-Canaveral", "label": "Port Canaveral" } ],
}
GLOBAL_FALLBACK_PORTS = [
    { "link": "/ports/USCPV?name=Port-Canaveral", "label": "Port Canaveral" },
    { "link": "/ports/USFLL?name=Port-Everglades", "label": "Port Everglades" },
    { "link": "/ports/USGLV?name=Galveston", "label": "Galveston" },
    { "link": "/ports/PRSJU?name=San-Juan", "label": "San Juan" },
    { "link": "/ports/BSNAS?name=Nassau", "label": "Nassau" },
]

# ---------- Utilities ----------

def _sleep_jitter(min_s=0.8, max_s=1.6):
    time.sleep(random.uniform(min_s, max_s))

def _looks_blocked(html: str) -> bool:
    if not html: return True
    low = html.lower()
    return ("captcha" in low) or ("access denied" in low) or ("cf-") in low and ("turnstile" in low)

def zinfo(tz_name: str):
    """Safe ZoneInfo constructor with fallback to America/New_York."""
    try:
        if ZoneInfo:
            return ZoneInfo(tz_name)
    except Exception:
        pass
    return ZoneInfo("America/New_York") if ZoneInfo else None

def zinfo_eastern():
    return zinfo("America/New_York")

def load_json(path, default):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"[warn] Failed to load {path}: {e}", file=sys.stderr)
    return default

def save_json(path, data):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[error] Failed to save {path}: {e}", file=sys.stderr)

def load_history(slug: str):
    os.makedirs(HIST_DIR, exist_ok=True)
    path = os.path.join(HIST_DIR, f"{slug}.json")
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"[warn] Failed to read history {path}: {e}", file=sys.stderr)
    return []

def save_history(slug: str, items: list):
    os.makedirs(HIST_DIR, exist_ok=True)
    path = os.path.join(HIST_DIR, f"{slug}.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[error] Failed to write history {path}: {e}", file=sys.stderr)

def _event_key(it):
    try:
        return datetime.fromisoformat(it.get("eventUtc","")).timestamp()
    except Exception:
        return 0.0

def merge_items(existing: list, new_items: list, cap: int):
    by_guid = {}
    for it in existing:
        by_guid[it.get("guid","")] = it
    for it in new_items:
        by_guid[it.get("guid","")] = it
    merged = sorted(by_guid.values(), key=_event_key, reverse=True)
    return merged[:cap]

def rss_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def to_rfc2822(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def make_id(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

# ---- TBA filtering: drop ship-page items with no real UTC time ----
# You can allow Arrived but skip Departed by toggling these.
SKIP_TBA = {
    "Arrived": True,
    "Departed": True
}

# ---- Canonical de-dupe (Option C)

def _normalize_port_name(name: str) -> str:
    s = (name or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    # light normalization (you’ll normalize further downstream)
    s = s.replace("cape canaveral", "port canaveral")
    s = s.replace("ft lauderdale", "fort lauderdale")
    return re.sub(r"\s+", " ", s)

def _canonical_guid(slug: str, verb: str, port: str, event_iso: str) -> str:
    """
    Canonical ID by ship + verb + normalized port + UTC minute.
    Prevents duplicate notifications when a stale ship-page later updates.
    """
    try:
        dt = datetime.fromisoformat(event_iso)
    except Exception:
        dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    key = f"canon|{slug}|{verb.lower()}|{_normalize_port_name(port)}|{dt.isoformat()}"
    return make_id(key)

# ---- XML output formatting knobs + helpers ----
PRETTY_XML = True
USE_CDATA  = True
STYLESHEET_NAME = "rss-dcl.xsl"   # written to docs/

def _pretty_xml(xml_str: str) -> str:
    """Indent XML nicely; fall back to raw if anything fails."""
    try:
        from xml.dom import minidom
        dom = minidom.parseString(xml_str.encode("utf-8"))
        pretty = dom.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")
        return "\n".join([ln for ln in pretty.splitlines() if ln.strip()])
    except Exception:
        return xml_str

def _cdata(s: str) -> str:
    """Wrap text in CDATA safely (handles ']]>')."""
    s = s or ""
    parts = s.split("]]>")
    return "<![CDATA[" + "]]]]><![CDATA[>".join(parts) + "]]>" if len(parts) > 1 else f"<![CDATA[{s}]]>"

def _ensure_stylesheet_dcl():
    """Write/overwrite the DCL-styled XSL to docs/ every run."""
    try:
        os.makedirs(DOCS_DIR, exist_ok=True)
        xsl_path = os.path.join(DOCS_DIR, STYLESHEET_NAME)
        xsl = """<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="html" indent="yes"/>
  <xsl:template match="/">
    <html>
      <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title><xsl:value-of select="rss/channel/title"/></title>
        <style>
          :root{
            --dcl-navy:#16578A;
            --dcl-gold:#C9A227;
            --ink:#1b1b1b;
            --muted:#6b6f76;
            --bg:#16578A;
            --card:#ffffff;
            --line:#e9edf2;
            --pill:#eef4fb;
          }
          *{box-sizing:border-box}
          body{
            margin:0;
            background:var(--bg);
            color:var(--ink);
            font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,Helvetica,sans-serif;
          }
          .bar{background:#ffffff;color:var(--dcl-navy);padding:14px 18px;border-bottom:4px solid var(--dcl-gold);}
          .brand{display:flex;flex-direction:column;align-items:center;text-align:center;gap:6px;max-width:1100px;margin:0 auto;}
          .logo-img{width:325px;height:auto;display:block;margin:0 auto;}
          .brand h1{margin:0;font-size:18px;line-height:1.2;font-weight:700;color:var(--dcl-navy);}
          .wrap{max-width:1100px;margin:18px auto;padding:0 16px}
          .card{background:var(--card);border-radius:10px;box-shadow:0 6px 18px rgba(0,0,0,.10);border:1px solid var(--line);}
          .meta{padding:14px 16px;display:flex;flex-wrap:wrap;gap:12px;align-items:center;border-bottom:1px solid var(--line);color:var(--muted);font-size:12px;}
          .meta a{color:var(--dcl-navy);text-decoration:underline}
          .chip{background:var(--pill);color:var(--dcl-navy);border:1px solid #d7e5f6;padding:4px 8px;border-radius:999px;font-size:12px;font-weight:600;}
          table{width:100%;border-collapse:collapse;font-size:14px;background:#fff}
          thead th{position:sticky;top:0;background:#fbfdff;z-index:1;text-align:left;padding:12px 14px;border-bottom:2px solid var(--line);color:#133c5e;font-weight:700;}
          tbody td{padding:12px 14px;border-bottom:1px solid var(--line);vertical-align:top;}
          tbody tr:hover{background:#fbfdff}
          .title a{color:var(--dcl-navy);text-decoration:none;font-weight:700}
          .title a:hover{text-decoration:underline}
          .guid{font-family:ui-monospace,Menlo,Consolas,monospace;color:var(--muted);font-size:12px}
          .desc{white-space:pre-wrap}
          .badge{display:inline-block;padding:3px 8px;border-radius:6px;font-weight:700;font-size:12px;border:1px solid transparent;margin-right:8px;}
          .arr{background:#e8f6ee;color:#11643a;border-color:#cfead9}
          .dep{background:#fff0f0;color:#8a1620;border-color:#ffd9de}
          @media (max-width:760px){
            thead{display:none}
            tbody tr{display:block;border-bottom:8px solid #f0f4f8}
            tbody td{display:block;border:0;padding:8px 14px}
            tbody td::before{content:attr(data-label) " ";font-weight:600;color:var(--muted);display:block;margin-bottom:2px}
            .brand{gap:8px}
          }
        </style>
      </head>
      <body>
        <div class="bar">
          <div class="brand">
            <img src="DCLDailySummary.png" alt="DCL Logo" class="logo-img"/>
            <h1><xsl:value-of select="rss/channel/title"/></h1>
          </div>
        </div>
        <div class="wrap">
          <div class="card">
            <div class="meta">
              <span class="chip">DCL • Airport &amp; Resort Reporting</span>
              <span><strong>Feed link:</strong> <a href="{rss/channel/link}"><xsl:value-of select="rss/channel/link"/></a></span>
              <span><strong>Last Build:</strong> <xsl:value-of select="rss/channel/lastBuildDate"/></span>
            </div>
            <table role="table" aria-label="Items">
              <thead><tr><th>Title</th><th>Published</th><th>Description</th></tr></thead>
              <tbody>
                <xsl:for-each select="rss/channel/item">
                  <tr>
                    <td class="title" data-label="Title">
                      <span class="badge">
                        <xsl:attribute name="class">
                          <xsl:text>badge </xsl:text>
                          <xsl:choose>
                            <xsl:when test="contains(title,'Arrived')">arr</xsl:when>
                            <xsl:otherwise>dep</xsl:otherwise>
                          </xsl:choose>
                        </xsl:attribute>
                        <xsl:choose>
                          <xsl:when test="contains(title,'Arrived')">ARRIVED</xsl:when>
                          <xsl:otherwise>DEPARTED</xsl:otherwise>
                        </xsl:choose>
                      </span>
                      <a href="{link}"><xsl:value-of select="title"/></a><br/>
                      <span class="guid"><xsl:value-of select="guid"/></span>
                    </td>
                    <td data-label="Published"><xsl:value-of select="pubDate"/></td>
                    <td class="desc" data-label="Description"><xsl:value-of select="description" disable-output-escaping="yes"/></td>
                  </tr>
                </xsl:for-each>
              </tbody>
            </table>
          </div>
        </div>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
"""
        with open(xsl_path, "w", encoding="utf-8") as f:
            f.write(xsl)
    except Exception as e:
        print(f"[warn] Could not write stylesheet: {e}", file=sys.stderr)

def build_rss(channel_title: str, channel_link: str, items: list, stylesheet=None, use_cdata=None) -> str:
    """Build RSS with optional CDATA and XSL stylesheet reference."""
    if stylesheet is None:
        stylesheet = STYLESHEET_NAME
    if use_cdata is None:
        use_cdata = USE_CDATA

    xml_items = []
    for it in items:
        title = rss_escape(it.get("title",""))
        link  = rss_escape(it.get("link",""))
        guid  = rss_escape(it.get("guid",""))
        pub   = rss_escape(it.get("pubDate",""))
        desc  = it.get("description","")
        desc_xml = _cdata(desc) if use_cdata else rss_escape(desc)

        xml_items.append(f"""
  <item>
    <title>{title}</title>
    <link>{link}</link>
    <guid isPermaLink="false">{guid}</guid>
    <pubDate>{pub}</pubDate>
    <description>{desc_xml}</description>
  </item>""")

    pi = f'\n<?xml-stylesheet type="text/xsl" href="{stylesheet}"?>' if stylesheet else ""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>{pi}
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
    return xml

# ---------- Time handling ----------

def _parse_vf_time_utc(raw_time: str):
    if not raw_time:
        return None
    raw = raw_time.strip()
    fmts = ["%b %d, %H:%M", "%b %d, %I:%M %p", "%b %d, %H:%M:%S"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(year=datetime.utcnow().year, tzinfo=timezone.utc)
        except Exception:
            continue
    return None

def _port_zoneinfo_from_link(port_link: str):
    try:
        m = re.search(r"/ports/([A-Z]{2})", port_link or "")
        if not m: return None
        cc = m.group(1)
        tz = TZ_BY_PORT_PREFIX.get(cc)
        return zinfo(tz) if tz else None
    except Exception:
        return None

def _port_zoneinfo_from_name(port_name: str):
    if not port_name:
        return zinfo_eastern()
    name = port_name.lower()
    for needle, tz in PORT_TZ_MAP:
        if needle in name:
            return zinfo(tz)
    return zinfo_eastern()

def format_times_for_notification(port_name: str, port_link: str, when_raw: str):
    dt_utc = _parse_vf_time_utc(when_raw)
    if not dt_utc:
        return None, None, None

    eastern = zinfo_eastern()
    est_dt = dt_utc.astimezone(eastern) if eastern else dt_utc
    est_str = est_dt.strftime("%b %d, %I:%M %p %Z")

    tz_local = _port_zoneinfo_from_link(port_link) or _port_zoneinfo_from_name(port_name)
    local_dt = dt_utc.astimezone(tz_local) if tz_local else dt_utc
    local_str = local_dt.strftime("%b %d, %I:%M %p %Z")

    return est_str, local_str, dt_utc.isoformat()

# ---------- VesselFinder ship-page scraping ----------

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
    lab = soup.find(string=lambda s: isinstance(s, str) and ("arrival (utc)" in s.lower() or "ata (utc)" in s.lower()))
    if lab:
        node = lab
        for _ in range(6):
            node = node.parent
            if not isinstance(node, Tag): break
            labels = node.find_all(string=lambda s: isinstance(s, str) and ("arrival (utc)" in s.lower() or "ata (utc)" in s.lower()))
            if len(labels) >= 2:
                return node
    return None

def _parse_vf(html: str):
    """
    Ship-page parser:
      - supports 'Arrival (UTC)' / 'Departure (UTC)'
      - also supports 'ATA (UTC)' / 'ATD (UTC)' variants
      - emits items even when the time cell is blank (pending) with when_raw=""
    """
    soup = BeautifulSoup(html, "html.parser")
    root = _find_root(soup)
    results = []
    if not root:
        return results

    def block_has_labels(block: Tag) -> bool:
        txt = (block.get_text(" ", strip=True) or "").lower()
        return any(k in txt for k in ("arrival (utc)", "departure (utc)", "ata (utc)", "atd (utc)"))

    def value_after_label(matched: Tag, label_keys):
        lab_node = None
        for key in label_keys:
            lab_node = matched.find(string=lambda s: isinstance(s, str) and key in s.lower())
            if lab_node: break
        if not lab_node:
            return None
        try:
            lab_div = lab_node.parent if isinstance(lab_node.parent, Tag) else None
            if lab_div:
                nxt = lab_div.find_next_sibling()
                hops = 0
                while nxt and hops < 6 and (not isinstance(nxt, Tag) or nxt.get_text(strip=True) == ""):
                    nxt = nxt.next_sibling; hops += 1
                if isinstance(nxt, Tag):
                    val = nxt.get_text(strip=True)
                    return val if val else ""
        except Exception:
            pass
        return ""

    blocks = [c for c in root.find_all(recursive=False) if isinstance(c, Tag)]
    for block in blocks:
        candidates = [block] + [c for c in block.find_all(recursive=False) if isinstance(c, Tag)]
        matched = next((c for c in candidates if block_has_labels(c)), None)
        if not matched:
            continue

        a = matched.find("a")
        port_name = a.get_text(strip=True) if a else "Unknown Port"
        port_link = a["href"] if (a and a.has_attr("href")) else ""

        arr_val = value_after_label(matched, ["arrival (utc)", "ata (utc)"])
        dep_val = value_after_label(matched, ["departure (utc)", "atd (utc)"])

        if arr_val is not None:
            if arr_val:
                results.append({"event":"Arrived","port":port_name,"when_raw":arr_val,"link":port_link,
                                "detail":f"{port_name} Arrival (UTC) {arr_val}"})
            else:
                results.append({"event":"Arrived","port":port_name,"when_raw":"","link":port_link,
                                "detail":f"{port_name} Arrival (UTC) (time not yet posted)"})

        if dep_val is not None:
            if dep_val:
                results.append({"event":"Departed","port":port_name,"when_raw":dep_val,"link":port_link,
                                "detail":f"{port_name} Departure (UTC) {dep_val}"})
            else:
                results.append({"event":"Departed","port":port_name,"when_raw":"","link":port_link,
                                "detail":f"{port_name} Departure (UTC) (time not yet posted)"})

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
        html = page.content()
    finally:
        ctx.close(); browser.close()

    if _looks_blocked(html) and not mobile:
        _sleep_jitter()
        parsed = urlparse(url)
        mobile_url = urlunparse(parsed._replace(netloc="m.vesselfinder.com"))
        return _rendered_html(mobile_url, p, mobile=True)
    _sleep_jitter()
    return html

def _vf_events_for_ship(p, ship):
    base_url = ship["url"]
    # Desktop
    try:
        html = _rendered_html(base_url, p, mobile=False)
        rows = _parse_vf(html)
        if rows: return rows, base_url
    except Exception as e:
        print(f"[warn] desktop VF render failed for {ship['name']}: {e}", file=sys.stderr)
    # Mobile
    try:
        parsed = urlparse(base_url)
        mobile_url = urlunparse(parsed._replace(netloc="m.vesselfinder.com"))
        html = _rendered_html(mobile_url, p, mobile=True)
        rows = _parse_vf(html)
        if rows: return rows, mobile_url
    except Exception as e:
        print(f"[warn] mobile VF render failed for {ship['name']}: {e}", file=sys.stderr)
    return [], base_url

# ---------- CruiseMapper coordinate scrape ----------

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
            when_raw = now_utc.strftime("%b %d, %H:%M")
            est_str, local_str, event_iso = format_times_for_notification(fence_name, "", when_raw)
            title = f"{ship_name} Arrived at {fence_name} at {est_str or 'time TBD ET'}"
            if local_str:
                title += f". The local time to the port is {local_str}"
            desc = f"{fence_name} Arrival (UTC) {now_utc.strftime('%b %d, %H:%M')} — Geofence"
            guid = _canonical_guid(slug, "Arrived", fence_name, event_iso or now_utc.isoformat())
            items.append({
                "title": title,
                "description": desc,
                "link": "",
                "guid": guid,
                "pubDate": to_rfc2822(now_utc),
                "eventUtc": event_iso or now_utc.isoformat(),
                "shipSlug": slug,
                "shipName": ship_name,
                "source": "geo"
            })

        elif (not inside) and prev:
            when_raw = now_utc.strftime("%b %d, %H:%M")
            est_str, local_str, event_iso = format_times_for_notification(fence_name, "", when_raw)
            title = f"{ship_name} Departed from {fence_name} at {est_str or 'time TBD ET'}"
            if local_str:
                title += f". The local time to the port is {local_str}"
            desc = f"{fence_name} Departure (UTC) {now_utc.strftime('%b %d, %H:%M')} — Geofence"
            guid = _canonical_guid(slug, "Departed", fence_name, event_iso or now_utc.isoformat())
            items.append({
                "title": title,
                "description": desc,
                "link": "",
                "guid": guid,
                "pubDate": to_rfc2822(now_utc),
                "eventUtc": event_iso or now_utc.isoformat(),
                "shipSlug": slug,
                "shipName": ship_name,
                "source": "geo"
            })

        geo_state[key] = inside

    return items

# ---------- Port-page fallback ----------

def _ensure_tab(url: str, tab: str) -> str:
    """Return url with ?tab=arrivals/ departures set."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs["tab"] = [tab]
    new_q = urlencode({k: v[0] if isinstance(v, list) else v for k, v in qs.items()})
    return urlunparse(parsed._replace(query=new_q))

def _parse_port_time_lt(raw_time: str, tz: ZoneInfo):
    """
    raw_time is like 'Nov 7, 17:24' (LT on VF port pages).
    Returns (est_str, local_str, iso_utc) using tz and Eastern.
    """
    raw = (raw_time or "").strip()
    for fmt in ("%b %d, %H:%M", "%b %d, %I:%M %p"):
        try:
            base = datetime.strptime(raw, fmt).replace(year=datetime.utcnow().year)
            local = base.replace(tzinfo=tz)
            utc_dt = local.astimezone(timezone.utc)
            eastern = zinfo_eastern()
            est_dt = utc_dt.astimezone(eastern) if eastern else utc_dt
            return est_dt.strftime("%b %d, %I:%M %p %Z"), local.strftime("%b %d, %I:%M %p %Z"), utc_dt.isoformat()
        except Exception:
            continue
    return None, None, None

def _port_tz_from_url(port_url: str, fallback_name: str):
    tz = _port_zoneinfo_from_link(port_url)
    if tz: return tz
    return _port_zoneinfo_from_name(fallback_name)

def _parse_port_table_for_ship(html: str, ship_name: str, port_url: str, tab_kind: str, port_label: str):
    """
    Parse a VF port Arrivals/Departures page and emit rows for the named ship.
    tab_kind: 'arrivals' or 'departures'
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows = []
    tz = _port_tz_from_url(port_url, port_label)

    # Look for a row with the ship name; first column is time (LT)
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        txt = tr.get_text(" ", strip=True)
        if ship_name.lower() not in txt.lower():
            continue

        lt = tds[0].get_text(strip=True)
        est_str, local_str, iso_utc = _parse_port_time_lt(lt, tz)
        if not iso_utc:
            continue

        event = "Arrived" if tab_kind == "arrivals" else "Departed"
        port_name = port_label
        detail = f"{port_name} {'Arrival' if event=='Arrived' else 'Departure'} (UTC) {datetime.fromisoformat(iso_utc).strftime('%b %d, %H:%M')}"

        rows.append({
            "event": event,
            "port": port_name,
            "when_raw": datetime.fromisoformat(iso_utc).strftime("%b %d, %H:%M"),
            "link": port_url,
            "detail": detail,
            "_est": est_str,
            "_local": local_str,
            "_iso": iso_utc,
            "_source": f"port:{tab_kind}"
        })
    return rows

def _fetch_port_fallback_events(p, ship_name: str, candidate_links_with_labels: list):
    """
    Try multiple port links (and both tabs). Each candidate is (port_url, port_label).
    Returns aggregated rows for the ship across all tried pages.
    """
    out = []
    seen = set()
    for port_url, label in candidate_links_with_labels:
        for tab in ("departures", "arrivals"):
            try:
                url = _ensure_tab(urljoin("https://www.vesselfinder.com", port_url), tab)
                html = _rendered_html(url, p, mobile=False)
                rows = _parse_port_table_for_ship(html, ship_name, port_url, tab, label or port_url)
                for r in rows:
                    key = (r["event"], r["port"], r["_iso"])
                    if key in seen:
                        continue
                    out.append(r)
                    seen.add(key)
            except Exception as e:
                print(f"[warn] Port fallback {label or port_url} ({tab}) failed: {e}", file=sys.stderr)
    return out

# ---------- Main ----------

def main():
    os.makedirs(DOCS_DIR, exist_ok=True)

    ships = load_json(SHIPS_PATH, [])
    if not ships:
        print(f"[error] ships.json not found or empty at {SHIPS_PATH}", file=sys.stderr)
        return  # nothing to do

    # name -> slug lookup (for latest-all fallback)
    slug_by_name = {s["name"]: s["slug"] for s in ships}

    state = load_json(STATE_PATH, {"seen": {}, "geo": {}, "canon_seen": {}})
    if "seen" not in state: state["seen"] = {}
    if "geo" not in state: state["geo"] = {}
    canon_seen = state.setdefault("canon_seen", {})

    all_items_new = []

    _ensure_stylesheet_dcl()

    with sync_playwright() as p:
        for s in ships:
            name = s.get("name"); slug = s.get("slug"); vf_url = s.get("url")
            if not (name and slug and vf_url):
                print(f"[warn] skipping malformed ship entry: {s}", file=sys.stderr)
                continue

            print(f"[info] Fetching VF for {name}: {vf_url}")

            # 1) VesselFinder port-calls (ship page)
            try:
                rows, used = _vf_events_for_ship(p, s)
                print(f"[info] Parsed VF {name}: {len(rows)} events")
            except Exception as e:
                print(f"[error] VF parse failed for {name}: {e}\n{traceback.format_exc()}", file=sys.stderr)
                rows = []
                used = vf_url

            ship_items_new = []

            # 1a) Build items from ship page rows (ATA/ATD supported)
            for r in rows:
                try:
                    est_str, local_str, event_iso = format_times_for_notification(
                        r.get("port",""), r.get("link",""), r.get("when_raw","")
                    )
                    verb = "Arrived" if r.get("event") == "Arrived" else "Departed"
                    title_verb = "Arrived at" if verb == "Arrived" else "Departed from"

                    # ---- Skip TBA (no event time) based on SKIP_TBA switches
                    if (not event_iso) and SKIP_TBA.get(verb, False):
                        continue

                    if est_str and local_str:
                        title = f"{name} {title_verb} {r['port']} at {est_str}. The local time to the port is {local_str}"
                    elif est_str:
                        title = f"{name} {title_verb} {r['port']} at {est_str}"
                    else:
                        # no reliable time -> drop instead of fabricating
                        continue

                    base_desc = r.get("detail","").replace(" (UTC) -", " (UTC) (time not yet posted)")
                    if est_str and local_str:
                        desc = f"{base_desc} — ET: {est_str} | Local: {local_str}"
                    elif est_str:
                        desc = f"{base_desc} — ET: {est_str}"
                    else:
                        desc = base_desc

                    link = urljoin(vf_url, r.get("link","")) if r.get("link") else vf_url

                    event_iso_final = event_iso
                    if not event_iso_final:
                        continue  # double-guard; shouldn't happen with logic above

                    guid = _canonical_guid(slug, verb, r['port'], event_iso_final)
                    if canon_seen.get(guid):
                        continue

                    item = {
                        "title": title,
                        "description": desc,
                        "link": link,
                        "guid": guid,
                        "pubDate": to_rfc2822(datetime.utcnow()),
                        "eventUtc": event_iso_final,
                        "shipSlug": slug,
                        "shipName": name,
                        "source": "vf_ship"
                    }
                    ship_items_new.append(item)
                    all_items_new.append(item)
                    canon_seen[guid] = True
                except Exception as e:
                    print(f"[warn] VF item build failed for {name}: {e}", file=sys.stderr)

            # 2) Port-page fallback
            try:
                candidate_links = []
                # If ship-page produced any rows, prefer its *first* port link as a hint
                if rows and rows[0].get("link"):
                    candidate_links.append((rows[0]["link"], rows[0].get("port","")))

                # Optional: 'home_ports' array in ships.json
                for hp in s.get("home_ports", []):
                    if isinstance(hp, str):
                        candidate_links.append((hp, ""))
                    elif isinstance(hp, dict):
                        link = hp.get("link","")
                        label = hp.get("label","")
                        if link:
                            candidate_links.append((link, label))

                # De-dup by URL
                dedup = {}
                for u,lbl in candidate_links:
                    if u and u not in dedup:
                        dedup[u] = lbl
                candidate_links = [(u, dedup[u]) for u in dedup.keys()]

                # Defaults if nothing to try
                if not candidate_links:
                    dflt = DEFAULT_PORTS_BY_SHIP.get(name, [])
                    if dflt:
                        candidate_links = [(d["link"], d.get("label","")) for d in dflt if d.get("link")]
                    else:
                        candidate_links = [(d["link"], d.get("label","")) for d in GLOBAL_FALLBACK_PORTS]

                if candidate_links:
                    port_rows = _fetch_port_fallback_events(p, name, candidate_links)
                    print(f"[info] Port fallback {name} using {len(candidate_links)} port(s): {len(port_rows)} rows")

                    for r in port_rows:
                        try:
                            verb = r["event"]
                            est_str, local_str, event_iso = r.get("_est"), r.get("_local"), r.get("_iso")
                            title_verb = "Arrived at" if verb == "Arrived" else "Departed from"
                            title = f"{name} {title_verb} {r['port']} at {est_str}. The local time to the port is {local_str}"

                            base_desc = r.get("detail","")
                            desc = f"{base_desc} — ET: {est_str} | Local: {local_str}"

                            link = urljoin("https://www.vesselfinder.com", r.get("link",""))

                            guid = _canonical_guid(slug, verb, r['port'], event_iso)
                            if canon_seen.get(guid):
                                continue

                            item = {
                                "title": title,
                                "description": desc,
                                "link": link,
                                "guid": guid,
                                "pubDate": to_rfc2822(datetime.utcnow()),
                                "eventUtc": event_iso,
                                "shipSlug": slug,
                                "shipName": name,
                                "source": "vf_port"
                            }
                            ship_items_new.append(item)
                            all_items_new.append(item)
                            canon_seen[guid] = True
                        except Exception as e:
                            print(f"[warn] Port-fallback build failed for {name}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"[warn] Port fallback failed for {name}: {e}", file=sys.stderr)

            # 3) Geofence (CruiseMapper coords)
            cm_url = s.get("cm_url") or f"https://www.cruisemapper.com/ships/{_cm_slug(name)}"
            try:
                coords = _cm_fetch_coords(p, cm_url)
                if coords:
                    geo_items = geofence_events_from_coords(name, slug, coords, state)
                    for it in geo_items:
                        if canon_seen.get(it["guid"]):
                            continue
                        ship_items_new.append(it)
                        all_items_new.append(it)
                        canon_seen[it["guid"]] = True
                else:
                    print(f"[warn] No coords from CruiseMapper for {name} ({cm_url})")
            except Exception as e:
                print(f"[warn] Geofence failed for {name}: {e}", file=sys.stderr)

            # ---- PER SHIP HISTORY (sorted by event time) ----
            ship_hist = load_history(slug)
            ship_hist = merge_items(ship_hist, ship_items_new, PER_SHIP_CAP)
            save_history(slug, ship_hist)

            # DEBUG metrics
            print(f"[debug] {name} new_items: ship_page={len([i for i in ship_items_new if i.get('source')=='vf_ship'])} "
                  f"port_fallback={len([i for i in ship_items_new if i.get('source')=='vf_port'])} "
                  f"geo={len([i for i in ship_items_new if i.get('source')=='geo'])} "
                  f"total_added_this_run={len(ship_items_new)} "
                  f"hist_after_merge={len(ship_hist)}")

            # Write per-ship feeds (pretty + XSL PI)
            try:
                ship_xml = build_rss(f"{name} - Arrivals & Departures", vf_url, ship_hist)
                if PRETTY_XML: ship_xml = _pretty_xml(ship_xml)
                with open(os.path.join(DOCS_DIR, f"{slug}.xml"), "w", encoding="utf-8") as f:
                    f.write(ship_xml)

                latest_xml = build_rss(f"{name} - Latest Arrival/Departure", vf_url, ship_hist[:1])
                if PRETTY_XML: latest_xml = _pretty_xml(latest_xml)
                with open(os.path.join(DOCS_DIR, f"{slug}-latest.xml"), "w", encoding="utf-8") as f:
                    f.write(latest_xml)
            except Exception as e:
                print(f"[error] Writing ship feeds failed for {name}: {e}", file=sys.stderr)

    # ---- COMBINED HISTORY (sorted by event time) ----
    all_hist = load_history("all")
    all_hist = merge_items(all_hist, all_items_new, ALL_CAP)
    save_history("all", all_hist)

    try:
        all_xml = build_rss("DCL Ships - Arrivals & Departures (All)", "https://github.com/", all_hist)
        if PRETTY_XML: all_xml = _pretty_xml(all_xml)
        with open(os.path.join(DOCS_DIR, "all.xml"), "w", encoding="utf-8") as f:
            f.write(all_xml)
    except Exception as e:
        print(f"[error] Writing all.xml failed: {e}", file=sys.stderr)

    # ---- Latest one per ship (use shipSlug) ----
    def _infer_slug_from_title(title: str) -> str:
        for nm, sl in slug_by_name.items():
            if title.startswith(nm):
                return sl
        cut = title.find(" Arrived")
        if cut == -1:
            cut = title.find(" Departed")
        base = title[:cut] if cut != -1 else title
        for nm, sl in slug_by_name.items():
            if base.strip() == nm:
                return sl
        return base.strip()

    latest_by_slug = {}
    for it in all_hist:  # already sorted DESC by eventUtc
        key = it.get("shipSlug")
        if not key:
            key = _infer_slug_from_title(it.get("title",""))
        if key and key not in latest_by_slug:
            latest_by_slug[key] = it

    latest_all = list(latest_by_slug.values())
    try:
        latest_all_xml = build_rss("DCL Ships - Latest (One per Ship)", "https://github.com/", latest_all)
        if PRETTY_XML: latest_all_xml = _pretty_xml(latest_all_xml)
        with open(os.path.join(DOCS_DIR, "latest-all.xml"), "w", encoding="utf-8") as f:
            f.write(latest_all_xml)
    except Exception as e:
        print(f"[error] Writing latest-all.xml failed: {e}", file=sys.stderr)

    save_json(STATE_PATH, state)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[fatal] {e}\n{traceback.format_exc()}", file=sys.stderr)
        sys.exit(1)
