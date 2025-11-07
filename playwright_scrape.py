#!/usr/bin/env python3
# Hybrid scraper:
# - VesselFinder "Recent Port Calls" (JS-rendered via Playwright)
# - PLUS geofencing for private islands using live coordinates parsed from CruiseMapper
# - NEW: Port-page fallback (prevents missed arrivals/departures and dedupes later ship-page updates)
#
# Requirements:
#   pip install playwright beautifulsoup4
#   python -m playwright install --with-deps chromium

import os, json, hashlib, sys, math, traceback, re
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # we handle gracefully below
from urllib.parse import urljoin, urlparse, urlunparse
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
    # Port Canaveral geofence (covers CT8/CT10 berths and channel approach)
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

# ---------- Utilities ----------

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

# ---- Canonical GUID (prevents duplicates across sources) ----
def canonical_guid(ship_slug: str, event_kind: str, event_iso_utc: str) -> str:
    return make_id(f"{ship_slug}|{event_kind}|{event_iso_utc}")

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
    """
    Write/overwrite the DCL-styled XSL to docs/ every run.
    Uses your PNG logo at docs/DCLDailySummary.png
    """
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
            --dcl-navy:#16578A;     /* page background + brand color */
            --dcl-gold:#C9A227;     /* trim accent */
            --ink:#1b1b1b;          /* body text on white */
            --muted:#6b6f76;
            --bg:#16578A;           /* page background (blue) */
            --card:#ffffff;         /* card background (white) */
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
          .bar{ background:#ffffff; color:var(--dcl-navy); padding:14px 18px; border-bottom:4px solid var(--dcl-gold); }
          .brand{ display:flex; flex-direction:column; align-items:center; text-align:center; gap:6px; max-width:1100px; margin:0 auto; }
          .logo-img{ width:325px; height:auto; display:block; margin:0 auto; }
          .brand h1{ margin:0; font-size:18px; line-height:1.2; font-weight:700; color:var(--dcl-navy); }

          .wrap{max-width:1100px;margin:18px auto;padding:0 16px}
          .card{ background:#fff; border-radius:10px; box-shadow:0 6px 18px rgba(0,0,0,.10); border:1px solid #e9edf2; }
          .meta{ padding:14px 16px; display:flex; flex-wrap:wrap; gap:12px; align-items:center; border-bottom:1px solid #e9edf2; color:#6b6f76; font-size:12px; }
          .meta a{ color:var(--dcl-navy); text-decoration:underline }
          .chip{ background:#eef4fb; color:var(--dcl-navy); border:1px solid #d7e5f6; padding:4px 8px; border-radius:999px; font-size:12px; font-weight:600; }

          table{ width:100%; border-collapse:collapse; font-size:14px; background:#fff }
          thead th{ position:sticky; top:0; background:#fbfdff; z-index:1; text-align:left; padding:12px 14px; border-bottom:2px solid #e9edf2; color:#133c5e; font-weight:700; }
          tbody td{ padding:12px 14px; border-bottom:1px solid #e9edf2; vertical-align:top; }
          tbody tr:hover{ background:#fbfdff }
          .title a{ color:var(--dcl-navy); text-decoration:none; font-weight:700 }
          .title a:hover{ text-decoration:underline }
          .guid{ font-family:ui-monospace,Menlo,Consolas,monospace; color:#6b6f76; font-size:12px }
          .desc{ white-space:pre-wrap }

          .badge{ display:inline-block; padding:3px 8px; border-radius:6px; font-weight:700; font-size:12px; border:1px solid transparent; margin-right:8px; }
          .arr{ background:#e8f6ee; color:#11643a; border-color:#cfead9 }
          .dep{ background:#fff0f0; color:#8a1620; border-color:#ffd9de }

          @media (max-width:760px){
            thead{display:none}
            tbody tr{display:block;border-bottom:8px solid #f0f4f8}
            tbody td{display:block;border:0;padding:8px 14px}
            tbody td::before{content:attr(data-label) " ";font-weight:600;color:#6b6f76;display:block;margin-bottom:2px}
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
              <thead>
                <tr><th>Title</th><th>Published</th><th>Description</th></tr>
              </thead>
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
                    <td class="desc" data-label="Description">
                      <xsl:value-of select="description" disable-output-escaping="yes"/>
                    </td>
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

# ---------- VesselFinder scraping (ship page) ----------

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

# Ship-page parser (emits "pending" when label exists but time cell blank)
def _parse_vf(html: str):
    soup = BeautifulSoup(html, "html.parser")
    root = _find_root(soup)
    results = []
    if not root:
        return results

    def block_has_labels(block: Tag) -> bool:
        txt = (block.get_text(" ", strip=True) or "").lower()
        return ("arrival (utc)" in txt) or ("departure (utc)" in txt)

    def value_after(matched: Tag, label_substr: str):
        lab = matched.find(string=lambda s: isinstance(s, str) and label_substr in s.lower())
        if not lab:
            return None
        try:
            lab_div = lab.parent if isinstance(lab.parent, Tag) else None
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

        arr_val = value_after(matched, "arrival (utc)")
        dep_val = value_after(matched, "departure (utc)")

        if arr_val is not None:
            results.append({
                "event":"Arrived",
                "port":port_name,
                "when_raw":arr_val or "",
                "link":port_link,
                "detail": f"{port_name} Arrival (UTC) {arr_val}" if arr_val else f"{port_name} Arrival (UTC) (time not yet posted)",
                "source":"vf-ship"
            })

        if dep_val is not None:
            results.append({
                "event":"Departed",
                "port":port_name,
                "when_raw":dep_val or "",
                "link":port_link,
                "detail": f"{port_name} Departure (UTC) {dep_val}" if dep_val else f"{port_name} Departure (UTC) (time not yet posted)",
                "source":"vf-ship"
            })

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

# ---------- Port page fallback (Recent Port Calls table) ----------

def _parse_port_recent_for_ship(html: str, ship_name: str):
    """Return [{'event','when_raw','port','link','source'}] for rows where the ship matches."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    # Find a section/table that has Recent Port Calls
    root = None
    for h in soup.find_all(["h1","h2","h3","div","section","table"]):
        if (h.get_text(strip=True) or "").lower().find("recent port calls") != -1:
            root = h if h.name == "table" else h.find("table")
            if root: break
    if not root:
        return results

    # Iterate rows; look for ship name link + arrival/departure columns
    for tr in root.find_all("tr"):
        tds = tr.find_all("td")
        if not tds or len(tds) < 3:  # defensive
            continue

        # Heuristic: first td contains a link to the ship; others contain ports/times
        link = tr.find("a", href=True)
        if not link: 
            continue
        name = (link.get_text(strip=True) or "")
        if name.lower() != (ship_name or "").lower():
            continue  # only rows for our ship

        # Try to find port name cell and child link
        port_a = None
        for a in tr.find_all("a", href=True):
            if "/ports/" in a["href"]:
                port_a = a
                break

        port_name = port_a.get_text(strip=True) if port_a else "Unknown Port"
        port_link = port_a["href"] if port_a else ""

        # Scrape text of the row for Arrival (UTC) and Departure (UTC)
        row_txt = tr.get_text(" ", strip=True).lower()
        # Simpler/robust: search by labels in the row
        def extract_after(label):
            # Look for next sibling td after a cell that contains label
            lab_td = None
            for td in tds:
                if label in (td.get_text(" ", strip=True) or "").lower():
                    lab_td = td
                    break
            if not lab_td:
                return None
            # Next meaningful sibling td
            nxt = lab_td.find_next_sibling("td")
            hops = 0
            while nxt and hops < 6 and (nxt.get_text(strip=True) == ""):
                nxt = nxt.find_next_sibling("td"); hops += 1
            if not nxt:
                return ""
            return nxt.get_text(strip=True)

        arr_raw = extract_after("arrival (utc)")
        dep_raw = extract_after("departure (utc)")

        if arr_raw is not None:
            results.append({
                "event":"Arrived",
                "when_raw": arr_raw or "",
                "port": port_name,
                "link": port_link,
                "detail": f"{port_name} Arrival (UTC) {arr_raw}" if arr_raw else f"{port_name} Arrival (UTC) (time not yet posted)",
                "source":"vf-port"
            })
        if dep_raw is not None:
            results.append({
                "event":"Departed",
                "when_raw": dep_raw or "",
                "port": port_name,
                "link": port_link,
                "detail": f"{port_name} Departure (UTC) {dep_raw}" if dep_raw else f"{port_name} Departure (UTC) (time not yet posted)",
                "source":"vf-port"
            })
    return results

def _fetch_port_page_and_parse(p, port_link: str, ship_name: str):
    if not port_link:
        return []
    # Normalize to same host as ship page
    parsed = urlparse(port_link)
    if not parsed.netloc:
        port_link = urljoin("https://www.vesselfinder.com/", port_link.lstrip("/"))
    try:
        html = _rendered_html(port_link, p, mobile=False)
    except Exception:
        try:
            html = _rendered_html(port_link.replace("www.","m."), p, mobile=True)
        except Exception:
            return []
    return _parse_port_recent_for_ship(html, ship_name)

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
            guid = canonical_guid(slug, "Arrived", event_iso or now_utc.isoformat())
            items.append({
                "title": title,
                "description": desc,
                "link": "",
                "guid": guid,
                "pubDate": to_rfc2822(now_utc),
                "eventUtc": event_iso or now_utc.isoformat(),
                "shipSlug": slug,
                "shipName": ship_name,
            })

        elif (not inside) and prev:
            when_raw = now_utc.strftime("%b %d, %H:%M")
            est_str, local_str, event_iso = format_times_for_notification(fence_name, "", when_raw)
            title = f"{ship_name} Departed from {fence_name} at {est_str or 'time TBD ET'}"
            if local_str:
                title += f". The local time to the port is {local_str}"
            desc = f"{fence_name} Departure (UTC) {now_utc.strftime('%b %d, %H:%M')} — Geofence"
            guid = canonical_guid(slug, "Departed", event_iso or now_utc.isoformat())
            items.append({
                "title": title,
                "description": desc,
                "link": "",
                "guid": guid,
                "pubDate": to_rfc2822(now_utc),
                "eventUtc": event_iso or now_utc.isoformat(),
                "shipSlug": slug,
                "shipName": ship_name,
            })

        geo_state[key] = inside

    return items

# ---------- Main ----------

def main():
    os.makedirs(DOCS_DIR, exist_ok=True)

    ships = load_json(SHIPS_PATH, [])
    if not ships:
        print(f"[error] ships.json not found or empty at {SHIPS_PATH}", file=sys.stderr)
        return  # nothing to do

    # name -> slug lookup (for latest-all fallback)
    slug_by_name = {s["name"]: s["slug"] for s in ships}

    state = load_json(STATE_PATH, {"seen": {}, "geo": {}})
    if "seen" not in state: state["seen"] = {}
    if "geo" not in state: state["geo"] = {}

    all_items_new = []

    # Write stylesheet each run (kept simple + deterministic)
    _ensure_stylesheet_dcl()

    with sync_playwright() as p:
        for s in ships:
            name = s.get("name"); slug = s.get("slug"); vf_url = s.get("url")
            if not (name and slug and vf_url):
                print(f"[warn] skipping malformed ship entry: {s}", file=sys.stderr)
                continue

            print(f"[info] Fetching VF for {name}: {vf_url}")

            # 1) VesselFinder (ship page)
            try:
                rows, used = _vf_events_for_ship(p, s)
                print(f"[info] Parsed VF {name}: {len(rows)} events")
            except Exception as e:
                print(f"[error] VF parse failed for {name}: {e}\n{traceback.format_exc()}", file=sys.stderr)
                rows = []
                used = vf_url

            # 1B) For any row that lacks a time, try the Port-page fallback for THIS ship at THAT port.
            enriched_rows = []
            for r in rows:
                enriched_rows.append(r)
                if (r.get("when_raw","") == "") and r.get("link"):
                    # attempt to read the port page and find this ship's timestamp
                    try:
                        port_rows = _fetch_port_page_and_parse(p, r["link"], name)
                        # Keep only same event-kind; prefer a row with a real time
                        for pr in port_rows:
                            if pr["event"] == r["event"] and pr.get("when_raw"):
                                # replace the pending with a timed entry from port
                                enriched_rows[-1] = pr  # overwrite last appended (r)
                                break
                    except Exception as e:
                        print(f"[warn] Port fallback failed for {name} at {r.get('port')}: {e}", file=sys.stderr)

            rows = enriched_rows

            # 2) Build items from ship/port rows (with canonical GUIDs when we have an ISO time)
            ship_items_new = []
            for r in rows:
                try:
                    est_str, local_str, event_iso = format_times_for_notification(
                        r.get("port",""), r.get("link",""), r.get("when_raw","")
                    )
                    event_kind = "Arrived" if r.get("event") == "Arrived" else "Departed"
                    verb = "Arrived at" if event_kind == "Arrived" else "Departed from"

                    if est_str and local_str:
                        title = f"{name} {verb} {r['port']} at {est_str}. The local time to the port is {local_str}"
                    elif est_str:
                        title = f"{name} {verb} {r['port']} at {est_str}"
                    else:
                        title = f"{name} {verb} {r['port']} (time TBA)"

                    base_desc = r.get("detail","").replace(" (UTC) -", " (UTC) (time not yet posted)")
                    if est_str and local_str:
                        desc = f"{base_desc} — ET: {est_str} | Local: {local_str}"
                    elif est_str:
                        desc = f"{base_desc} — ET: {est_str}"
                    else:
                        desc = base_desc

                    link = urljoin(vf_url, r.get("link","")) if r.get("link") else vf_url

                    # GUID + seen logic:
                    if event_iso:
                        guid = canonical_guid(slug, event_kind, event_iso)
                    else:
                        # keep "pending" items separate so a later timed event still publishes once
                        guid = make_id(f"pending|{slug}|{event_kind}|{r.get('port','')}|{r.get('source','vf-ship')}")

                    if state["seen"].get(guid):
                        continue  # dedupe across sources

                    item = {
                        "title": title,
                        "description": desc,
                        "link": link,
                        "guid": guid,
                        "pubDate": to_rfc2822(datetime.utcnow()),
                        "eventUtc": event_iso or datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
                        "shipSlug": slug,
                        "shipName": name,
                    }
                    ship_items_new.append(item)
                    all_items_new.append(item)
                    state["seen"][guid] = True
                except Exception as e:
                    print(f"[warn] VF/PORT item build failed for {name}: {e}", file=sys.stderr)

            # 3) Geofence (CruiseMapper coords)
            cm_url = s.get("cm_url") or f"https://www.cruisemapper.com/ships/{_cm_slug(name)}"
            try:
                coords = _cm_fetch_coords(p, cm_url)
                if coords:
                    geo_items = geofence_events_from_coords(name, slug, coords, state)
                    for it in geo_items:
                        if state["seen"].get(it["guid"]):
                            continue
                        ship_items_new.append(it)
                        all_items_new.append(it)
                        state["seen"][it["guid"]] = True
                else:
                    print(f"[warn] No coords from CruiseMapper for {name} ({cm_url})")
            except Exception as e:
                print(f"[warn] Geofence failed for {name}: {e}", file=sys.stderr)

            # ---- PER SHIP HISTORY (sorted by event time) ----
            ship_hist = load_history(slug)
            ship_hist = merge_items(ship_hist, ship_items_new, PER_SHIP_CAP)
            save_history(slug, ship_hist)

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
