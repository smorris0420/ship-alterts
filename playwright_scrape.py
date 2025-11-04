#!/usr/bin/env python3
# Free, JS-rendered scrape of VesselFinder "Recent Port Calls" using Playwright (Chromium)
# Requires: pip install playwright beautifulsoup4 && python -m playwright install chromium

import os, json, hashlib, sys, traceback
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup, Tag, NavigableString

REPO_ROOT = os.path.dirname(__file__)
DOCS_DIR  = os.path.join(REPO_ROOT, "docs")
STATE_PATH= os.path.join(REPO_ROOT, "state.json")
SHIPS_PATH= os.path.join(REPO_ROOT, "ships.json")

def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

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

# ---- Parse VesselFinder "Recent Port Calls" (div-based card list) ----
def find_recent_port_calls_root(soup: BeautifulSoup):
    # 1) look for a header containing "Recent Port Calls"
    for tag in soup.find_all(lambda t: isinstance(t, Tag) and t.name in ("h1","h2","h3","h4","div")):
        txt = (tag.get_text(strip=True) or "").lower()
        if "recent port calls" in txt:
            nxt = tag.find_next_sibling()
            hops = 0
            while nxt and hops < 6 and (isinstance(nxt, NavigableString) or (isinstance(nxt, Tag) and nxt.get_text(strip=True) == "")):
                nxt = nxt.next_sibling; hops += 1
            if isinstance(nxt, Tag):
                return nxt
    # 2) fallback: search any element that contains label "Arrival (UTC)"
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

def parse_recent_port_calls(html: str):
    soup = BeautifulSoup(html, "html.parser")
    root = find_recent_port_calls_root(soup)
    results = []
    if not root:
        return results

    def block_has_labels(block: Tag) -> bool:
        txt = (block.get_text(" ", strip=True) or "").lower()
        return ("arrival (utc)" in txt) or ("departure (utc)" in txt)

    blocks = [c for c in root.find_all(recursive=False) if isinstance(c, Tag)]
    for block in blocks:
        # entries may be nested one level
        candidates = [block] + [c for c in block.find_all(recursive=False) if isinstance(c, Tag)]
        matched = next((c for c in candidates if block_has_labels(c)), None)
        if not matched:
            continue

        a = matched.find("a")
        port_name = a.get_text(strip=True) if a else "Unknown Port"
        port_link = a["href"] if (a and a.has_attr("href")) else ""

        def value_after(label_substr: str) -> str:
            lab = matched.find(string=lambda s: isinstance(s, str) and label_substr in s.lower())
            if not lab:
                return ""
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
            results.append({"event":"Arrival","port":port_name,"when_raw":arr,"link":port_link,
                            "detail":f"{port_name} Arrival (UTC) {arr}"})
        if dep:
            results.append({"event":"Departure","port":port_name,"when_raw":dep,"link":port_link,
                            "detail":f"{port_name} Departure (UTC) {dep}"})
    return results

def rendered_html_for(url: str, p, mobile: bool):
    ua = ("Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120 Mobile Safari/537.36") if mobile else \
         ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/120 Safari/537.36")
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(user_agent=ua, viewport={"width": 1280, "height": 2000})
    page = ctx.new_page()
    try:
        page.goto(url, timeout=45000, wait_until="domcontentloaded")
        # Scroll a bit to trigger lazy sections
        page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6);")
        # Try to wait for the Recent Port Calls labels; don't fail hard if missing
        try:
            page.wait_for_selector("text=Recent Port Calls", timeout=8000)
        except PWTimeout:
            pass
        html = page.content()
        return html
    finally:
        ctx.close()
        browser.close()

def fetch_events_for_ship(p, ship):
    base_url = ship["url"]
    # Try desktop first
    try:
        html = rendered_html_for(base_url, p, mobile=False)
        rows = parse_recent_port_calls(html)
        if rows:
            return rows, base_url
    except Exception as e:
        print(f"[warn] desktop render failed for {ship['name']}: {e}", file=sys.stderr)

    # Try mobile hostname
    try:
        parsed = urlparse(base_url)
        mobile_url = urlunparse(parsed._replace(netloc="m.vesselfinder.com"))
        html = rendered_html_for(mobile_url, p, mobile=True)
        rows = parse_recent_port_calls(html)
        if rows:
            return rows, mobile_url
    except Exception as e:
        print(f"[warn] mobile render failed for {ship['name']}: {e}", file=sys.stderr)

    return [], base_url

def main():
    os.makedirs(DOCS_DIR, exist_ok=True)
    ships = load_json(SHIPS_PATH, [])
    state = load_json(STATE_PATH, {"seen": {}})
    all_items = []

    with sync_playwright() as p:
        for s in ships:
            name = s["name"]; slug = s["slug"]; url = s["url"]
            print(f"[info] Fetching {name}: {url}")
            try:
                rows, used = fetch_events_for_ship(p, s)
                print(f"[info] Parsed {name}: {len(rows)} events (source: {used})")
            except Exception as e:
                print(f"[error] parse failed for {name}: {e}\n{traceback.format_exc()}", file=sys.stderr)
                rows = []

            ship_items = []
            for r in rows:
                guid_src = f"{slug}|{r['event']}|{r['detail']}"
                guid = make_id(guid_src)
                if state["seen"].get(guid):
                    continue
                title = f"{name} — {r['event']} — {r['port'] or 'Unknown Port'}"
                desc  = r["detail"]
                link  = urljoin(url, r["link"]) if r["link"] else url
                item = {
                    "title": title,
                    "description": desc,
                    "link": link,
                    "guid": guid,
                    "pubDate": to_rfc2822(datetime.utcnow())
                }
                ship_items.append(item)
                all_items.append(item)
                state["seen"][guid] = True

            xml = build_rss(f"{name} - Arrivals & Departures", url, ship_items[:50])
            with open(os.path.join(DOCS_DIR, f"{slug}.xml"), "w", encoding="utf-8") as f:
                f.write(xml)

    all_xml = build_rss("DCL Ships - Arrivals & Departures (All)", "https://github.com/", all_items[::-1][:100])
    with open(os.path.join(DOCS_DIR, "all.xml"), "w", encoding="utf-8") as f:
        f.write(all_xml)

    save_json(STATE_PATH, state)

if __name__ == "__main__":
    main()
