#!/usr/bin/env python3
# DCL Ship Alerts — scraper + RSS generator (GitHub Pages compatible)
# Requires: requests, beautifulsoup4
import os, re, json, hashlib, sys, traceback
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup, NavigableString, Tag

REPO_ROOT = os.path.dirname(__file__)
DOCS_DIR = os.path.join(REPO_ROOT, "docs")
STATE_PATH = os.path.join(REPO_ROOT, "state.json")
SHIPS_PATH = os.path.join(REPO_ROOT, "ships.json")

USER_AGENT = "Mozilla/5.0 (compatible; DCL-Ship-Alerts/1.0)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})

def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def rss_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def make_id(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def to_rfc2822(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def build_rss(channel_title: str, channel_link: str, items: list) -> str:
    xml_items = []
    for it in items:
        title = rss_escape(it["title"])
        desc  = rss_escape(it.get("description",""))
        link  = rss_escape(it.get("link",""))
        guid  = rss_escape(it["guid"])
        pub   = rss_escape(it["pubDate"])
        xml_items.append(f"""
  <item>
    <title>{title}</title>
    <link>{link}</link>
    <guid isPermaLink="false">{guid}</guid>
    <pubDate>{pub}</pubDate>
    <description>{desc}</description>
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

# ------------------ NEW: robust parser for "Recent Port Calls" ------------------

def _is_header(tag: Tag) -> bool:
    if not isinstance(tag, Tag): return False
    if tag.name in ("h1","h2","h3","h4","h5"):
        return True
    # some pages use divs styled like headers
    txt = (tag.get_text(strip=True) or "").lower()
    return tag.name == "div" and any(k in txt for k in ("recent port calls", "port calls", "arrivals", "departures"))

def _find_recent_port_calls_root(soup: BeautifulSoup) -> Tag | None:
    # 1) exact phrase
    for tag in soup.find_all(lambda t: isinstance(t, Tag) and _is_header(t)):
        if "recent port calls" in (tag.get_text(strip=True) or "").lower():
            # usually the container with cards is right after this header
            nxt = tag.find_next_sibling()
            # if whitespace/text nodes in between, walk forward a bit
            steps = 0
            while nxt and steps < 5 and (isinstance(nxt, NavigableString) or (isinstance(nxt, Tag) and nxt.get_text(strip=True) == "")):
                nxt = nxt.next_sibling
                steps += 1
            if isinstance(nxt, Tag):
                return nxt

    # 2) fallback: search for any element that contains "Arrival (UTC)" which appears in each card
    cand = soup.find(string=lambda s: isinstance(s, str) and "arrival (utc)" in s.lower())
    if cand:
        # climb up until we reach a container that has multiple siblings like cards
        node = cand
        for _ in range(6):
            node = node.parent
            if not isinstance(node, Tag): break
            # heuristic: a container whose direct children contain multiple "Arrival (UTC)" labels
            siblings = node.find_all(string=lambda s: isinstance(s, str) and "arrival (utc)" in s.lower())
            if len(siblings) >= 2:
                return node
    return None

def parse_port_calls(html: str):
    soup = BeautifulSoup(html, "html.parser")
    root = _find_recent_port_calls_root(soup)
    rows = []

    if not root:
        # nothing found — return empty; caller will log this
        return rows

    # Each port call looks like a block with an anchor (port name) and label/value pairs
    # Strategy:
    #  - find each child block that contains a port link and an "Arrival (UTC)" label
    #  - read the value immediately to the right/below the label
    for block in root.find_all(recursive=False):
        if not isinstance(block, Tag): 
            continue
        # be tolerant: some sites nest a block extra deep
        blk_txt = (block.get_text(" ", strip=True) or "").lower()
        if "arrival (utc)" not in blk_txt and "departure (utc)" not in blk_txt:
            # try one level deeper
            inner = block.find(string=lambda s: isinstance(s, str) and ("arrival (utc)" in s.lower() or "departure (utc)" in s.lower()))
            if not inner:
                continue

        # port name + link
        a = block.find("a")
        port_name = a.get_text(strip=True) if a else "Unknown Port"
        port_link = a["href"] if (a and a.has_attr("href")) else ""

        def value_after(label_substr: str) -> str:
            lab = block.find(string=lambda s: isinstance(s, str) and label_substr in s.lower())
            if not lab: 
                return ""
            # typical layout: label is inside a small <div>, value is the next <div> sibling
            try:
                lab_div = lab.parent if isinstance(lab.parent, Tag) else None
                if lab_div:
                    # find the next tag sibling that has some text
                    nxt = lab_div.find_next_sibling()
                    hops = 0
                    while nxt and hops < 4 and (not isinstance(nxt, Tag) or (isinstance(nxt, Tag) and (nxt.get_text(strip=True) == ""))):
                        nxt = nxt.next_sibling
                        hops += 1
                    if isinstance(nxt, Tag):
                        return nxt.get_text(strip=True)
            except Exception:
                pass
            return ""

        arr = value_after("arrival (utc)")
        dep = value_after("departure (utc)")

        # Build items (use whichever exists)
        if arr:
            rows.append({
                "event": "Arrival",
                "port": port_name,
                "when_raw": arr,
                "link": port_link,
                "detail": f"{port_name} Arrival (UTC) {arr}"
            })
        if dep:
            rows.append({
                "event": "Departure",
                "port": port_name,
                "when_raw": dep,
                "link": port_link,
                "detail": f"{port_name} Departure (UTC) {dep}"
            })

    return rows

# ------------------ main runner ------------------

def main():
    os.makedirs(DOCS_DIR, exist_ok=True)
    ships = load_json(SHIPS_PATH, [])
    state = load_json(STATE_PATH, {"seen": {}})
    all_items = []

    for s in ships:
        name = s["name"]; slug = s["slug"]; url = s["url"]
        print(f"[info] Fetching {name}: {url}")
        try:
            resp = SESSION.get(url, timeout=45)
            resp.raise_for_status()
        except Exception as e:
            print(f"[warn] fetch failed for {name}: {e}", file=sys.stderr)
            continue

        try:
            rows = parse_port_calls(resp.text)
            print(f"[info] Parsed {name}: found {len(rows)} events")
        except Exception as e:
            print(f"[error] parse failed for {name}: {e}\n{traceback.format_exc()}", file=sys.stderr)
            rows = []

        ship_items = []
        for r in rows:
            guid_src = f"{slug}|{r['event']}|{r['detail']}"
            guid = make_id(guid_src)
            if state["seen"].get(guid):
                continue
            # We don't have a machine timestamp from the page reliably; use now.
            pub_dt = datetime.utcnow()
            title = f"{name} — {r['event']} — {r['port'] or 'Unknown Port'}"
            desc  = r["detail"]
            link  = urljoin(url, r["link"]) if r["link"] else url
            item = {
                "title": title,
                "description": desc,
                "link": link,
                "guid": guid,
                "pubDate": to_rfc2822(pub_dt)
            }
            ship_items.append(item)
            all_items.append(item)
            state["seen"][guid] = True

        # write per-ship feed (cap to last 50 new items per run)
        feed_xml = build_rss(f"{name} - Arrivals & Departures", url, ship_items[:50])
        with open(os.path.join(DOCS_DIR, f"{slug}.xml"), "w", encoding="utf-8") as f:
            f.write(feed_xml)

    # write combined feed (up to 100 items)
    all_items_sorted = all_items[::-1]
    all_xml = build_rss("DCL Ships - Arrivals & Departures (All)", "https://github.com/", all_items_sorted[:100])
    with open(os.path.join(DOCS_DIR, "all.xml"), "w", encoding="utf-8") as f:
        f.write(all_xml)

    save_json(STATE_PATH, state)

if __name__ == "__main__":
    main()
