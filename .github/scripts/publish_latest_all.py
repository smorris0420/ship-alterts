#!/usr/bin/env python3
import os, argparse, hashlib
from datetime import datetime, timezone
from xml.dom import minidom

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DOCS_DIR = os.path.join(REPO_ROOT, "docs")

def to_rfc2822(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def pretty_xml(xml_str: str) -> str:
    try:
        dom = minidom.parseString(xml_str.encode("utf-8"))
        pretty = dom.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")
        return "\n".join([ln for ln in pretty.splitlines() if ln.strip()])
    except Exception:
        return xml_str

def rss_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") \
                    .replace('"',"&quot;").replace("'","&apos;")

def cdata(s: str) -> str:
    s = s or ""
    parts = s.split("]]>")
    return "<![CDATA[" + "]]]]><![CDATA[>".join(parts) + "]]>" if len(parts) > 1 else f"<![CDATA[{s}]]>"

def guid_manual(ship: str, event: str, port: str, est_label: str) -> str:
    key = f"manual|{ship}|{event.lower()}|{port.lower()}|{est_label}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def build_item(ship, event, port, est, local, link):
    verb = "Arrived at" if event.lower() == "arrived" else "Departed from"
    title = f"{ship} {verb} {port} at {est}"
    if local:
        title += f". The local time to the port is {local}"
    desc = f"{port} ({event.capitalize()})" + (f" — Local: {local}" if local else "")
    return {
        "title": title,
        "description": desc,
        "link": link or "#",
        "guid": guid_manual(ship, event, port, est),
        "pubDate": to_rfc2822(datetime.utcnow()),
    }

def build_rss(channel_title: str, channel_link: str, items: list, stylesheet: str | None = "rss-dcl.xsl") -> str:
    xml_items = []
    for it in items:
        xml_items.append(f"""
  <item>
    <title>{rss_escape(it["title"])}</title>
    <link>{rss_escape(it["link"])}</link>
    <guid isPermaLink="false">{rss_escape(it["guid"])}</guid>
    <pubDate>{rss_escape(it["pubDate"])}</pubDate>
    <description>{cdata(it["description"])}</description>
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
    return pretty_xml(xml)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ship", required=True)
    ap.add_argument("--event", choices=["Arrived","Departed"], required=True)
    ap.add_argument("--port", required=True)
    ap.add_argument("--est", dest="est_time", required=True)
    ap.add_argument("--local", dest="local_time", default="")
    ap.add_argument("--link", default="#")
    ap.add_argument("--filename", default="latest-all.xml", help="Target file in docs/ (default: latest-all.xml)")
    ap.add_argument("--also-underscore", action="store_true", help="Also write docs/latest_all.xml")
    args = ap.parse_args()

    os.makedirs(DOCS_DIR, exist_ok=True)
    item = build_item(args.ship, args.event, args.port, args.est_time, args.local_time, args.link)
    rss = build_rss("DCL Ships - Latest (One per Ship)", "https://github.com/", [item])

    # Write only latest-all.xml (and optional underscore variant). We DO NOT touch latest.xml.
    main_path = os.path.join(DOCS_DIR, args.filename)
    with open(main_path, "w", encoding="utf-8") as f:
        f.write(rss)

    if args.also_underscore:
        with open(os.path.join(DOCS_DIR, "latest_all.xml"), "w", encoding="utf-8") as f:
            f.write(rss)

    print(f"[manual-publish] Wrote: {os.path.relpath(main_path, start=REPO_ROOT)}")
    if args.also_underscore:
        print("[manual-publish] Also wrote: docs/latest_all.xml")

if __name__ == "__main__":
    main()
