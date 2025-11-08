# add at top with others
import os, argparse, hashlib
from datetime import datetime, timezone
from xml.dom import minidom

# ...

def guid_manual(ship: str, event: str, port: str, est_label: str, nonce: str = "") -> str:
    # include a nonce so repeated tests are unique to Power Automate
    key = f"manual|{ship}|{event.lower()}|{port.lower()}|{est_label}|{nonce}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def build_item(ship, event, port, est, local, link, nonce):
    verb = "Arrived at" if event.lower() == "arrived" else "Departed from"
    title = f"{ship} {verb} {port} at {est}"
    if local:
        title += f". The local time to the port is {local}"
    desc = f"{port} ({event.capitalize()})" + (f" — Local: {local}" if local else "")
    # append nonce to link as a harmless query param (extra safety)
    link_final = (link or "#")
    if nonce:
        sep = "&" if "?" in link_final else "?"
        link_final = f"{link_final}{sep}n={nonce}"
    return {
        "title": title,
        "description": desc,
        "link": link_final,
        "guid": guid_manual(ship, event, port, est, nonce),
        "pubDate": to_rfc2822(datetime.utcnow()),
    }
