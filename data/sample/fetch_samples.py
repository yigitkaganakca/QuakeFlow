"""Run-once helper that downloads small sample payloads for offline tests.

Output:
    afad_2025-01-01.json    - one full day from AFAD (≈100 events)
    emsc_recent.json        - recent week from EMSC, top of bbox
    usgs_recent.json        - recent month from USGS, top of bbox
    koeri_lst0.html         - rolling KOERI snapshot (last 500)

Run:
    python data/sample/fetch_samples.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

OUT = Path(__file__).resolve().parent
HEADERS = {"User-Agent": "ITU-YZV322E-QuakeFlow/1.0 (academic; sample fetch)"}

TR_BBOX = "minlatitude=35.5&maxlatitude=42.5&minlongitude=25.5&maxlongitude=45.0"


def get(url: str, **kw) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=60, **kw)
    r.raise_for_status()
    return r


def afad():
    p = (OUT / "afad_2025-01-01.json")
    if p.exists():
        print("afad: already present, skipping")
        return
    r = get("https://deprem.afad.gov.tr/apiv2/event/filter",
            params={"start": "2025-01-01 00:00:00",
                    "end":   "2025-01-02 00:00:00",
                    "minmag": 1.0})
    p.write_text(json.dumps(r.json(), ensure_ascii=False, indent=2),
                 encoding="utf-8")
    print(f"afad -> {p} ({p.stat().st_size} bytes)")


def emsc():
    p = OUT / "emsc_recent.json"
    if p.exists():
        print("emsc: already present, skipping")
        return
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    url = (f"https://www.seismicportal.eu/fdsnws/event/1/query?format=json&"
           f"starttime={start:%Y-%m-%dT%H:%M:%S}&"
           f"endtime={end:%Y-%m-%dT%H:%M:%S}&"
           f"minmag=2.0&limit=200&{TR_BBOX}")
    r = get(url)
    p.write_text(r.text, encoding="utf-8")
    print(f"emsc -> {p} ({p.stat().st_size} bytes)")


def usgs():
    p = OUT / "usgs_recent.json"
    if p.exists():
        print("usgs: already present, skipping")
        return
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    url = (f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&"
           f"starttime={start:%Y-%m-%dT%H:%M:%S}&"
           f"endtime={end:%Y-%m-%dT%H:%M:%S}&"
           f"minmagnitude=2.5&limit=200&{TR_BBOX}")
    r = get(url)
    p.write_text(r.text, encoding="utf-8")
    print(f"usgs -> {p} ({p.stat().st_size} bytes)")


def koeri():
    p = OUT / "koeri_lst0.html"
    if p.exists():
        print("koeri: already present, skipping")
        return
    r = get("http://www.koeri.boun.edu.tr/scripts/lst0.asp")
    r.encoding = r.apparent_encoding or "iso-8859-9"
    p.write_text(r.text, encoding="utf-8")
    print(f"koeri -> {p} ({p.stat().st_size} bytes)")


if __name__ == "__main__":
    afad()
    emsc()
    usgs()
    koeri()
    print("done.")
