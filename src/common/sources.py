"""HTTP clients for the four upstream agencies.

Each function returns a list of records (a record is a `dict`) plus the
exact bytes that were retrieved. The bytes are what we archive immutably
to MinIO and  the parsed records are what we insert into Postgres `raw.*` table.

Design rules we decided on are:
  * No dependency on Airflow or psycopg here# because this module is also used
    by the standalone backfill container and pure unit tests.
  * Polite networking to not get caught to a rate limit: a User-Agent identifying the academic project,
    timeouts, and conservative retry budgets. because the idea is to showcase the pipeline works we know not to process massive data
  * Türkiye bounding box for the global feeds (USGS / EMSC) so we do
    not pull the entire world events
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from .config import settings

TR_BBOX = dict(
    minlatitude=35.5,
    maxlatitude=42.5,
    minlongitude=25.5,
    maxlongitude=45.0,
)

DEFAULT_TIMEOUT = 60


@dataclass
class FetchResult:
    """Output of a `fetch_*` call.

    raw_bytes are what we archive to MinIO untouched.
    raw_ext   arefile extension on the archive object (json or html).
    records   are list of dicts; each dict has at minimum 'event_id' + 'payload'.
    """

    raw_bytes: bytes
    raw_ext:   str
    records:   list[dict[str, Any]]


def _ua_headers() -> dict[str, str]:
    return {"User-Agent": settings.user_agent}


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


AFAD_URL = "https://deprem.afad.gov.tr/apiv2/event/filter"


def fetch_afad(start: datetime, end: datetime, min_mag: float = 1.0) -> FetchResult:
    params = {
        "start":  start.strftime("%Y-%m-%d %H:%M:%S"),
        "end":    end.strftime("%Y-%m-%d %H:%M:%S"),
        "minmag": min_mag,
    }
    r = requests.get(AFAD_URL, params=params, headers=_ua_headers(),
                     timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    raw = r.content
    data = r.json()
    rows = data if isinstance(data, list) else data.get("result", []) or []
    records = [
        {"event_id": str(item.get("eventID")), "payload": item}
        for item in rows
        if item.get("eventID") is not None
    ]
    return FetchResult(raw_bytes=raw, raw_ext="json", records=records)


def parse_afad_bytes(blob: bytes) -> list[dict[str, Any]]:
    """Used by replay mode - re-parse a previously archived AFAD JSON."""
    import json
    data = json.loads(blob)
    rows = data if isinstance(data, list) else data.get("result", []) or []
    return [
        {"event_id": str(item.get("eventID")), "payload": item}
        for item in rows if item.get("eventID") is not None
    ]


EMSC_URL = "https://www.seismicportal.eu/fdsnws/event/1/query"


def fetch_emsc(start: datetime, end: datetime, min_mag: float = 1.5,
               limit: int = 5000) -> FetchResult:
    params = {
        "format":     "json",
        "starttime":  _iso(start),
        "endtime":    _iso(end),
        "minmag":     min_mag,
        "limit":      limit,
        **TR_BBOX,
    }
    r = requests.get(EMSC_URL, params=params, headers=_ua_headers(),
                     timeout=DEFAULT_TIMEOUT)
    if r.status_code == 204:
        return FetchResult(raw_bytes=b"{}", raw_ext="json", records=[])
    r.raise_for_status()
    raw = r.content
    feats = r.json().get("features", []) or []
    records = []
    for f in feats:
        eid = f.get("id") or (f.get("properties") or {}).get("unid")
        if eid is None:
            continue
        records.append({"event_id": str(eid), "payload": f})
    return FetchResult(raw_bytes=raw, raw_ext="json", records=records)


def parse_emsc_bytes(blob: bytes) -> list[dict[str, Any]]:
    import json
    data = json.loads(blob or b"{}")
    feats = data.get("features", []) or []
    out = []
    for f in feats:
        eid = f.get("id") or (f.get("properties") or {}).get("unid")
        if eid is None:
            continue
        out.append({"event_id": str(eid), "payload": f})
    return out



USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def fetch_usgs(start: datetime, end: datetime, min_mag: float = 2.5,
               limit: int = 20000) -> FetchResult:
    params = {
        "format":       "geojson",
        "starttime":    _iso(start),
        "endtime":      _iso(end),
        "minmagnitude": min_mag,
        "limit":        limit,
        "orderby":      "time",
        **TR_BBOX,
    }
    r = requests.get(USGS_URL, params=params, headers=_ua_headers(),
                     timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    raw = r.content
    feats = r.json().get("features", []) or []
    records = [
        {"event_id": str(f.get("id")), "payload": f}
        for f in feats if f.get("id")
    ]
    return FetchResult(raw_bytes=raw, raw_ext="json", records=records)


def parse_usgs_bytes(blob: bytes) -> list[dict[str, Any]]:
    import json
    data = json.loads(blob or b"{}")
    feats = data.get("features", []) or []
    return [
        {"event_id": str(f.get("id")), "payload": f}
        for f in feats if f.get("id")
    ]


# koeri or kandilli, for this the
# endpoint returns an HTML page containing a single <pre> block u can check on website
#
# Only the most recent 500 events are exposed. There is no archive query
# parameter and so as we stated before this source is therefore live-only (again one can check the docs/REPLAY.md).

KOERI_URL = "http://www.koeri.boun.edu.tr/scripts/lst0.asp"

# Year then date + time = a robust event_id (KOERI does not expose a real id).
_KOERI_LINE = re.compile(
    r"^\s*(?P<date>\d{4}\.\d{2}\.\d{2})\s+(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<lat>-?\d+\.\d+)\s+(?P<lon>-?\d+\.\d+)\s+(?P<depth>-?\d+\.\d+)\s+"
    r"(?P<md>[\-.\d]+)\s+(?P<ml>[\-.\d]+)\s+(?P<mw>[\-.\d]+)\s+"
    r"(?P<rest>.+?)\s*$"
)


def _koeri_mag(token: str) -> float | None:
    if not token or token == "-.-":
        return None
    try:
        return float(token)
    except ValueError:
        return None


def fetch_koeri() -> FetchResult:
    r = requests.get(KOERI_URL, headers=_ua_headers(), timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "iso-8859-9"
    raw = r.text.encode(r.encoding or "utf-8", errors="replace")
    records = _parse_koeri_text(r.text)
    return FetchResult(raw_bytes=raw, raw_ext="html", records=records)


def parse_koeri_bytes(blob: bytes) -> list[dict[str, Any]]:
    """Replay-mode parse for an archived KOERI HTML blob."""
    text = blob.decode("iso-8859-9", errors="replace")
    return _parse_koeri_text(text)


def _parse_koeri_text(text: str) -> list[dict[str, Any]]:
    m = re.search(r"<pre[^>]*>(.*?)</pre>", text, flags=re.I | re.S)
    if not m:
        return []
    block = m.group(1)
    out: list[dict[str, Any]] = []
    for line in block.splitlines():
        if not line.strip() or "Tarih" in line or "Date" in line or line.strip().startswith("-"):
            continue
        match = _KOERI_LINE.match(line)
        if not match:
            continue
        d = match.groupdict()
        rest = d["rest"].strip()
        # rest typically is "<place ...>  <Manual|Auto|REVISED>"
        #quality tag (last whitespace-separated token if it looks like a status).
        place = rest
        quality = None
        toks = rest.rsplit(maxsplit=1)
        if len(toks) == 2 and toks[1].lower() in ("manual", "automatic", "auto",
                                                   "revised"):
            place, quality = toks[0].strip(), toks[1]
        record = {
            "date":      d["date"].replace(".", "-"),
            "time":      d["time"],
            "latitude":  float(d["lat"]),
            "longitude": float(d["lon"]),
            "depth_km":  float(d["depth"]),
            "md":        _koeri_mag(d["md"]),
            "ml":        _koeri_mag(d["ml"]),
            "mw":        _koeri_mag(d["mw"]),
            "place":     place,
            "quality":   quality,
        }
        eid = f"KOERI-{record['date']}T{record['time']}-{record['latitude']:.3f}-{record['longitude']:.3f}"
        out.append({"event_id": eid, "payload": record})
    return out



# Day window helper used by the historical backfill (chunks the query so we dont exceed rate lim
def daterange_days(start: datetime, end: datetime, chunk_days: int = 1):
    """Yield (chunk_start, chunk_end) pairs covering [start, end)."""
    cursor = start
    delta = timedelta(days=chunk_days)
    while cursor < end:
        nxt = min(cursor + delta, end)
        yield cursor, nxt
        cursor = nxt


def polite_sleep(seconds: float = 0.5) -> None:
    time.sleep(seconds)
