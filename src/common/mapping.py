"""Per-source mapping into the canonical `harmonized.events` schema.

Each function takes the raw `payload` JSON object exactly as we stored it
in raw.<source>_events and returns a dict ready to UPSERT into
harmonized.events. None is returned if the row is unusable
(missing required field) that is the caller should skip those.

Required canonical fields:
    source, event_id, event_time (UTC ISO), latitude, longitude
Optional ones are:
    depth_km, magnitude, mag_type, place, province, district
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

#magnitude precedence decided on and used when an agency returns multiple scales.
KOERI_MAG_PRECEDENCE = ("mw", "ml", "md")

#helpers we needed learned when debugging
def _f(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _afad_time(value: str) -> str | None:
    #AFAD returns local Turkish time without timezone info so we treat as UTC+3.
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        from datetime import timedelta
        dt = dt.replace(tzinfo=timezone(timedelta(hours=3)))
    return dt.astimezone(timezone.utc).isoformat()


def _ms_to_iso(ms: int | float | None) -> str | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None

def afad_to_canonical(event_id: str, payload: Mapping[str, Any]
                      ) -> dict[str, Any] | None:
    lat = _f(payload.get("latitude"))
    lon = _f(payload.get("longitude"))
    if lat is None or lon is None:
        return None
    return {
        "source":     "AFAD",
        "event_id":   event_id,
        "event_time": _afad_time(str(payload.get("date") or "")),
        "latitude":   lat,
        "longitude":  lon,
        "depth_km":   _f(payload.get("depth")),
        "magnitude":  _f(payload.get("magnitude")),
        "mag_type":   payload.get("type"),
        "place":      payload.get("location"),
        "province":   payload.get("province"),
        "district":   payload.get("district"),
    }

def koeri_to_canonical(event_id: str, payload: Mapping[str, Any]
                       ) -> dict[str, Any] | None:
    lat = _f(payload.get("latitude"))
    lon = _f(payload.get("longitude"))
    if lat is None or lon is None:
        return None
    mag, mag_type = None, None
    for k in KOERI_MAG_PRECEDENCE:
        v = payload.get(k)
        if v is not None:
            mag, mag_type = float(v), k.upper()
            break
    date = payload.get("date")
    time = payload.get("time")
    if not (date and time):
        return None
    # KOERI publishes Türkiye local time (UTC+3, no DST in the published table).
    from datetime import timedelta
    iso_local = f"{date}T{time}"
    try:
        dt_local = datetime.fromisoformat(iso_local).replace(
            tzinfo=timezone(timedelta(hours=3))
        )
    except ValueError:
        return None
    return {
        "source":     "KOERI",
        "event_id":   event_id,
        "event_time": dt_local.astimezone(timezone.utc).isoformat(),
        "latitude":   lat,
        "longitude":  lon,
        "depth_km":   _f(payload.get("depth_km")),
        "magnitude":  mag,
        "mag_type":   mag_type,
        "place":      payload.get("place"),
        "province":   None,
        "district":   None,
    }

def emsc_to_canonical(event_id: str, payload: Mapping[str, Any]
                      ) -> dict[str, Any] | None:
    props = payload.get("properties") or {}
    lat = _f(props.get("lat"))
    lon = _f(props.get("lon"))
    if lat is None or lon is None:
        return None
    return {
        "source":     "EMSC",
        "event_id":   event_id,
        "event_time": props.get("time"),  # already UTC ISO
        "latitude":   lat,
        "longitude":  lon,
        "depth_km":   _f(props.get("depth")),
        "magnitude":  _f(props.get("mag")),
        "mag_type":   props.get("magtype"),
        "place":      props.get("flynn_region"),
        "province":   None,
        "district":   None,
    }


def usgs_to_canonical(event_id: str, payload: Mapping[str, Any]
                      ) -> dict[str, Any] | None:
    props = payload.get("properties") or {}
    geom = payload.get("geometry") or {}
    coords = geom.get("coordinates") or []
    if len(coords) < 2:
        return None
    lon, lat = _f(coords[0]), _f(coords[1])
    depth = _f(coords[2]) if len(coords) >= 3 else None
    if lat is None or lon is None:
        return None
    return {
        "source":     "USGS",
        "event_id":   event_id,
        "event_time": _ms_to_iso(props.get("time")),
        "latitude":   lat,
        "longitude":  lon,
        "depth_km":   depth,
        "magnitude":  _f(props.get("mag")),
        "mag_type":   props.get("magType"),
        "place":      props.get("place"),
        "province":   None,
        "district":   None,
    }

#registru
MAPPERS = {
    "AFAD":  afad_to_canonical,
    "KOERI": koeri_to_canonical,
    "EMSC":  emsc_to_canonical,
    "USGS":  usgs_to_canonical,
}


def map_row(source: str, event_id: str, payload: Mapping[str, Any]
            ) -> dict[str, Any] | None:
    fn = MAPPERS.get(source)
    if fn is None:
        raise ValueError(f"unknown source: {source}")
    return fn(event_id, payload)
