"""Spatiotemporal deduplication of harmonized events.

This is also documented in docs/DEDUPE.md and the report.

Inputs are dicts with the harmonized.events schema:
    source, event_id, event_time (UTC ISO or datetime),
    latitude, longitude, depth_km, magnitude, mag_type,
    place, province, district

Output is a list of cluster dicts ready to UPSERT into
mart.fact_earthquakes:
    event_uid, event_time, latitude, longitude, depth_km, magnitude,
    mag_type, place, province, district,
    preferred_source, agreement_level, sources, source_values,
    first_seen_at

Algorithm (single pass over time-sorted events):

    1. Sort harmonized rows by event_time ascending.
    2. Maintain an "active window" of recent clusters (cluster.event_time
       within DT_WINDOW seconds of current event).
    3. For each new event e:
         pick the cluster c in the active window where
              dt(e, c)        <= DT_WINDOW       (default 30s)
         and  haversine(e, c) <= DIST_KM_WINDOW  (default 20 km)
         and  |dM(e, c)|      <= DM_WINDOW       (default 1.0)
         using the smallest dt as the tiebreaker.
       If no such cluster exists then the e starts a new cluster.
    4. After all events have been assigned, for each cluster pick a
       "preferred source" using SOURCE_PRIORITY (AFAD > KOERI > EMSC > USGS,
       i.e. we give the local authorities first because we thought that matches operational practice of
       Turkish geoscientists).
    5. Build the mart row from the cluster: preferred fields from the
       preferred source, agreement_level = COUNT DISTINCT cluster.source,
       and source_values JSON for drill-down.

Idempotency: event_uid is a deterministic hash of the cluster centroid
(time bucket, lat/lon rounded) so re-running on the same input yields
the same UPSERT key.
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence

# these are efault thresholds but can be overridden by callers (DAG / tests).
# defaults tuned against the smoke-test data from AFAD and KOERI
# routinely report shared events with ~30-50 km epicentre offsets and
# 30-90 s reporting-latency differences, owing to their distinct sismoghrahic networks. This is where we think data-tuned 
# or expertise opinion would matter for future work.
DT_WINDOW_S    = 90.0
DIST_KM_WINDOW = 50.0
DM_WINDOW      = 1.0

# lower index means higher priority. AFAD and KOERI are local authorities
# the global feeds back-stop them.
SOURCE_PRIORITY = ("AFAD", "KOERI", "EMSC", "USGS")


#helpers


def _to_dt(v):
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        # `datetime.fromisoformat` handles "2025-01-01T00:06:56+00:00".
        # Some inputs use 'Z' for UTC (USGS does) so we normalize.
        s = v.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # Last-resort: drop fractional micro precision tail.
            dt = datetime.fromisoformat(s.split(".")[0] + s[s.find("+"):]
                                        if "+" in s else s.split(".")[0])
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise TypeError(f"unsupported event_time: {v!r}")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def _priority_index(source: str) -> int:
    try:
        return SOURCE_PRIORITY.index(source)
    except ValueError:
        return len(SOURCE_PRIORITY)  # unknown sources sink to last

#cluster
@dataclass
class _Cluster:
    """Mutable accumulator used during the single-pass dedup walk."""

    members: list[dict] = field(default_factory=list)

    @property
    def t_min(self) -> datetime:
        return min(_to_dt(m["event_time"]) for m in self.members)

    @property
    def t_max(self) -> datetime:
        return max(_to_dt(m["event_time"]) for m in self.members)

    @property
    def centroid(self) -> tuple[float, float]:
        n = len(self.members)
        return (
            sum(m["latitude"]  for m in self.members) / n,
            sum(m["longitude"] for m in self.members) / n,
        )

    @property
    def avg_mag(self) -> float | None:
        mags = [m.get("magnitude") for m in self.members
                if m.get("magnitude") is not None]
        return sum(mags) / len(mags) if mags else None

    def add(self, ev: Mapping) -> None:
        self.members.append(dict(ev))

    def _preferred(self) -> dict:
        return min(
            self.members,
            key=lambda m: (_priority_index(m["source"]),
                           _to_dt(m["event_time"])),
        )

    def to_mart_row(self,
                    dt_window_s: float = DT_WINDOW_S) -> dict:
        pref = self._preferred()
        srcs = sorted({m["source"] for m in self.members})
        first = self.t_min

        # event_uid: stable hash that does not change when the same physical
        # event re-arrives via a slow source; bucket time to dt_window_s.
        bucket_secs = int(first.timestamp() // dt_window_s) * int(dt_window_s)
        lat_q = round(pref["latitude"],  2)
        lon_q = round(pref["longitude"], 2)
        seed = f"{bucket_secs}|{lat_q:.2f}|{lon_q:.2f}"
        event_uid = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]

        # Per-source breakdown for Kibana panels.
        source_values = {}
        for m in self.members:
            t = _to_dt(m["event_time"]).isoformat()
            source_values[m["source"]] = {
                "event_id":  m.get("event_id"),
                "magnitude": m.get("magnitude"),
                "mag_type":  m.get("mag_type"),
                "depth_km":  m.get("depth_km"),
                "event_time": t,
                "latitude":  m.get("latitude"),
                "longitude": m.get("longitude"),
                "place":     m.get("place"),
            }

        return {
            "event_uid":        event_uid,
            "event_time":       first,
            "latitude":         pref["latitude"],
            "longitude":        pref["longitude"],
            "depth_km":         pref.get("depth_km"),
            "magnitude":        pref.get("magnitude"),
            "mag_type":         pref.get("mag_type"),
            "place":            pref.get("place"),
            "province":         pref.get("province"),
            "district":         pref.get("district"),
            "preferred_source": pref["source"],
            "agreement_level":  len(srcs),
            "sources":          srcs,
            "source_values":    source_values,
            "first_seen_at":    first,
        }



def dedupe(events: Iterable[Mapping],
           dt_window_s:    float = DT_WINDOW_S,
           dist_km_window: float = DIST_KM_WINDOW,
           dm_window:      float = DM_WINDOW) -> list[dict]:
    """Cluster harmonized events into mart rows.

    Stable, deterministic, idempotent. See module docstring for algorithm.
    """
    rows = sorted(events, key=lambda e: _to_dt(e["event_time"]))
    clusters: list[_Cluster] = []

    for ev in rows:
        ev_t   = _to_dt(ev["event_time"])
        ev_lat = ev["latitude"]
        ev_lon = ev["longitude"]
        ev_mag = ev.get("magnitude")

        best: tuple[float, _Cluster] | None = None
        for c in reversed(clusters):  # walk newest-first cheap early-exit
            dt_seconds = (ev_t - c.t_max).total_seconds()
            if dt_seconds > dt_window_s:
                # Anything older cannot be a match either (that is rows are sorted).
                break
            c_lat, c_lon = c.centroid
            if haversine_km(ev_lat, ev_lon, c_lat, c_lon) > dist_km_window:
                continue
            c_mag = c.avg_mag
            if (ev_mag is not None and c_mag is not None
                    and abs(ev_mag - c_mag) > dm_window):
                continue
            # here prefer the cluster with the smallest |dt|.
            score = abs(dt_seconds)
            if best is None or score < best[0]:
                best = (score, c)

        if best is None:
            cluster = _Cluster()
            cluster.add(ev)
            clusters.append(cluster)
        else:
            best[1].add(ev)

    return [c.to_mart_row(dt_window_s=dt_window_s) for c in clusters]


def dedupe_summary(rows: Sequence[Mapping]) -> dict[int, int]:
    """Histogram of agreement_level. Used by the quality-check DAG."""
    out = defaultdict(int)
    for r in rows:
        out[int(r.get("agreement_level", 1))] += 1
    return dict(out)
