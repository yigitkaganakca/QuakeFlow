"""Postgres helpers shared by the backfill container and the DAGs.

We use psycopg 3 with executemany / COPY-based bulk insert paths and
context-managed connections, nothing fancy actuallz here
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from typing import Iterable, Iterator, Mapping, Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from .config import settings

log = logging.getLogger("quakeflow.db")


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(settings.pg_dsn, autocommit=False)
    try:
        yield conn
    finally:
        conn.close()


def wait_until_ready(timeout_s: int = 90) -> None:
    """Block until the operational Postgres accepts a SELECT 1."""
    deadline = time.time() + timeout_s
    last: Exception | None = None
    while time.time() < deadline:
        try:
            with psycopg.connect(settings.pg_dsn, connect_timeout=3) as c:
                c.execute("SELECT 1")
            return
        except Exception as exc: 
            last = exc
            time.sleep(1)
    raise RuntimeError(f"postgres not reachable within {timeout_s}s: {last!r}")

_RAW_TABLES = {
    "AFAD":  "raw.afad_events",
    "KOERI": "raw.koeri_events",
    "EMSC":  "raw.emsc_events",
    "USGS":  "raw.usgs_events",
}


def upsert_raw(source: str, records: Iterable[Mapping[str, Any]],
               archive_uri: str | None = None) -> int:
    """Insert (or no-op on conflict) records into the per-source raw table.

    Each record must be {'event_id': str, 'payload': dict}. Returns the
    number of rows attempted (insert + skipped duplicates both count).
    """
    table = _RAW_TABLES[source]
    rows = list(records)
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {table} (event_id, payload, received_at, archive_uri)
        VALUES (%s, %s, NOW(), %s)
        ON CONFLICT (event_id) DO NOTHING;
    """
    params = [
        (r["event_id"], Json(r["payload"]), archive_uri) for r in rows
    ]
    with connect() as conn, conn.cursor() as cur:
        cur.executemany(sql, params)
        conn.commit()
    return len(rows)

def upsert_harmonized(rows: Iterable[Mapping[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    sql = """
        INSERT INTO harmonized.events (
            source, event_id, event_time, latitude, longitude,
            depth_km, magnitude, mag_type, place, province, district,
            archive_uri, harmonized_at
        ) VALUES (
            %(source)s, %(event_id)s, %(event_time)s, %(latitude)s, %(longitude)s,
            %(depth_km)s, %(magnitude)s, %(mag_type)s, %(place)s, %(province)s, %(district)s,
            %(archive_uri)s, NOW()
        )
        ON CONFLICT (source, event_id) DO UPDATE SET
            event_time   = EXCLUDED.event_time,
            latitude     = EXCLUDED.latitude,
            longitude    = EXCLUDED.longitude,
            depth_km     = EXCLUDED.depth_km,
            magnitude    = EXCLUDED.magnitude,
            mag_type     = EXCLUDED.mag_type,
            place        = EXCLUDED.place,
            province     = EXCLUDED.province,
            district     = EXCLUDED.district,
            archive_uri  = EXCLUDED.archive_uri,
            harmonized_at= NOW();
    """
    with connect() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()
    return len(rows)

def upsert_mart(rows: Iterable[Mapping[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    sql = """
        INSERT INTO mart.fact_earthquakes (
            event_uid, event_time, latitude, longitude,
            depth_km, magnitude, mag_type, place, province, district,
            preferred_source, agreement_level, sources, source_values,
            first_seen_at, updated_at
        ) VALUES (
            %(event_uid)s, %(event_time)s, %(latitude)s, %(longitude)s,
            %(depth_km)s, %(magnitude)s, %(mag_type)s, %(place)s, %(province)s, %(district)s,
            %(preferred_source)s, %(agreement_level)s, %(sources)s, %(source_values)s,
            COALESCE(%(first_seen_at)s, NOW()), NOW()
        )
        ON CONFLICT (event_uid) DO UPDATE SET
            event_time       = EXCLUDED.event_time,
            latitude         = EXCLUDED.latitude,
            longitude        = EXCLUDED.longitude,
            depth_km         = EXCLUDED.depth_km,
            magnitude        = EXCLUDED.magnitude,
            mag_type         = EXCLUDED.mag_type,
            place            = EXCLUDED.place,
            province         = EXCLUDED.province,
            district         = EXCLUDED.district,
            preferred_source = EXCLUDED.preferred_source,
            agreement_level  = EXCLUDED.agreement_level,
            sources          = EXCLUDED.sources,
            source_values    = EXCLUDED.source_values,
            updated_at       = NOW();
    """
    # source_values is a JSON document so we wrap it.
    materialised = []
    for r in rows:
        d = dict(r)
        if not isinstance(d.get("source_values"), Json):
            d["source_values"] = Json(d.get("source_values") or {})
        materialised.append(d)
    with connect() as conn, conn.cursor() as cur:
        cur.executemany(sql, materialised)
        conn.commit()
    return len(rows)

#watermark heplers
def get_watermark(job_name: str, default: str = "1970-01-01T00:00:00+00:00") -> str:
    with connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT last_value FROM harmonized._watermarks WHERE job_name = %s",
            (job_name,),
        )
        row = cur.fetchone()
        if row is None:
            return default
        return row["last_value"].isoformat()


def set_watermark(job_name: str, value_iso: str) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO harmonized._watermarks (job_name, last_value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (job_name) DO UPDATE SET
                last_value = EXCLUDED.last_value,
                updated_at = NOW();
            """,
            (job_name, value_iso),
        )
        conn.commit()


def fetch_dict(sql: str, params: tuple = ()) -> list[dict]:
    with connect() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())
