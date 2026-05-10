"""live_ingest_dag - Airflow-side live polling, complementing NiFi.

NiFi handles the primary 60-second live ingestion. This DAG runs every 5 minutes and pulls a
short window from each source: if the NiFi
flow needs operator attention the pipeline still receives fresh data.

Both writers use ON CONFLICT (event_id) DO NOTHING so the two paths can
co-exist without producing duplicates which is important for idempotecny.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import db, minio_client, sources

log = logging.getLogger("quakeflow.live_ingest")


def _archive(source: str, blob: bytes, ext: str) -> str:
    when = datetime.now(timezone.utc)
    key = minio_client.archive_key(source, ext, when=when)
    ctype = "application/json" if ext == "json" else "text/html"
    return minio_client.put_blob(key, blob, content_type=ctype)


def t_pull_afad() -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=60)
    res = sources.fetch_afad(start, end, min_mag=1.0)
    uri = _archive("AFAD", res.raw_bytes, res.raw_ext)
    n = db.upsert_raw("AFAD", res.records, archive_uri=uri)
    log.info("AFAD live: %d records (%s)", n, uri)
    return n


def t_pull_emsc() -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=1)
    res = sources.fetch_emsc(start, end, min_mag=1.5)
    uri = _archive("EMSC", res.raw_bytes, res.raw_ext)
    n = db.upsert_raw("EMSC", res.records, archive_uri=uri)
    log.info("EMSC live: %d records", n)
    return n


def t_pull_usgs() -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    res = sources.fetch_usgs(start, end, min_mag=2.5)
    uri = _archive("USGS", res.raw_bytes, res.raw_ext)
    n = db.upsert_raw("USGS", res.records, archive_uri=uri)
    log.info("USGS live: %d records", n)
    return n


def t_pull_koeri() -> int:
    res = sources.fetch_koeri()
    uri = _archive("KOERI", res.raw_bytes, res.raw_ext)
    n = db.upsert_raw("KOERI", res.records, archive_uri=uri)
    log.info("KOERI live: %d records", n)
    return n


with DAG(
    dag_id="live_ingest",
    description=("Airflow-side live polling - back-up to NiFi. "
                 "Both write into raw.<source>_events with ON CONFLICT DO NOTHING."),
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "quakeflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["quakeflow", "ingest"],
) as dag:
    PythonOperator(task_id="afad",  python_callable=t_pull_afad)
    PythonOperator(task_id="emsc",  python_callable=t_pull_emsc)
    PythonOperator(task_id="usgs",  python_callable=t_pull_usgs)
    PythonOperator(task_id="koeri", python_callable=t_pull_koeri)
