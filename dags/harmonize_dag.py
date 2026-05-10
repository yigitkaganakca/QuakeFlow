"""harmonize_dag - bronze (raw) -> silver (harmonized).

Schedule: every 2 minutes. Reads new rows from raw.<source>_events using a
per-source watermark on received_at, maps them through
common.mapping.map_row, and UPSERTs into harmonized.events.

Idempotency# ON CONFLICT (source, event_id) DO UPDATE in upsert_harmonized.
Watermark advanced only after a successful upsert and if the task fails Airflow
will retry from the same watermark.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import db
from common.mapping import map_row

log = logging.getLogger("quakeflow.harmonize")

SOURCES = (
    ("AFAD",  "raw.afad_events"),
    ("KOERI", "raw.koeri_events"),
    ("EMSC",  "raw.emsc_events"),
    ("USGS",  "raw.usgs_events"),
)

BATCH_SIZE = 5000


def _watermark_name(source: str) -> str:
    return f"harmonize::{source}"


def harmonize_one_source(source: str, table: str) -> None:
    wm_name = _watermark_name(source)
    cursor = db.get_watermark(wm_name)
    log.info("source=%s watermark=%s", source, cursor)

    rows = db.fetch_dict(
        f"""
        SELECT event_id, payload, archive_uri, received_at
        FROM   {table}
        WHERE  received_at > %s::timestamptz
        ORDER  BY received_at ASC
        LIMIT  %s;
        """,
        (cursor, BATCH_SIZE),
    )
    if not rows:
        log.info("source=%s nothing new since %s", source, cursor)
        return

    canonical: list[dict] = []
    for r in rows:
        c = map_row(source, r["event_id"], r["payload"])
        if c is None:
            continue
        c["archive_uri"] = r.get("archive_uri")
        canonical.append(c)

    n = db.upsert_harmonized(canonical)
    new_wm = max(r["received_at"] for r in rows).isoformat()
    db.set_watermark(wm_name, new_wm)
    log.info("source=%s mapped=%d upserted=%d new_watermark=%s",
             source, len(rows), n, new_wm)


with DAG(
    dag_id="harmonize",
    description="Map raw.<source>_events into harmonized.events",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "quakeflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["quakeflow", "silver"],
) as dag:
    for src, tbl in SOURCES:
        PythonOperator(
            task_id=f"harmonize_{src.lower()}",
            python_callable=harmonize_one_source,
            op_kwargs={"source": src, "table": tbl},
        )
