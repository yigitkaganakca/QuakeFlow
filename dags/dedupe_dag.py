"""dedupe_dag - silver (harmonized) -> gold (mart.fact_earthquakes). this is the final step to create user end.

Schedule: every 2 minutes (1 min after the harmonize DAG so it has fresh
data to consume).

The actual clustering algorithm lives in common.dedup and this file is a
Airflow wrapper that:
    1) reads a sliding window of the last DEDUPE_WINDOW_DAYS days from
       harmonized.events,
    2) calls common.dedup.dedupe to produce mart rows,
    3) UPSERTs into mart.fact_earthquakes using the deterministic event_uid
       as the primary key (idempotent across retries / re-runs).

here we chose a sliding window instead of incrmeental with discussions with AI assistance too we concluded that:
  - dedup is O(N * window) where window is bounded by DT_WINDOW_S, so the
    total cost stays small 
  - It is the simplest way to guarantee correctness when a slow source
    reports an event 5 hours after the fact, the next dedup pass picks it up
    and the existing event_uid gets its agreement_level bumped so as expected.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import db
from common.dedup import dedupe, dedupe_summary

log = logging.getLogger("quakeflow.dedupe")

DEDUPE_WINDOW_DAYS = 30


def run_dedupe() -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=DEDUPE_WINDOW_DAYS)).isoformat()
    log.info("loading harmonized events with event_time >= %s", cutoff)

    rows = db.fetch_dict(
        """
        SELECT source, event_id, event_time, latitude, longitude,
               depth_km, magnitude, mag_type, place, province, district
        FROM   harmonized.events
        WHERE  event_time >= %s::timestamptz;
        """,
        (cutoff,),
    )
    log.info("loaded %d harmonized rows for dedup", len(rows))
    if not rows:
        return

    mart_rows = dedupe(rows)
    log.info("produced %d mart rows; agreement histogram=%s",
             len(mart_rows), dedupe_summary(mart_rows))

    n = db.upsert_mart(mart_rows)
    log.info("upserted %d rows into mart.fact_earthquakes", n)


with DAG(
    dag_id="dedupe",
    description=("Spatiotemporal dedup of harmonized events -> "
                 "mart.fact_earthquakes (single task, idempotent)."),
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval="1-59/2 * * * *",  #offset from harmonize by 1 min
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "quakeflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["quakeflow", "gold"],
) as dag:
    PythonOperator(
        task_id="dedupe_window",
        python_callable=run_dedupe,
    )
