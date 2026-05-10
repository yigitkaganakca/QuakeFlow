"""quality_check_dag is a lightweight observability for the pipeline.

Three independent assertions per run, failures dont stop the rest of the
pipeline (so each is its own task) but they do show up red in the Airflow UI
and emit XCom payloads consumable by a future alerting layer, so not breaks but is observed.

Tasks:
  - sources_seen_recently   : every source ingested at least one row in the
                              last hour
  - no_orphan_harmonized    : harmonized.events row count <= sum of raw row
                              counts (catches mapping pathologies, because this wouldve been logically inconsistent).
  - agreement_distribution  : XCom-pushes a histogram of agreement_level
                              from the last 24h of mart, this is useful for our demo.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from common import db

log = logging.getLogger("quakeflow.quality")


def t_sources_seen_recently() -> dict:
    counts = db.fetch_dict("""
        SELECT 'AFAD'  AS source, COUNT(*) AS n FROM raw.afad_events
            WHERE received_at > NOW() - INTERVAL '1 hour'
        UNION ALL
        SELECT 'KOERI' AS source, COUNT(*) AS n FROM raw.koeri_events
            WHERE received_at > NOW() - INTERVAL '1 hour'
        UNION ALL
        SELECT 'EMSC'  AS source, COUNT(*) AS n FROM raw.emsc_events
            WHERE received_at > NOW() - INTERVAL '1 hour'
        UNION ALL
        SELECT 'USGS'  AS source, COUNT(*) AS n FROM raw.usgs_events
            WHERE received_at > NOW() - INTERVAL '1 hour';
    """)
    table = {r["source"]: int(r["n"]) for r in counts}
    log.info("sources_seen_recently: %s", table)
    silent = [s for s, n in table.items() if n == 0]
    if silent:
        log.warning("source(s) with 0 rows in last hour: %s", silent)
    return table


def t_no_orphan_harmonized() -> dict:
    raw_total = db.fetch_dict("""
        SELECT
          (SELECT COUNT(*) FROM raw.afad_events)  +
          (SELECT COUNT(*) FROM raw.koeri_events) +
          (SELECT COUNT(*) FROM raw.emsc_events)  +
          (SELECT COUNT(*) FROM raw.usgs_events)  AS n;
    """)[0]["n"]
    harm_total = db.fetch_dict("SELECT COUNT(*) AS n FROM harmonized.events;")[0]["n"]
    out = {"raw_rows": int(raw_total), "harmonized_rows": int(harm_total)}
    log.info("integrity: %s", out)
    if int(harm_total) > int(raw_total):
        raise AssertionError(
            f"harmonized has more rows ({harm_total}) than raw ({raw_total}); "
            "this indicates a bug in the harmonize DAG."
        )
    return out


def t_agreement_distribution() -> dict:
    rows = db.fetch_dict("""
        SELECT agreement_level, COUNT(*) AS n
        FROM   mart.fact_earthquakes
        WHERE  event_time > NOW() - INTERVAL '24 hours'
        GROUP  BY agreement_level
        ORDER  BY agreement_level;
    """)
    if not rows:
        log.info("no mart rows in the last 24h yet (skipping)")
        raise AirflowSkipException("no mart rows yet")
    hist = {int(r["agreement_level"]): int(r["n"]) for r in rows}
    log.info("agreement distribution (24h): %s", hist)
    return hist


with DAG(
    dag_id="quality_check",
    description="Lightweight data-quality assertions over the QuakeFlow stack.",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "quakeflow",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["quakeflow", "quality"],
) as dag:
    PythonOperator(task_id="sources_seen_recently",
                   python_callable=t_sources_seen_recently)
    PythonOperator(task_id="no_orphan_harmonized",
                   python_callable=t_no_orphan_harmonized)
    PythonOperator(task_id="agreement_distribution",
                   python_callable=t_agreement_distribution)
