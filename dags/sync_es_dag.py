"""sync_es_dag - gold (mart) -> Elasticsearch (search + Kibana) the part to  create dashboards, we need to pass it to ES so this,.

reads any mart.fact_earthquakes rows whose updated_at is newer than the
watermark and bulk-indexes them into the `quakes` index. doc_id =
event_uid, so re-running is idempotent here too.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch, helpers

from common import db
from common.config import settings

log = logging.getLogger("quakeflow.sync_es")

WATERMARK = "sync_es::mart"
BATCH_SIZE = 2000


def _row_to_doc(row: dict) -> dict:
    et = row["event_time"]
    if hasattr(et, "isoformat"):
        et = et.isoformat()
    return {
        "_op_type": "index",
        "_index":   settings.es_index,
        "_id":      row["event_uid"],
        "_source": {
            "event_uid":       row["event_uid"],
            "event_time":      et,
            "location":        {"lat": row["latitude"], "lon": row["longitude"]},
            "latitude":        row["latitude"],
            "longitude":       row["longitude"],
            "depth_km":        row.get("depth_km"),
            "magnitude":       row.get("magnitude"),
            "mag_type":        row.get("mag_type"),
            "place":           row.get("place"),
            "province":        row.get("province"),
            "district":        row.get("district"),
            "preferred_source":row["preferred_source"],
            "agreement_level": int(row["agreement_level"]),
            "sources":         list(row["sources"] or []),
            "source_values":   row.get("source_values") or {},
        },
    }


def run_sync_es() -> None:
    cursor = db.get_watermark(WATERMARK)
    log.info("watermark=%s -> reading mart rows updated since", cursor)

    rows = db.fetch_dict(
        """
        SELECT event_uid, event_time, latitude, longitude, depth_km,
               magnitude, mag_type, place, province, district,
               preferred_source, agreement_level, sources, source_values,
               updated_at
        FROM   mart.fact_earthquakes
        WHERE  updated_at > %s::timestamptz
        ORDER  BY updated_at ASC
        LIMIT  %s;
        """,
        (cursor, BATCH_SIZE),
    )
    if not rows:
        log.info("no mart rows to sync")
        return

    es = Elasticsearch(settings.es_url, request_timeout=30)
    actions = (_row_to_doc(r) for r in rows)
    ok, errors = helpers.bulk(es, actions, raise_on_error=False, request_timeout=30)
    log.info("indexed %d docs into '%s' (errors=%s)",
             ok, settings.es_index, len(errors) if errors else 0)
    if errors:
        for e in errors[:5]:
            log.warning("ES bulk error sample: %s", e)

    new_wm = max(r["updated_at"] for r in rows).isoformat()
    db.set_watermark(WATERMARK, new_wm)
    log.info("new watermark=%s", new_wm)


with DAG(
    dag_id="sync_es",
    description="Push updated mart.fact_earthquakes rows into Elasticsearch.",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "quakeflow",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["quakeflow", "search"],
) as dag:
    PythonOperator(
        task_id="bulk_index",
        python_callable=run_sync_es,
    )
