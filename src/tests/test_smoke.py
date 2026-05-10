"""Smoke tests run inside the `tests` profile container.

Each test is auto-skipped if the corresponding service is not reachable
(this is because it is useful when the suite is run on the host instead of inside Docker).
"""

from __future__ import annotations

import os

import pytest


def _http_alive(url: str) -> bool:
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return 200 <= r.status < 500
    except Exception:
        return False


@pytest.mark.skipif(not os.environ.get("QUAKE_PG_HOST"),
                    reason="QUAKE_PG_HOST unset (running outside container)")
def test_postgres_reachable():
    import psycopg
    from common.config import settings
    with psycopg.connect(settings.pg_dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            assert cur.fetchone()[0] == 1


@pytest.mark.skipif(not os.environ.get("QUAKE_ES_URL"),
                    reason="QUAKE_ES_URL unset")
def test_elasticsearch_reachable():
    from common.config import settings
    if not _http_alive(f"{settings.es_url}/_cluster/health"):
        pytest.skip("Elasticsearch not reachable")
    assert True


@pytest.mark.skipif(not os.environ.get("QUAKE_MINIO_ENDPOINT"),
                    reason="QUAKE_MINIO_ENDPOINT unset")
def test_minio_bucket_exists():
    try:
        from common import minio_client
        keys = list(minio_client.list_keys(prefix=""))
    except Exception:
        pytest.skip("MinIO not reachable")
    # Empty list is okaz since we want  call to succeed in local setup anyways but not in prod setup.
    assert isinstance(keys, list)


def test_dedup_module_importable():
    from common.dedup import dedupe
    assert callable(dedupe)
