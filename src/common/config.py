"""Single source of truth for environment driven configuration of ours. Here we deliberately avoid pydantic / dynaconf to keep the dependency surface
small inside the Airflow image. All values are read once at import time from environment variables that the docker-compose file populates.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    val = os.environ.get(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"required env var '{name}' is not set")
    return val


@dataclass(frozen=True)
class Settings:
    pg_host:        str
    pg_port:        int
    pg_user:        str
    pg_password:    str
    pg_db:          str

    es_url:         str
    es_index:       str

    minio_endpoint: str
    minio_access:   str
    minio_secret:   str
    minio_bucket:   str

    user_agent:     str

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} "
            f"user={self.pg_user} password={self.pg_password} "
            f"dbname={self.pg_db}"
        )

    @property
    def pg_sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


def _load() -> Settings:
    return Settings(
        pg_host        = _env("QUAKE_PG_HOST",        "postgres"),
        pg_port        = int(_env("QUAKE_PG_PORT",    "5432") or "5432"),
        pg_user        = _env("QUAKE_PG_USER",        required=True) or "",
        pg_password    = _env("QUAKE_PG_PASSWORD",    required=True) or "",
        pg_db          = _env("QUAKE_PG_DB",          required=True) or "",
        es_url         = _env("QUAKE_ES_URL",         "http://elasticsearch:9200") or "",
        es_index       = _env("QUAKE_ES_INDEX",       "quakes") or "quakes",
        minio_endpoint = _env("QUAKE_MINIO_ENDPOINT", "http://minio:9000") or "",
        minio_access   = _env("QUAKE_MINIO_ACCESS_KEY", "") or "",
        minio_secret   = _env("QUAKE_MINIO_SECRET_KEY", "") or "",
        minio_bucket   = _env("QUAKE_MINIO_BUCKET",   "quake-raw") or "quake-raw",
        user_agent     = _env("USER_AGENT",
                              "ITU-YZV322E-QuakeFlow/1.0 (academic)") or "",
    )


# fpr spme ligt tests we onlz want to import the module without a
# fully populated environment. to handle that, fall back to a partially loaded settings object
# in that case so import never fails.
try:
    settings = _load()
except RuntimeError:
    settings = Settings(
        pg_host="postgres", pg_port=5432, pg_user="", pg_password="", pg_db="",
        es_url="http://elasticsearch:9200", es_index="quakes",
        minio_endpoint="http://minio:9000",
        minio_access="", minio_secret="", minio_bucket="quake-raw",
        user_agent="ITU-YZV322E-QuakeFlow/1.0 (academic)",
    )
