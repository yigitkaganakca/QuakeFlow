
-- 01_raw_tables.sql
--
-- One table per upstream agency. We keep the original payload as JSONB so that
-- nothing the agency reported is ever lost. and so if our parser changes we can
-- re-derive the harmonized rows by simply reading these tables (or by
-- replaying from MinIO which is our immutable archive, u can check and see docs/REPLAY.md).
--
-- All 4 tables here shares the same shape below
--   event_id      : the agency's own identifier (PK along with source)
--   payload       : the response (object for JSON, single record / line for HTML)
--   received_at   : when our pipeline saw it (ingest watermark)
--   archive_uri   : pointer to the immutable raw blob in MinIO (s3a://...) this is audit handle as a link back to our imm. archive


CREATE TABLE IF NOT EXISTS raw.afad_events (
    event_id     TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archive_uri  TEXT,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS raw.koeri_events (
    event_id     TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archive_uri  TEXT,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS raw.emsc_events (
    event_id     TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archive_uri  TEXT,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS raw.usgs_events (
    event_id     TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archive_uri  TEXT,
    PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS ix_raw_afad_received   ON raw.afad_events  (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_raw_koeri_received  ON raw.koeri_events (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_raw_emsc_received   ON raw.emsc_events  (received_at DESC);
CREATE INDEX IF NOT EXISTS ix_raw_usgs_received   ON raw.usgs_events  (received_at DESC);
