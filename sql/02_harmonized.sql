
-- 02_harmonized.sql
-- this is the single canonical event table. Every row knows which agency it came from
-- (source) and the agency's own id (event_id). PK is the pair so the same
-- physical earthquake reported by AFAD and KOERI lands as two rows here and at the end the dedup step in mart is what collapses them.


CREATE TABLE IF NOT EXISTS harmonized.events (
    source        TEXT             NOT NULL CHECK (source IN ('AFAD','KOERI','EMSC','USGS')),
    event_id      TEXT             NOT NULL,
    event_time    TIMESTAMPTZ      NOT NULL,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    depth_km      DOUBLE PRECISION,
    magnitude     DOUBLE PRECISION,
    mag_type      TEXT,
    place         TEXT,
    province      TEXT,
    district      TEXT,
    archive_uri   TEXT,
    harmonized_at TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, event_id)
);

CREATE INDEX IF NOT EXISTS ix_harm_event_time   ON harmonized.events (event_time DESC);
CREATE INDEX IF NOT EXISTS ix_harm_harm_at      ON harmonized.events (harmonized_at DESC);
CREATE INDEX IF NOT EXISTS ix_harm_geo          ON harmonized.events (latitude, longitude);
CREATE INDEX IF NOT EXISTS ix_harm_province     ON harmonized.events (province);

-- this is the watermark table so DAGs do not have to re-scan the whole raw set every run.
CREATE TABLE IF NOT EXISTS harmonized._watermarks (
    job_name      TEXT PRIMARY KEY,
    last_value    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
