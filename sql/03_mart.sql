
-- 03_mart.sql

-- this is the final user facing the deduplicated fact table. just one row per real world earthquake, regardless
-- of how many agencies reported it. agreement_level (1..4) tells dashboard
-- consumers how trustworthy the event is at a glance which is important for scientitst as we state in the report.

-- The full per-source breakdown is preserved in source_values JSONB, so a
-- click-through in Kibana can show "AFAD said mag=4.3 at 14:03:11; KOERI said
-- mag=4.1 at 14:03:15" without having to re-join harmonized.events.


CREATE TABLE IF NOT EXISTS mart.fact_earthquakes (
    event_uid        TEXT             PRIMARY KEY,
    event_time       TIMESTAMPTZ      NOT NULL,
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    depth_km         DOUBLE PRECISION,
    magnitude        DOUBLE PRECISION,
    mag_type         TEXT,
    place            TEXT,
    province         TEXT,
    district         TEXT,
    preferred_source TEXT             NOT NULL,
    agreement_level  SMALLINT         NOT NULL CHECK (agreement_level BETWEEN 1 AND 4),
    sources          TEXT[]           NOT NULL,
    source_values    JSONB            NOT NULL,
    first_seen_at    TIMESTAMPTZ      NOT NULL,
    updated_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_mart_event_time  ON mart.fact_earthquakes (event_time DESC);
CREATE INDEX IF NOT EXISTS ix_mart_magnitude   ON mart.fact_earthquakes (magnitude);
CREATE INDEX IF NOT EXISTS ix_mart_updated_at  ON mart.fact_earthquakes (updated_at DESC);
CREATE INDEX IF NOT EXISTS ix_mart_province    ON mart.fact_earthquakes (province);
CREATE INDEX IF NOT EXISTS ix_mart_sources_gin ON mart.fact_earthquakes USING GIN (sources);
