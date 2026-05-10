
-- 00_schemas.sql

-- as we state in our prooposal and report we used a Postgres-side medallion layout for QuakeFlow.
--   raw         : one table per source, payload kept verbatim (bronze)
--   harmonized  : one canonical schema across all sources    (silver)
--   mart        : deduplicated fact_earthquakes              (gold)

-- this file is the very fisrt thing the operational Postgres container runs at
-- first boot (mounted under /docker-entrypoint-initdb.d/).


CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS harmonized;
CREATE SCHEMA IF NOT EXISTS mart;

COMMENT ON SCHEMA raw         IS 'Bronze layer: per-source ingested payloads, kept verbatim for replay/audit.';
COMMENT ON SCHEMA harmonized  IS 'Silver layer: canonical column names/types across all sources.';
COMMENT ON SCHEMA mart        IS 'Gold layer: deduplicated fact tables consumed by Elasticsearch and BI. So this is what enduser sees.';
