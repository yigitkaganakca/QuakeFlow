
-- 04_views.sql
--
-- These are just the views for demo / pgAdmin inspection. None of the application
-- code depends on these but we cretaed them and  they exist purely to make the live demo readable.


CREATE OR REPLACE VIEW mart.v_latest AS
SELECT  event_time, magnitude, mag_type, depth_km,
        province, district, place,
        agreement_level, sources, preferred_source,
        latitude, longitude
FROM    mart.fact_earthquakes
ORDER BY event_time DESC
LIMIT   500;

CREATE OR REPLACE VIEW mart.v_disagreement AS
SELECT  event_uid,
        event_time,
        magnitude AS preferred_magnitude,
        preferred_source,
        agreement_level,
        sources,
        source_values
FROM    mart.fact_earthquakes
WHERE   agreement_level >= 2
ORDER BY event_time DESC
LIMIT   500;

CREATE OR REPLACE VIEW mart.v_by_province AS
SELECT  province,
        COUNT(*)                                 AS n_events,
        ROUND(AVG(magnitude)::numeric, 2)        AS avg_magnitude,
        MAX(magnitude)                           AS max_magnitude,
        MIN(event_time)                          AS first_event,
        MAX(event_time)                          AS last_event
FROM    mart.fact_earthquakes
WHERE   province IS NOT NULL
GROUP BY province
ORDER BY n_events DESC;
