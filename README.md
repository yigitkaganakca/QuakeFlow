# QuakeFlow

> **YZV 322E — Applied Data Engineering**, Spring 2026
> Final project: an end-to-end containerized data engineering pipeline that
> ingests, harmonizes, deduplicates and visualizes earthquake events
> reported by four independent agencies covering Türkiye and the
> surrounding region. Note: we have benefited AI  assistance for the creation of Readme, yet information here is verified.

| | |
|---|---|
| Course           | YZV 322E — Applied Data Engineering, Istanbul Technical University |
| Team             | İlhan Arda Yavuz, Yiğit Kağan Akça (team lead for communication) |
| Submission date  | May 10, 2026 |
| License          | MIT (see `LICENSE`) |


---

## 1. What QuakeFlow does

Each of the four upstream agencies — AFAD and KOERI/Kandilli (national),
EMSC (regional), USGS (global) — publishes earthquake events through a
different interface (two REST/JSON, one FDSN-JSON, one HTML table) and
under different identifiers, magnitude scales and reporting latencies.

QuakeFlow turns those four feeds into a single, deduplicated, queryable
catalogue, served as both a SQL fact table (`mart.fact_earthquakes`) and
an Elasticsearch index (`quakes`), with a Kibana dashboard on top.

The whole stack — every database, every helper script, every agent — runs
inside Docker. Bringing it up is a single command:

```bash
docker compose up --build
```

Within ~5 minutes of `git clone`, an evaluator's machine has:

* live ingestion of all four sources via Apache **NiFi** (60–300 s
  cadence, source-dependent) — visible on the canvas,
* every raw response archived immutably in **MinIO** so it can be
  replayed without re-hitting the agency,
* `harmonize → dedupe → sync_es` Airflow DAG chain materializing the
  deduplicated fact table and pushing updated rows to Elasticsearch,
* a **defence-in-depth** Airflow `live_ingest` DAG that runs every
  5 minutes and writes to the same tables (so either NiFi or Airflow
  alone is sufficient),
* a Kibana dashboard with a geographic map at <http://localhost:5601>.

If you also run the one-shot **historical backfill profile**, you'll have
30 days of past events in Postgres + MinIO before the first NiFi tick:

```bash
docker compose --profile backfill up --build backfill
```

---

## 2. Architecture

```
                ┌─ AFAD JSON  ─┐
                ├─ KOERI HTML ─┤   Apache    ┌── Postgres raw      ┐ Airflow ┌── Postgres mart ─┐
External agency │              │     NiFi    │   (4 source tables) │ harmoni-│  fact_earthquakes│ ─► Elasticsearch ─► Kibana
feeds (public)  ├─ EMSC FDSN  ─┤   (live)    │                     │ ze +    │  (deduped, gold) │
                └─ USGS FDSN  ─┘             └── Postgres harmonized── dedupe ┴───────────────────
                              ▲                  (canonical, silver)         │
                              │                                              │
                              │     ┌─────────────────────────────────┐      │
                              │     │  MinIO – immutable raw archive  │ ◄──── replay path
                              │     └─────────────────────────────────┘
                              │                                              ▲
                              ├──────── Airflow live_ingest (defence-in-depth)
                              │
                              └──────── Backfill container (historical / replay modes)
```

| Layer       | Tool                       | Rubric coverage                  |
|-------------|----------------------------|----------------------------------|
| Ingestion   | Apache NiFi 1.28           | ✓ course tool (visual data flow) |
| Storage     | PostgreSQL 16              | ✓ course tool                    |
| Storage UI  | pgAdmin 4                  | ✓ course tool                    |
| Archive     | MinIO (S3-compatible)      | supplementary tool               |
| Orchestrat. | Apache Airflow 2.10        | ✓ course tool                    |
| Search      | Elasticsearch 8.15         | ✓ course tool                    |
| Visualiz.   | Kibana 8.15                | ✓ course tool                    |

---

## 3. Quick start

### 3.1 Prerequisites

* Docker Desktop ≥ 24 (or Docker Engine + Compose v2.20+)
* ≥ 8 GB RAM allocated to Docker (Elasticsearch ~1 GB, NiFi ~1 GB,
  Airflow ~1.5 GB, Postgres ~512 MB)
* ≥ 10 GB free disk for images + volumes

### 3.2 Clone & launch

```bash
git clone <REPO_URL> AppliedTeam
cd AppliedTeam
cp .env.example .env          # edit only if you don't like the defaults
docker compose up --build
```

That's it. The Airflow scheduler / NiFi flow / Kibana dashboard
self-bootstrap.

### 3.3 Where the services live

| Service        | URL                          | Login                                   |
|----------------|------------------------------|-----------------------------------------|
| Airflow        | http://localhost:8088        | `admin` / `admin`                       |
| Kibana         | http://localhost:5601        | (anonymous, security disabled locally)  |
| NiFi           | http://localhost:8081/nifi   | `admin` / `adminadminadmin`             |
| pgAdmin        | http://localhost:5050        | `admin@itu.edu.tr` / `admin`            |
| MinIO console  | http://localhost:9001        | `quake` / `quake_minio_pw`              |
| Postgres (ops) | `localhost:5432` (db `quakes`) | `quake` / `quake_pw`                  |
| Elasticsearch  | http://localhost:9200        | (anonymous)                             |

### 3.4 Load 30 days of historical data

```bash
docker compose --profile backfill up --build backfill
```

Pulls AFAD/EMSC/USGS for the last 30 days, archives each raw response
to MinIO under `s3://quake-raw/<source>/<date>/<time>-<uuid>.<ext>`, and
loads parsed events into `raw.<source>_events`. Idempotent — running it
twice does not duplicate rows. Follow with the harmonize / dedupe DAGs
(triggered automatically every 2 minutes; you can also trigger them
manually in the Airflow UI).

KOERI is not part of the historical backfill because its public
endpoint only exposes a rolling 500-event snapshot — it is consumed
in streaming mode only by NiFi and the `live_ingest` DAG.

### 3.5 Replay from the immutable MinIO archive

The backfill container also runs in **replay mode**, which re-parses
the bytes already sitting in MinIO without making any API calls. This
proves the archive is the durable source of truth and supports parser
fixes, schema evolution, audit and disaster recovery:

```bash
$env:BACKFILL_MODE = "replay"     # PowerShell
docker compose --profile backfill up --build backfill
```

You can scope the replay to a single source via
`BACKFILL_SOURCES=AFAD` (default is the same set as historical mode).

### 3.6 Run the tests

```bash
docker compose --profile tests up --build tests
```

Runs the entire pytest suite (24 tests) inside its own container.

### 3.7 Tear it down

```bash
docker compose down            # stops services, keeps volumes
docker compose down -v         # also removes volumes (clean reset)
```

---

## 4. End-to-end data flow example

1. **NiFi** polls all four sources every 60–300 s (visible on the
   canvas) and flows each response through a parsing chain.
2. **Airflow `live_ingest`** DAG runs every 5 minutes as a
   defence-in-depth backup to NiFi: it pulls a short window from each
   source, archives the raw bytes to MinIO via `boto3`, and upserts
   into `raw.*` with `ON CONFLICT (event_id) DO NOTHING`.
3. **Airflow `harmonize`** DAG reads new raw rows (per-source watermark
   on `received_at`) and projects them into `harmonized.events` using
   `src/common/mapping.py`.
4. **Airflow `dedupe`** DAG runs the spatiotemporal clustering algorithm
   in `src/common/dedup.py` over a 30-day sliding window of harmonized
   data. The output is `mart.fact_earthquakes`, one row per real-world
   earthquake. Each row carries:
   * `agreement_level` (1–4) — how many agencies confirmed it,
   * `sources` array — which agencies,
   * `source_values` JSONB — per-agency mag/depth/time for drill-down.
5. **Airflow `sync_es`** DAG bulk-indexes updated mart rows into
   Elasticsearch (`quakes` index). The Kibana data view, the
   geographic map and the QuakeFlow dashboard are pre-imported on
   first boot by the `kibana-init` container from
   `es/kibana/exports/dashboards.ndjson`.
6. **Airflow `quality_check`** DAG runs every 15 min and asserts
   source freshness, harmonized integrity, and pushes the
   agreement-level histogram to XCom.

---

## 5. Repository layout

```
AppliedTeam/
├── docker-compose.yml             # single source of truth for the stack
├── .env.example                   # all configuration; copy to .env
├── .gitignore
├── .gitattributes                 # forces LF line endings on shell scripts
├── LICENSE
├── README.md                      # ← you are here
├── docker/
│   ├── airflow/                   # Airflow image + project deps
│   ├── backfill/                  # historical / replay container
│   ├── es-init/                   # one-shot ES mapping installer
│   ├── kibana-init/               # one-shot dashboard importer
│   ├── nifi/                      # NiFi 1.28 + Postgres JDBC + flow installer
│   └── tests/                     # pytest container
├── nifi/
│   ├── flows/quakeflow.json       # documentary description of the flow
│   └── scripts/                   # Python REST-API installer + start.sh
├── dags/
│   ├── harmonize_dag.py
│   ├── dedupe_dag.py
│   ├── sync_es_dag.py
│   ├── quality_check_dag.py
│   └── live_ingest_dag.py         # defence-in-depth Airflow live polling
├── src/
│   ├── common/                    # shared lib used by both DAGs and backfill
│   │   ├── config.py
│   │   ├── db.py
│   │   ├── dedup.py               # the spatiotemporal clustering algorithm
│   │   ├── mapping.py
│   │   ├── minio_client.py
│   │   └── sources.py
│   ├── backfill/ingest.py         # historical / replay entrypoint
│   └── tests/                     # pytest, host- or container-runnable
├── sql/                           # initdb scripts (run once at first boot)
│   ├── 00_schemas.sql
│   ├── 01_raw_tables.sql
│   ├── 02_harmonized.sql
│   ├── 03_mart.sql
│   └── 04_views.sql
├── es/
│   ├── mappings/fact_earthquakes.json
│   └── kibana/exports/dashboards.ndjson
└── data/sample/                   # captured tiny payloads for offline tests
```

---

## 6. Common operations

```bash
# Start the whole stack (default profile = infra + nifi + airflow + live_ingest)
docker compose up --build

# Pull 30 days of historical data (one-shot)
docker compose --profile backfill up --build backfill

# Same code path, but instead of hitting agency APIs, re-parse the MinIO archive
$env:BACKFILL_MODE = "replay"     # PowerShell
docker compose --profile backfill up --build backfill

# Run the entire test suite inside Docker
docker compose --profile tests up --build tests

# Trigger a DAG manually
docker compose exec airflow-scheduler airflow dags trigger harmonize

# Open a psql shell on the operational Postgres
docker compose exec postgres psql -U quake -d quakes

# Sample queries:
#   number of distinct earthquakes per province
docker compose exec postgres psql -U quake -d quakes \
    -c "SELECT * FROM mart.v_by_province LIMIT 10;"
#   show events confirmed by 2+ agencies (drill-down panel data)
docker compose exec postgres psql -U quake -d quakes \
    -c "SELECT * FROM mart.v_disagreement LIMIT 5;"
```

---

## 7. Troubleshooting- what we encounter during the project, and fixes.

| Symptom | Likely cause | Fix |
|---|---|---|
| `airflow-init` container exits with rc=1 on first boot | Postgres-airflow not yet ready | Compose retries on dependent services; wait 30 s and re-run `docker compose up`. |
| Kibana shows "No results found" | Backfill not run yet & NiFi flow needs a moment | Either run the `backfill` profile or wait for NiFi's first poll; the `live_ingest` Airflow DAG also catches up automatically within 5 min. |
| NiFi process group exists but processors are stopped | The auto-start REST call raced NiFi's component validation | Open the NiFi UI ([http://localhost:8081/nifi](http://localhost:8081/nifi)), right-click the **QuakeFlow** process group, click **Start**. The chains are pre-built and pre-wired. |
| `docker compose down` did not free disk | Volumes are still around | `docker compose down -v` removes them. |
| Airflow DAG turns red on first run | DB schemas not yet present | The first call after init should retry automatically; if it doesn't, click **Clear** on the failed task. |
| Kibana dashboards rebuilt in the UI disappear after `down -v` | Saved objects live in the `es_data` volume; `down -v` wipes them | After rebuilding any Kibana saved object (Map, Lens panel, dashboard), export it from **Stack Management → Saved Objects** and overwrite `es/kibana/exports/dashboards.ndjson`, then commit. |

---

## 8. Production concerns we addressed

| Concern              | Mechanism                                                     |
|----------------------|---------------------------------------------------------------|
| Idempotent ingestion | `ON CONFLICT (event_id) DO NOTHING` on raw, UPSERT on harmonized and mart |
| Idempotent dedup     | `event_uid` is a deterministic hash of the cluster centroid   |
| Retries              | NiFi penalize+retry; Airflow `retries=2..3, retry_delay=30s`  |
| Failure isolation    | Each DAG task is a separate process; one task failing doesn't block siblings |
| Replay               | Every raw response archived immutably in MinIO; `BACKFILL_MODE=replay` re-parses without API calls |
| Defence in depth     | NiFi (60 s cadence, visible flow) + Airflow `live_ingest` (5 min, boto3 MinIO writer) target the same `raw.*` tables; either alone is sufficient |
| Data quality         | `quality_check_dag` asserts source freshness, harmonized integrity, agreement-level histogram |
| Observability        | Airflow UI per-task logs, structured Python logging, pgAdmin / MinIO consoles, Kibana dashboard |
| Secrets              | `.env` (gitignored), every credential interpolated, none hardcoded |

---

## 9. Known limitations we acknowledge and future work for

* **KOERI streaming-only.** KOERI's `lst0.asp` endpoint exposes only the
  rolling last 500 events, with no public archive query. KOERI is
  therefore consumed in live mode only; the historical backfill uses
  AFAD + EMSC + USGS.
* **Data scale.** The brief's §3.4 minimum is met through the
  multi-source clause ("a stream that merges multiple sources"). The
  default 30-day backfill produces several thousand records; longer
  windows (`BACKFILL_DAYS=90` etc.) scale roughly linearly and clear
  the ten-thousand-record bar comfortably at the cost of a few extra
  minutes of cold-boot polling.
* **NiFi `PutS3Object` ↔ MinIO.** NiFi 1.x ships AWS SDK v1, which has
  a long-standing "invalid header name" incompatibility with MinIO
  that path-style addressing alone does not fix. The MinIO archive is
  therefore written by the Airflow `live_ingest` DAG and the backfill
  container using `boto3`, which speaks MinIO cleanly. NiFi handles
  the visible polling and parsing.
* **Single-node Elasticsearch.** Security disabled; appropriate for a
  local academic deployment, not production.
* **Threshold tuning.** The dedup window `(Δt ≤ 90 s,
  Δd ≤ 50 km, |ΔM| ≤ 1.0)` is hand-picked from observed
  inter-agency offsets. AFAD and KOERI routinely report shared events
  with epicentres 30–50 km apart, so even our relaxed default leaves
  AFAD-KOERI agreements rare. A principled extension would be a
  network-pair-specific or learned threshold. you can access us and see the technical
  report's Limitations section for detail.

---

## 10. AI usage

This project was developed with AI assistance (Cursor IDE + Anthropic
Claude in the Opus 4.x / Sonnet 4.x families). The complete declaration is again in our
technical report.

---

## 11. Team and contributions

| Member               | Main contributions |
|----------------------|--------------------|
| Yiğit Kağan Akça     | Compose stack, service Dockerfiles, NiFi REST installer, Airflow DAGs, SQL schemas, Elasticsearch / Kibana setup, README |
| İlhan Arda Yavuz     | Per-source HTTP clients, harmonization mappers, deduplication algorithm, unit tests, sample-data capture, technical report, slides |

A more granular contribution table is included in the technical
report's Appendix A.
