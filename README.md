# QuakeFlow

> **YZV 322E — Applied Data Engineering**, Spring 2026
> Final project: an end-to-end containerized data engineering pipeline that
> ingests, harmonizes, deduplicates and visualizes earthquake events
> reported by four independent agencies covering Türkiye and the
> surrounding region.

| | |
|---|---|
| Course           | YZV 322E — Applied Data Engineering, Istanbul Technical University |
| Team             | İlhan Arda Yavuz, Yiğit Kağan Akça (team lead: Yiğit Kağan Akça) |
| Submission date  | May 10, 2026 |
| License          | MIT (see `LICENSE`) |

---

## 1. What QuakeFlow does

Each of the four upstream agencies — AFAD, KOERI/Kandilli (national),
EMSC (regional), USGS (global) — publishes earthquake events through
a different interface (two REST/JSON, one FDSN-JSON, one HTML table) and
under different identifiers, magnitude scales and reporting latencies.

QuakeFlow turns those four feeds into a single, deduplicated, queryable
catalogue, served as both a SQL fact table (`mart.fact_earthquakes`) and
an Elasticsearch index (`quakes`), with a Kibana dashboard on top.

The whole stack — every database, every helper script, every agent — runs
inside Docker. Bringing it up is a single command:

```bash
docker compose up --build
```

Within 15 minutes of `git clone`, an evaluator's machine has:

* live ingestion of all four sources via Apache **NiFi** (60–300 s cadence,
  source-dependent),
* a defence-in-depth Airflow `live_ingest` DAG running every 5 minutes,
* every raw response archived immutably in **MinIO** (replayable; see
  [`docs/REPLAY.md`](docs/REPLAY.md)),
* `harmonize → dedupe → sync_es` Airflow DAG chain materializing the
  deduplicated fact table and pushing updated rows to Elasticsearch,
* dashboards visible at <http://localhost:5601> (Kibana).

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

Identical figure as a TikZ diagram is in `docs/report/report.tex`.

| Layer       | Tool                       | Rubric coverage                  |
|-------------|----------------------------|----------------------------------|
| Ingestion   | Apache NiFi 1.28           | ✓ course tool (visual data flow) |
| Storage     | PostgreSQL 16              | ✓ course tool                    |
| Storage UI  | pgAdmin 4                  | ✓ course tool                    |
| Archive     | MinIO (S3-compatible)      | supplementary tool               |
| Orchestrat. | Apache Airflow 2.10        | ✓ course tool                    |
| Search      | Elasticsearch 8.15         | ✓ course tool                    |
| Visualiz.   | Kibana 8.15                | ✓ course tool                    |

Six of the six course tools are used; the brief required at least two.

---

## 3. Quick start

### 3.1 Prerequisites

* Docker Desktop ≥ 24 (or Docker Engine + Compose v2.20+)
* ≥ 8 GB RAM allocated to Docker (Elasticsearch needs ~1 GB, NiFi ~1 GB,
  Airflow ~1.5 GB, Postgres ~512 MB)
* ≥ 10 GB free disk for images + volumes

### 3.2 Clone & launch

```bash
git clone <REPO_URL> AppliedTeam
cd AppliedTeam
cp .env.example .env          # edit only if you don't like the defaults
docker compose up --build
```

That's it. The Airflow scheduler / NiFi flow / Kibana dashboards self-bootstrap.

### 3.3 Where the services live

| Service        | URL                          | Login                                   |
|----------------|------------------------------|-----------------------------------------|
| Airflow        | http://localhost:8088        | `admin` / `admin`                       |
| Kibana         | http://localhost:5601        | (anonymous, security disabled locally)  |
| NiFi           | http://localhost:8081/nifi   | `admin` / `adminadminadmin`             |
| pgAdmin        | http://localhost:5050        | `admin@itu.edu.tr` / `admin`            |
| MinIO console  | http://localhost:9001        | `quake` / `quake_minio_pw`              |
| Postgres (ops) | `postgresql://localhost:5432/quakes` | `quake` / `quake_pw`            |
| Elasticsearch  | http://localhost:9200        | (anonymous)                             |

### 3.4 Load 30 days of historical data

```bash
docker compose --profile backfill up --build backfill
```

This pulls AFAD/EMSC/USGS for the last 30 days, archives each raw response
to MinIO under `s3://quake-raw/<source>/<date>/<time>-<uuid>.<ext>`, and
loads parsed events into `raw.<source>_events`. Idempotent — running it
twice does not duplicate rows. Follow with the harmonize / dedupe DAGs
(triggered automatically every 2 minutes; you can also trigger them in the
Airflow UI for an immediate run).

After backfill completes, the project's 10 000-record floor is comfortably
exceeded.

### 3.5 Run the tests

```bash
docker compose --profile tests up --build tests
```

Runs the entire pytest suite (24 tests) inside its own container.

### 3.6 Tear it down

```bash
docker compose down            # stops services, keeps volumes
docker compose down -v         # also removes volumes (clean reset)
```

---

## 4. End-to-end data flow (what to look for in the demo)

1. **NiFi** polls all four sources every 60–300 s, archives the raw
   payload in MinIO, and writes a row into `raw.<source>_events`.
2. **Airflow `harmonize`** DAG reads new raw rows (per-source watermark)
   and projects them into `harmonized.events` using
   `src/common/mapping.py`.
3. **Airflow `dedupe`** DAG runs the spatiotemporal clustering algorithm
   in `src/common/dedup.py` on a 30-day sliding window of harmonized
   data. The output is `mart.fact_earthquakes`, one row per real-world
   earthquake. Each row carries:
   * `agreement_level` (1–4) — how many agencies confirmed it,
   * `sources` array — which agencies,
   * `source_values` JSONB — per-agency mag/depth/time for drill-down.
4. **Airflow `sync_es`** DAG bulk-indexes updated mart rows into
   Elasticsearch (`quakes` index). The Kibana data view is pre-imported
   by the `kibana-init` container.
5. **Airflow `quality_check`** DAG runs every 15 min and asserts source
   freshness, harmonized integrity, and emits the agreement-level
   histogram for the dashboards.

See [`docs/DEDUPE.md`](docs/DEDUPE.md) for the algorithm in detail
(addresses the evaluator's request for explicit dedup documentation) and
[`docs/REPLAY.md`](docs/REPLAY.md) for how the immutable archive is
exercised (addresses the evaluator's request to highlight replay).

---

## 5. Repository layout

```
AppliedTeam/
├── docker-compose.yml          # single source of truth for the stack
├── .env.example                # all configuration, copied to .env at clone time
├── .gitignore
├── LICENSE
├── README.md                   # ← you are here
├── docker/
│   ├── airflow/                # Airflow image + project deps
│   ├── backfill/               # historical / replay container
│   ├── es-init/                # one-shot ES mapping installer
│   ├── kibana-init/            # one-shot dashboard importer
│   ├── nifi/                   # NiFi 1.28 + Postgres JDBC + flow installer
│   └── tests/                  # pytest container
├── nifi/
│   ├── flows/quakeflow.json    # documentary description of the flow
│   └── scripts/                # Python REST-API installer + start.sh
├── dags/
│   ├── harmonize_dag.py
│   ├── dedupe_dag.py
│   ├── sync_es_dag.py
│   ├── quality_check_dag.py
│   └── live_ingest_dag.py      # defence-in-depth Airflow live polling
├── src/
│   ├── common/                 # shared lib used by both DAGs and backfill
│   │   ├── config.py
│   │   ├── db.py
│   │   ├── dedup.py            # ← the algorithm flagged by the evaluator
│   │   ├── mapping.py
│   │   ├── minio_client.py
│   │   └── sources.py
│   ├── backfill/ingest.py      # historical / replay entrypoint
│   └── tests/                  # pytest, host- or container-runnable
├── sql/                        # initdb scripts (run once at first boot)
│   ├── 00_schemas.sql
│   ├── 01_raw_tables.sql
│   ├── 02_harmonized.sql
│   ├── 03_mart.sql
│   └── 04_views.sql
├── es/
│   ├── mappings/fact_earthquakes.json
│   └── kibana/exports/dashboards.ndjson
├── data/sample/                # captured tiny payloads for offline tests
└── docs/
    ├── DEDUPE.md               # detailed dedup documentation
    ├── REPLAY.md               # archive replay documentation
    ├── AI_USAGE.md             # mirror of the report's AI declaration
    ├── architecture.md
    ├── feasibility/            # original API-feasibility scripts (history)
    ├── report/                 # IEEE technical report (.tex + .pdf)
    └── slides/                 # presentation deck (.pptx + .pdf)
```

---

## 6. Common operations

```bash
# Start the whole stack (default profile = infra + nifi + airflow + live_ingest)
docker compose up --build

# Pull 30 days of historical data (one-shot)
docker compose --profile backfill up --build backfill

# Same code path, but instead of hitting agency APIs, re-parse the MinIO archive
BACKFILL_MODE=replay docker compose --profile backfill up --build backfill

# Run the entire test suite inside Docker
docker compose --profile tests up --build tests

# Trigger a DAG manually
docker compose exec airflow-webserver airflow dags trigger harmonize

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

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `airflow-init` container exits with rc=1 on first boot | Postgres-airflow not yet ready | Compose retries on dependent services; wait 30 s and re-run `docker compose up`. |
| Kibana shows "No results found" | Backfill not run yet & NiFi flow not started | Either run the backfill profile or start the NiFi process group from the UI; the `live_ingest` Airflow DAG also catches up automatically within 5 min. |
| NiFi process group exists but processors are stopped | The auto-start REST call lost the race with NiFi's controller validation | Open NiFi UI ([http://localhost:8081/nifi](http://localhost:8081/nifi)), right-click the QuakeFlow process group, "Start". The chains are pre-built and pre-wired. |
| `docker compose down` did not free disk | Volumes are still around | `docker compose down -v` removes them. |
| Airflow DAG turns red on first run | DB schemas not yet present | The first call after init should retry automatically; if it doesn't, click "Clear" on the failed task. |

---

## 8. Production concerns we addressed

| Concern              | Mechanism                                                     |
|----------------------|---------------------------------------------------------------|
| Idempotent ingestion | `ON CONFLICT (event_id) DO NOTHING` on raw, UPSERT on harmonized and mart |
| Idempotent dedup     | `event_uid` is a deterministic hash of the cluster centroid   |
| Retries              | NiFi penalize+retry; Airflow `retries=2..3, retry_delay=30s`  |
| Failure isolation    | Each DAG task is a separate process; one task failing doesn't block siblings |
| Replay               | Every raw response archived immutably in MinIO; `BACKFILL_MODE=replay` re-parses without API calls |
| Defence in depth     | NiFi (primary, 60 s cadence) + Airflow `live_ingest` (5 min) - both write to the same `raw.*` tables |
| Data quality         | `quality_check_dag` asserts source freshness, harmonized integrity, and emits agreement-level histogram |
| Observability        | Airflow UI per-task logs; structured Python logging; pgAdmin / MinIO consoles for storage layers |
| Secrets              | `.env` (gitignored), every credential interpolated, none hardcoded |

---

## 9. Known limitations

* **KOERI does not expose an archive API** – its `lst0.asp` page returns only the
  rolling last 500 events. We therefore use KOERI in live (streaming) mode
  only. The historical 30-day backfill uses AFAD + EMSC + USGS, which is more
  than enough for the 10 000-record minimum (≈30 000+ in practice).
* **Single-node Elasticsearch** – security disabled; absolutely fine for an
  academic demo on `localhost`, but obviously not production-grade.
* **NiFi flow installer** programmatically builds the canvas but cannot set
  controller-service passwords via the REST API; the operator clicks "enable"
  on `quake-pg` once. < 2 minutes, documented above.
* **Live Kandilli web demo** – the rolling feed is in Türkiye local time;
  our mapper converts to UTC; daylight-saving edge cases are handled in
  the unit tests.

See `docs/report/report.tex` §7 for a fuller treatment + future-work items.

---

## 10. Team and contributions

| Member               | Role                | Main contributions |
|----------------------|---------------------|--------------------|
| Yiğit Kağan Akça     | Team lead (admin only); Engineering | Compose stack, NiFi REST installer, Airflow DAGs, Kibana dashboards, technical report |
| İlhan Arda Yavuz     | Engineering         | Source clients, harmonization mappers, deduplication algorithm, unit tests, slides   |

A more granular contribution table is in `docs/report/report.tex` (Appendix).

AI usage is declared in [`docs/AI_USAGE.md`](docs/AI_USAGE.md) and mirrored
in §8 of the technical report.
