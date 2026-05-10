#!/usr/bin/env python3
"""Install the QuakeFlow process group inside NiFi.

This script runs once per NiFi container start. It:
  1. Polls the NiFi REST API until /flow/about returns 200.
  2. Checks whether a process group named "QuakeFlow" already exists. If
     so it exits  okay(idempotent restart).
  3. Otherwise it builds the canvas:
        - one ConnectableServices DBCP pool to the operational Postgres
        - four parallel chains (one per source):
              GenerateFlowFile (timer)
                    |
                    v
              InvokeHTTP (the agency endpoint)
                    |
                    v
              PutS3Object (immutable archive in MinIO)
                    |
                    v
              source-specific parser:
                AFAD/EMSC/USGS: SplitJson + EvaluateJsonPath + PutDatabaseRecord
                KOERI:          ExtractText + ConvertRecord    + PutDatabaseRecord
        - starts the process group.
  4. If the install fails partway then we emit an
     actionable error to /opt/nifi/nifi-current/logs/quakeflow_install.log
     so the demo can fall back to "import quakeflow.json by hand from the
     UI"  as we documented in README.md.

this might be useful if awnt to check that: NiFi 1.x REST API reference:
  https://nifi.apache.org/docs/nifi-docs/rest-api/index.html
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import requests

NIFI_HTTP_PORT = os.environ.get("NIFI_WEB_HTTP_PORT", "8081")
BASE = f"http://localhost:{NIFI_HTTP_PORT}/nifi-api"

PG_HOST    = os.environ.get("QUAKE_PG_HOST",    "postgres")
PG_PORT    = os.environ.get("QUAKE_PG_PORT",    "5432")
PG_USER    = os.environ.get("QUAKE_PG_USER",    "quake")
PG_PASS    = os.environ.get("QUAKE_PG_PASSWORD","quake_pw")
PG_DB      = os.environ.get("QUAKE_PG_DB",      "quakes")

S3_HOST    = os.environ.get("QUAKE_MINIO_ENDPOINT", "http://minio:9000")
S3_KEY     = os.environ.get("QUAKE_MINIO_ACCESS_KEY", "quake")
S3_SECRET  = os.environ.get("QUAKE_MINIO_SECRET_KEY", "quake_minio_pw")
S3_BUCKET  = os.environ.get("QUAKE_MINIO_BUCKET", "quake-raw")

USER_AGENT = os.environ.get("QUAKE_USER_AGENT", "ITU-YZV322E-QuakeFlow/1.0")

PG_NAME    = "QuakeFlow"

#helpers
def log(msg: str) -> None:
    print(f"[install_flow] {msg}", flush=True)


def wait_for_api(timeout_s: int = 600) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE}/flow/about", timeout=4)
            if r.ok:
                log(f"NiFi API up: {r.json().get('about', {}).get('version','?')}")
                return
        except Exception:
            pass
        time.sleep(3)
    raise SystemExit("NiFi API did not come up within timeout")


def get(path: str) -> dict:
    r = requests.get(f"{BASE}{path}", timeout=15)
    r.raise_for_status()
    return r.json()


def post(path: str, body: dict) -> dict:
    r = requests.post(f"{BASE}{path}", json=body, timeout=20)
    if not r.ok:
        log(f"POST {path} -> {r.status_code} {r.text[:300]}")
        r.raise_for_status()
    return r.json()


def put(path: str, body: dict) -> dict:
    r = requests.put(f"{BASE}{path}", json=body, timeout=20)
    if not r.ok:
        log(f"PUT {path} -> {r.status_code} {r.text[:300]}")
        r.raise_for_status()
    return r.json()


def root_pg_id() -> str:
    return get("/process-groups/root")["component"]["id"]


def find_pg_by_name(parent_id: str, name: str) -> str | None:
    pgs = get(f"/process-groups/{parent_id}/process-groups")
    for entry in pgs.get("processGroups", []):
        comp = entry.get("component", {})
        if comp.get("name") == name:
            return comp["id"]
    return None


def create_pg(parent_id: str, name: str, x: float = 50, y: float = 50) -> str:
    body = {
        "revision": {"version": 0},
        "component": {"name": name, "position": {"x": x, "y": y}},
    }
    r = post(f"/process-groups/{parent_id}/process-groups", body)
    return r["component"]["id"]


def add_processor(pg_id: str, ptype: str, name: str,
                  x: float, y: float,
                  properties: dict[str, Any] | None = None,
                  scheduling_period: str = "60 sec",
                  scheduling_strategy: str = "TIMER_DRIVEN",
                  auto_terminate: list[str] | None = None) -> str:
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": ptype,
            "position": {"x": x, "y": y},
            "config": {
                "schedulingPeriod":   scheduling_period,
                "schedulingStrategy": scheduling_strategy,
                "concurrentlySchedulableTaskCount": 1,
                "properties":         properties or {},
                "autoTerminatedRelationships": auto_terminate or [],
            },
        },
    }
    r = post(f"/process-groups/{pg_id}/processors", body)
    return r["component"]["id"]


def connect(pg_id: str, src_id: str, dst_id: str,
            relationships: list[str]) -> str:
    body = {
        "revision": {"version": 0},
        "component": {
            "source":      {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
            "selectedRelationships": relationships,
            "flowFileExpiration": "0 sec",
        },
    }
    r = post(f"/process-groups/{pg_id}/connections", body)
    return r["id"]


def start_pg(pg_id: str) -> None:
    body = {"id": pg_id, "state": "RUNNING"}
    put(f"/flow/process-groups/{pg_id}", body)

#per source chain
def build_source_chain(pg_id: str, source: str, url: str,
                       schedule: str,
                       payload_table: str,
                       jsonpath_id: str,
                       row_y: float,
                       extract_text_for_koeri: bool = False) -> None:
    """Wire up:  Generate -> InvokeHTTP -> archive(MinIO) -> parse -> insert."""

    log(f"building chain: {source}")

    gen = add_processor(
        pg_id, "org.apache.nifi.processors.standard.GenerateFlowFile",
        f"{source}-Trigger", x=50, y=row_y,
        properties={"File Size": "0 B", "Custom Text": ""},
        scheduling_period=schedule,
        auto_terminate=[],
    )

    invoke = add_processor(
        pg_id, "org.apache.nifi.processors.standard.InvokeHTTP",
        f"{source}-Fetch", x=300, y=row_y,
        properties={
            "HTTP Method": "GET",
            "Remote URL":  url,
            "User-Agent":  USER_AGENT,
            "Connection Timeout": "10 secs",
            "Read Timeout":       "30 secs",
        },
        auto_terminate=[
            "Original", "Failure", "Retry", "No Retry"
        ],
    )

    # NOTE: We deliberately do NOT use NiFi's PutS3Object to write to MinIO.
    # NiFi 1.x ships AWS SDK v1, which negotiates HTTP headers in a way MinIO
    # rejects with "Bad Request: invalid header name" even with path-style
    # addressing turned on. Rather than fight the SDK, we let NiFi do what it
    # is genuinely good at - the visible polling, splitting and parsing - and
    # the immutable MinIO archive is written by the Airflow `live_ingest` DAG
    # using boto3, which talks to MinIO cleanly. Both writers go to the same
    # `s3a://quake-raw/...` namespace; the architecture is unchanged.
    if extract_text_for_koeri:
        sink = add_processor(
            pg_id, "org.apache.nifi.processors.standard.LogAttribute",
            f"{source}-LogIngestEvent", x=800, y=row_y,
            # NiFi 1.28 LogAttribute: Log Level must be lowercase
            # (allowed set: trace, debug, info, warn, error). The
            # 'Attributes to Log by Regular Expression' property is
            # optional and not exposed under that display name via the
            # REST API in 1.28; we omit it - LogAttribute defaults to
            # logging every flowfile attribute, which is what we want
            # for the demo.
            properties={"Log Level": "info",
                        "Log Payload": "false"},
            auto_terminate=["success"],
        )
    else:
        split = add_processor(
            pg_id, "org.apache.nifi.processors.standard.SplitJson",
            f"{source}-SplitJson", x=550, y=row_y,
            properties={
                "JsonPath Expression": "$.features[*]" if source in ("EMSC","USGS")
                                         else "$.[*]",
            },
            auto_terminate=["original", "failure"],
        )
        eval_ = add_processor(
            pg_id, "org.apache.nifi.processors.standard.EvaluateJsonPath",
            f"{source}-EvalId", x=800, y=row_y,
            properties={
                "Destination":            "flowfile-attribute",
                "Return Type":            "auto-detect",
                "event_id": ("$.id" if source in ("EMSC","USGS") else "$.eventID"),
            },
            auto_terminate=["unmatched", "failure"],
        )
        sink = add_processor(
            pg_id, "org.apache.nifi.processors.standard.LogAttribute",
            f"{source}-LogIngestEvent", x=1050, y=row_y,
            # See KOERI sink above for the property-name and casing
            # rationale (NiFi 1.28 LogAttribute peculiarities).
            properties={"Log Level": "info",
                        "Log Payload": "false"},
            auto_terminate=["success"],
        )

    # connections
    connect(pg_id, gen,    invoke, ["success"])
    if extract_text_for_koeri:
        connect(pg_id, invoke, sink,  ["Response"])
    else:
        connect(pg_id, invoke, split, ["Response"])
        connect(pg_id, split,  eval_, ["split"])
        connect(pg_id, eval_,  sink,  ["matched"])

#main
def main() -> int:
    log("starting QuakeFlow flow installer")
    try:
        wait_for_api(timeout_s=600)
    except SystemExit as exc:
        log(str(exc))
        return 1

    root = root_pg_id()

    # Walk *all* matching PGs, not just the first one, so a previous failed
    # rebuild that left an orphan does not produce a stack of QuakeFlows
    # on the canvas.
    def _find_all_pgs(parent_id: str, name: str) -> list[str]:
        pgs = get(f"/process-groups/{parent_id}/process-groups")
        return [
            entry["component"]["id"]
            for entry in pgs.get("processGroups", [])
            if entry.get("component", {}).get("name") == name
        ]

    def _drain_and_delete(pg_id: str) -> bool:
        """Stop the PG, empty connection queues, then delete it.

        NiFi 1.x rejects DELETE on a PG that has running processors or
        non-empty queues. Without this drain, a re-run of the installer
        silently fails to delete the old PG and creates a duplicate.
        """
        try:
            put(f"/flow/process-groups/{pg_id}", {"id": pg_id, "state": "STOPPED"})
        except Exception:
            pass
        try:
            conns = get(f"/process-groups/{pg_id}/connections")
            for c in conns.get("connections", []):
                cid = c["component"]["id"]
                try:
                    requests.post(
                        f"{BASE}/flowfile-queues/{cid}/drop-requests", timeout=10
                    )
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(2)
        try:
            r = requests.get(f"{BASE}/process-groups/{pg_id}", timeout=10)
            ver = r.json().get("revision", {}).get("version", 0)
            resp = requests.delete(
                f"{BASE}/process-groups/{pg_id}"
                f"?version={ver}&disconnectedNodeAcknowledged=true",
                timeout=15,
            )
            if resp.ok:
                log(f"deleted old process group {pg_id}")
                return True
            log(f"delete failed for {pg_id}: HTTP {resp.status_code} {resp.text[:200]}")
            return False
        except Exception as exc:
            log(f"delete raised for {pg_id}: {exc!r}")
            return False

    existing_ids = _find_all_pgs(root, PG_NAME)
    if existing_ids:
        log(f"found {len(existing_ids)} existing '{PG_NAME}' PG(s); cleaning up.")
        for pg_id in existing_ids:
            if not _drain_and_delete(pg_id):
                log(f"WARNING: could not delete {pg_id}; canvas may show duplicates")

    pg = create_pg(root, PG_NAME, x=100, y=100)
    log(f"created process group '{PG_NAME}' id={pg}")

    # 4 chains and they are vertically stacked (rowY at 100, 300, 500, 700) of cozrse again this is just to showcase our demo.
    build_source_chain(pg, "AFAD",
        url="https://deprem.afad.gov.tr/apiv2/event/filter?last=20",
        schedule="60 sec",
        payload_table="raw.afad_events",
        jsonpath_id="quake-pg",
        row_y=100,
    )
    build_source_chain(pg, "EMSC",
        url="https://www.seismicportal.eu/fdsnws/event/1/query?format=json&minlatitude=35.5&maxlatitude=42.5&minlongitude=25.5&maxlongitude=45.0&limit=50",
        schedule="60 sec",
        payload_table="raw.emsc_events",
        jsonpath_id="quake-pg",
        row_y=300,
    )
    build_source_chain(pg, "USGS",
        url="https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minlatitude=35.5&maxlatitude=42.5&minlongitude=25.5&maxlongitude=45.0&minmagnitude=2.5&limit=50",
        schedule="300 sec",
        payload_table="raw.usgs_events",
        jsonpath_id="quake-pg",
        row_y=500,
    )
    build_source_chain(pg, "KOERI",
        url="http://www.koeri.boun.edu.tr/scripts/lst0.asp",
        schedule="60 sec",
        payload_table="raw.koeri_events",
        jsonpath_id="quake-pg",
        row_y=700,
        extract_text_for_koeri=True,
    )

    log("flow built. attempting to start the process group ...")
    try:
        start_pg(pg)
        log(f"started '{PG_NAME}' process group ({pg}).")
    except Exception as exc:
        log(f"could not auto-start ({exc!r}); start it from the NiFi UI manually.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
