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


# ---------------------------------------------------------------------------
# Controller-service helpers (DBCPConnectionPool for PutSQL)
# ---------------------------------------------------------------------------

DBCP_SERVICE_NAME = "quake-pg-dbcp"


def find_dbcp_at_root(root_id: str, name: str) -> str | None:
    """Return id of a controller service named `name` at the root level, or None."""
    r = requests.get(
        f"{BASE}/flow/process-groups/{root_id}/controller-services", timeout=10
    )
    if not r.ok:
        return None
    for svc in r.json().get("controllerServices", []):
        comp = svc.get("component", {})
        if comp.get("name") == name and comp.get("parentGroupId") == root_id:
            return comp["id"]
    return None


def disable_and_delete_service(service_id: str, timeout_s: int = 30) -> bool:
    """Disable then delete a controller service. Idempotent."""
    try:
        r = requests.get(f"{BASE}/controller-services/{service_id}", timeout=10)
        if not r.ok:
            return True  # already gone
        rev = r.json().get("revision", {})
        state = r.json().get("component", {}).get("state")

        if state in ("ENABLED", "ENABLING"):
            requests.put(
                f"{BASE}/controller-services/{service_id}/run-status",
                json={"revision": rev, "state": "DISABLED",
                      "disconnectedNodeAcknowledged": False},
                timeout=15,
            )
            # Poll for DISABLED.
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                r = requests.get(f"{BASE}/controller-services/{service_id}", timeout=10)
                if r.ok and r.json().get("component", {}).get("state") == "DISABLED":
                    break
                time.sleep(1)

        # Delete with current revision.
        r = requests.get(f"{BASE}/controller-services/{service_id}", timeout=10)
        if r.ok:
            ver = r.json().get("revision", {}).get("version", 0)
            requests.delete(
                f"{BASE}/controller-services/{service_id}?version={ver}",
                timeout=15,
            )
        return True
    except Exception as exc:
        log(f"disable_and_delete_service({service_id}) failed: {exc!r}")
        return False


def create_dbcp_service(parent_id: str, name: str) -> str:
    """Create a DBCPConnectionPool service at `parent_id`. Returns its id.

    Service is created in DISABLED state with all properties (including the
    sensitive Password) set; a subsequent enable_controller_service() call
    transitions it to ENABLED.
    """
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": "org.apache.nifi.dbcp.DBCPConnectionPool",
            # bundle hint helps NiFi resolve the right NAR on first install
            "bundle": {
                "group":    "org.apache.nifi",
                "artifact": "nifi-dbcp-service-nar",
                "version":  "1.28.1",
            },
            "properties": {
                "Database Connection URL":
                    f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}",
                "Database Driver Class Name": "org.postgresql.Driver",
                "database-driver-locations":
                    "/opt/nifi/nifi-current/lib/postgresql.jar",
                "Database User": PG_USER,
                "Password":      PG_PASS,
            },
        },
    }
    r = post(f"/process-groups/{parent_id}/controller-services", body)
    return r["component"]["id"]


def enable_controller_service(service_id: str, timeout_s: int = 60) -> bool:
    """Issue ENABLE then poll until state == 'ENABLED'. Returns True on success."""
    r = requests.get(f"{BASE}/controller-services/{service_id}", timeout=10)
    if not r.ok:
        log(f"cannot fetch service {service_id}: HTTP {r.status_code}")
        return False
    rev = r.json().get("revision", {})

    enable = requests.put(
        f"{BASE}/controller-services/{service_id}/run-status",
        json={"revision": rev, "state": "ENABLED",
              "disconnectedNodeAcknowledged": False},
        timeout=15,
    )
    if not enable.ok:
        log(f"failed to enable {service_id}: HTTP {enable.status_code} "
            f"{enable.text[:300]}")
        return False

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = requests.get(f"{BASE}/controller-services/{service_id}", timeout=10)
        if r.ok:
            comp = r.json().get("component", {})
            state = comp.get("state")
            if state == "ENABLED":
                return True
            if state == "INVALID":
                errs = comp.get("validationErrors", [])
                log(f"service {service_id} INVALID: {errs}")
                return False
        time.sleep(2)
    log(f"service {service_id} did not reach ENABLED within {timeout_s}s")
    return False

#per source chain
def build_source_chain(pg_id: str, source: str, url: str,
                       schedule: str,
                       payload_table: str,
                       row_y: float,
                       dbcp_id: str | None = None,
                       extract_text_for_koeri: bool = False) -> None:
    """Wire up the per-source NiFi chain.

    Layout (JSON sources):
        Generate -> InvokeHTTP -> SplitJson -> EvaluateJsonPath
                                                  |
                                                  v
                                          ReplaceText (build SQL)
                                                  |
                                                  v
                                          PutSQL  (DBCP -> raw.<source>_events)

    Layout (KOERI / HTML):
        Generate -> InvokeHTTP -> LogAttribute
        (HTML parsing is handled in Python by the live_ingest DAG; doing it
        in NiFi would require fragile per-line ExtractText regexes for no
        practical gain.)

    PutS3Object intentionally absent: NiFi 1.x's AWS SDK v1 has a known
    'invalid header name' bug against MinIO. The MinIO archive is written
    by the Airflow `live_ingest` DAG via boto3 (single source of truth
    for the agency-specific parser).
    """

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

    if extract_text_for_koeri:
        # KOERI: HTML response, Python (live_ingest) does the parsing+write.
        sink = add_processor(
            pg_id, "org.apache.nifi.processors.standard.LogAttribute",
            f"{source}-LogIngestEvent", x=800, y=row_y,
            properties={"Log Level": "info",
                        "Log Payload": "false"},
            auto_terminate=["success"],
        )
        connect(pg_id, gen,    invoke, ["success"])
        connect(pg_id, invoke, sink,   ["Response"])
        return

    # JSON sources
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

    if dbcp_id is None:
        # Fallback: no DBCP service available, just log. Stack still runs.
        sink = add_processor(
            pg_id, "org.apache.nifi.processors.standard.LogAttribute",
            f"{source}-LogIngestEvent", x=1050, y=row_y,
            properties={"Log Level": "info",
                        "Log Payload": "false"},
            auto_terminate=["success"],
        )
        connect(pg_id, gen,    invoke, ["success"])
        connect(pg_id, invoke, split,  ["Response"])
        connect(pg_id, split,  eval_,  ["split"])
        connect(pg_id, eval_,  sink,   ["matched"])
        return

    # NiFi 1.28's ReplaceText "Always Replace" does not perform expression
    # language substitution on attributes embedded in the Replacement Value
    # for content. The reliable way to wrap the JSON content with an SQL
    # template is two ReplaceText processors with Prepend and Append
    # strategies - these DO evaluate ${...} attributes in their prefix/
    # suffix text. Postgres dollar-quoting ($nf$..$nf$) avoids the
    # single-quote escaping nightmare from embedding raw JSON.
    sql_prefix = (
        f"INSERT INTO {payload_table} (event_id, payload, received_at) "
        f"VALUES ('${{event_id}}', $nf$"
    )
    sql_suffix = "$nf$::jsonb, NOW()) ON CONFLICT (event_id) DO NOTHING;"

    prepend = add_processor(
        pg_id, "org.apache.nifi.processors.standard.ReplaceText",
        f"{source}-PrefixSQL", x=1050, y=row_y,
        properties={
            "Replacement Strategy": "Prepend",
            "Evaluation Mode":      "Entire text",
            "Replacement Value":    sql_prefix,
        },
        auto_terminate=["failure"],
    )
    append = add_processor(
        pg_id, "org.apache.nifi.processors.standard.ReplaceText",
        f"{source}-SuffixSQL", x=1300, y=row_y,
        properties={
            "Replacement Strategy": "Append",
            "Evaluation Mode":      "Entire text",
            "Replacement Value":    sql_suffix,
        },
        auto_terminate=["failure"],
    )
    put_sql = add_processor(
        pg_id, "org.apache.nifi.processors.standard.PutSQL",
        f"{source}-PutSQL", x=1550, y=row_y,
        properties={
            "JDBC Connection Pool":             dbcp_id,
            "Batch Size":                       "1",
            "Support Fragmented Transactions":  "false",
            "Obtain Generated Keys":            "false",
        },
        auto_terminate=["success", "failure", "retry"],
    )

    connect(pg_id, gen,     invoke,  ["success"])
    connect(pg_id, invoke,  split,   ["Response"])
    connect(pg_id, split,   eval_,   ["split"])
    connect(pg_id, eval_,   prepend, ["matched"])
    connect(pg_id, prepend, append,  ["success"])
    connect(pg_id, append,  put_sql, ["success"])

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

    # Set up the DBCP controller service the JSON chains will write through.
    # We put it at the root so its lifecycle is independent of the QuakeFlow
    # PG (we tear down + rebuild the PG often during development and ripping
    # a controller service down with it would be more complex).
    old_dbcp = find_dbcp_at_root(root, DBCP_SERVICE_NAME)
    if old_dbcp:
        log(f"found stale DBCP service {old_dbcp}; removing.")
        disable_and_delete_service(old_dbcp)

    dbcp_id: str | None = None
    try:
        dbcp_id = create_dbcp_service(root, DBCP_SERVICE_NAME)
        log(f"created DBCP service '{DBCP_SERVICE_NAME}' id={dbcp_id}")
        if enable_controller_service(dbcp_id):
            log("DBCP service ENABLED; PutSQL chains will write to Postgres.")
        else:
            log("WARNING: DBCP service did not reach ENABLED; falling back "
                "to LogAttribute sinks for JSON sources.")
            dbcp_id = None
    except Exception as exc:
        log(f"DBCP setup raised ({exc!r}); falling back to LogAttribute sinks.")
        dbcp_id = None

    # 4 chains, vertically stacked; AFAD/EMSC/USGS write to Postgres via
    # PutSQL when dbcp_id is set, KOERI is parsed by the Airflow live_ingest
    # DAG (HTML in NiFi is too is not handled for the demo but koeri is stated as limitation anyway).
    # AFAD's API rejects `?last=N` ("Parameter Exception: Start-End Time is
    # required"). It requires `start` and `end` timestamps. We pass a sliding
    # 1-hour window via NiFi expression language; ON CONFLICT DO NOTHING in
    # PutSQL absorbs the overlap with previous polls.
    afad_url = (
        "https://deprem.afad.gov.tr/apiv2/event/filter"
        "?start=${now():toNumber():minus(3600000):toDate():format('yyyy-MM-dd HH:mm:ss'):urlEncode()}"
        "&end=${now():format('yyyy-MM-dd HH:mm:ss'):urlEncode()}"
        "&minmag=1.0"
    )
    build_source_chain(pg, "AFAD",
        url=afad_url,
        schedule="60 sec",
        payload_table="raw.afad_events",
        dbcp_id=dbcp_id,
        row_y=100,
    )
    build_source_chain(pg, "EMSC",
        url="https://www.seismicportal.eu/fdsnws/event/1/query?format=json&minlatitude=35.5&maxlatitude=42.5&minlongitude=25.5&maxlongitude=45.0&limit=50",
        schedule="60 sec",
        payload_table="raw.emsc_events",
        dbcp_id=dbcp_id,
        row_y=300,
    )
    build_source_chain(pg, "USGS",
        url="https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minlatitude=35.5&maxlatitude=42.5&minlongitude=25.5&maxlongitude=45.0&minmagnitude=2.5&limit=50",
        schedule="300 sec",
        payload_table="raw.usgs_events",
        dbcp_id=dbcp_id,
        row_y=500,
    )
    build_source_chain(pg, "KOERI",
        url="http://www.koeri.boun.edu.tr/scripts/lst0.asp",
        schedule="60 sec",
        payload_table="raw.koeri_events",
        dbcp_id=None,            # KOERI writes only via Airflow (HTML parsing)
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
