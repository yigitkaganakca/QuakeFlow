"""QuakeFlow backfill / replay container.

Two modes are supported, switched via the MODE env var:

  MODE=historical
      Pull AFAD / EMSC / USGS for the last DAYS days (though remember that KOERI is live only officialy),
      archive each raw response to MinIO, and write parsed rows to
      raw.<source>_events. This is what gets us above 10k records on a
      fresh clone.

  MODE=replay
      Walk the existing MinIO archive (no API calls), re-parse with the
      currently shipped parser, and re-write rows to raw.<source>_events.
      This proves the immutable archive does the job we built it for.

Both modes are idempotent end-to-end.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from common import db, minio_client, sources

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s :: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill")


#helperes


def _selected_sources() -> set[str]:
    raw = os.environ.get("SOURCES", "AFAD,EMSC,USGS")
    return {s.strip().upper() for s in raw.split(",") if s.strip()}


def _archive(source: str, blob: bytes, ext: str, when: datetime) -> str:
    key = minio_client.archive_key(source, ext, when=when)
    ctype = "application/json" if ext == "json" else "text/html"
    return minio_client.put_blob(key, blob, content_type=ctype)


#for historical mode


def run_historical(days: int, min_mag: float, selected: set[str]) -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    log.info("historical window: %s -> %s (%dd, minmag=%.1f, sources=%s)",
             start.isoformat(), end.isoformat(), days, min_mag, sorted(selected))

    total = 0

    if "AFAD" in selected:
        for chunk_start, chunk_end in sources.daterange_days(start, end, chunk_days=1):
            try:
                res = sources.fetch_afad(chunk_start, chunk_end, min_mag=min_mag)
            except Exception as exc:
                log.warning("AFAD chunk %s..%s failed: %r",
                            chunk_start, chunk_end, exc)
                continue
            uri = _archive("AFAD", res.raw_bytes, res.raw_ext, when=chunk_start)
            n = db.upsert_raw("AFAD", res.records, archive_uri=uri)
            log.info("AFAD %s..%s: %d records (archived %s)",
                     chunk_start.date(), chunk_end.date(), n, uri)
            total += n
            sources.polite_sleep(0.4)

    if "EMSC" in selected:
        # we saw that EMSC supports up to a few-thousand record response but chunk weekly to stay safe.
        for chunk_start, chunk_end in sources.daterange_days(start, end, chunk_days=7):
            try:
                res = sources.fetch_emsc(chunk_start, chunk_end, min_mag=max(min_mag, 1.5))
            except Exception as exc:  # noqa: BLE001
                log.warning("EMSC chunk failed: %r", exc)
                continue
            uri = _archive("EMSC", res.raw_bytes, res.raw_ext, when=chunk_start)
            n = db.upsert_raw("EMSC", res.records, archive_uri=uri)
            log.info("EMSC %s..%s: %d records", chunk_start.date(), chunk_end.date(), n)
            total += n
            sources.polite_sleep(0.4)

    if "USGS" in selected:
        # USGS for our small bbox is sparse so one shot is fine.
        try:
            res = sources.fetch_usgs(start, end, min_mag=max(min_mag, 2.5))
            uri = _archive("USGS", res.raw_bytes, res.raw_ext, when=start)
            n = db.upsert_raw("USGS", res.records, archive_uri=uri)
            log.info("USGS %s..%s: %d records", start.date(), end.date(), n)
            total += n
        except Exception as exc:
            log.warning("USGS failed: %r", exc)

    if "KOERI" in selected:
        # again KOERI exposes only the rolling last 500 events but we capture them
        # so the live demo has data even before NiFi has had time to poll.
        try:
            res = sources.fetch_koeri()
            uri = _archive("KOERI", res.raw_bytes, res.raw_ext,
                           when=datetime.now(timezone.utc))
            n = db.upsert_raw("KOERI", res.records, archive_uri=uri)
            log.info("KOERI rolling-snapshot: %d records", n)
            total += n
        except Exception as exc: 
            log.warning("KOERI failed: %r", exc)

    return total


#replaz mode


_PARSERS = {
    "AFAD":  sources.parse_afad_bytes,
    "EMSC":  sources.parse_emsc_bytes,
    "USGS":  sources.parse_usgs_bytes,
    "KOERI": sources.parse_koeri_bytes,
}


def run_replay(selected: set[str]) -> int:
    """Re-parse every blob currently sitting in the MinIO archive.

    No API calls. This proves the archive is good for parser changes,
    schema evolution and audits. See docs/REPLAY.md for scenarios.
    """
    minio_client.ensure_bucket()
    total = 0
    for source in sorted(selected):
        prefix = f"{source}/"
        log.info("replay: walking s3a://%s/%s", os.environ.get("QUAKE_MINIO_BUCKET"), prefix)
        keys = list(minio_client.list_keys(prefix=prefix))
        if not keys:
            log.info("replay: %s -> no archived blobs (skipping)", source)
            continue
        parse = _PARSERS[source]
        n_blobs = 0
        n_records = 0
        for key in keys:
            try:
                blob = minio_client.get_blob(key)
                records = parse(blob)
            except Exception as exc:  # noqa: BLE001
                log.warning("replay: failed to parse %s: %r", key, exc)
                continue
            uri = minio_client.archive_uri(key)
            n = db.upsert_raw(source, records, archive_uri=uri)
            n_blobs += 1
            n_records += n
        log.info("replay: %s -> %d blobs / %d records re-asserted", source, n_blobs, n_records)
        total += n_records
    return total

#main
def main() -> int:
    mode = os.environ.get("MODE", "historical").strip().lower()
    selected = _selected_sources()

    log.info("waiting for postgres ...")
    db.wait_until_ready()
    log.info("postgres up.")

    minio_client.ensure_bucket()

    if mode == "historical":
        days = int(os.environ.get("DAYS", "30"))
        min_mag = float(os.environ.get("MIN_MAG", "1.0"))
        n = run_historical(days, min_mag, selected)
    elif mode == "replay":
        n = run_replay(selected)
    else:
        log.error("unknown MODE=%r (expected 'historical' or 'replay')", mode)
        return 2

    log.info("backfill done. total upserts attempted = %d", n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
