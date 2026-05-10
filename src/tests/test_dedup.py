"""Unit tests for dags.common.dedup.

These tests exercises the spatiotemporal clustering + preferred-source rules and
are the primary protection against regressions. We benefited AI assistnance but verified the tests as stated in report
because we know AI create tests tend to pass because of its reward mechanisms. But a more comprenehsive test
would require geoscientists interpratatins of latest eartquakes or domain knowledge in general, which we do not much have.
so this is a limitation actually.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common.dedup import (
    SOURCE_PRIORITY,
    dedupe,
    dedupe_summary,
    haversine_km,
)


def _ev(source: str, t: datetime, lat: float, lon: float,
        mag: float | None = None, mag_type: str = "ML",
        depth: float | None = 10.0, **extra) -> dict:
    return {
        "source":     source,
        "event_id":   f"{source}-{t.isoformat()}",
        "event_time": t,
        "latitude":   lat,
        "longitude":  lon,
        "magnitude":  mag,
        "mag_type":   mag_type,
        "depth_km":   depth,
        "place":      extra.get("place"),
        "province":   extra.get("province"),
        "district":   extra.get("district"),
    }


#test haversine
def test_haversine_zero():
    assert haversine_km(40.0, 29.0, 40.0, 29.0) == pytest.approx(0.0, abs=1e-6)


def test_haversine_one_degree_lat_is_111km():
    d = haversine_km(40.0, 29.0, 41.0, 29.0)
    assert d == pytest.approx(111.0, abs=1.0)
#test dedupe
def test_three_sources_collapse_into_single_cluster():
    """Same physical event, slightly different reports across AFAD/KOERI/EMSC."""
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        _ev("AFAD",  t0,                          40.62, 29.02, 4.3),
        _ev("KOERI", t0 + timedelta(seconds=4),  40.63, 29.01, 4.1),
        _ev("EMSC",  t0 + timedelta(seconds=10), 40.62, 29.03, 4.4),
    ]
    out = dedupe(rows)
    assert len(out) == 1
    row = out[0]
    assert row["agreement_level"] == 3
    assert set(row["sources"]) == {"AFAD", "KOERI", "EMSC"}
    #AFAD takes precedence over KOERI/EMSC.
    assert row["preferred_source"] == "AFAD"
    assert row["magnitude"] == 4.3
    #Earliest event_time wins for first_seen_at.
    assert row["first_seen_at"] == t0


def test_two_distinct_nearby_events_are_not_collapsed():
    """Two real earthquakes 10 minutes apart at the same epicentre stay split."""
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        _ev("AFAD", t0,                          40.0, 29.0, 3.5),
        _ev("AFAD", t0 + timedelta(minutes=10), 40.0, 29.0, 3.6),
    ]
    out = dedupe(rows)
    assert len(out) == 2
    assert all(r["agreement_level"] == 1 for r in out)


def test_distant_events_not_collapsed_even_if_simultaneous():
    """Different epicentres at the same instant -> different clusters."""
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        _ev("AFAD", t0, 40.0, 29.0, 3.0),
        _ev("AFAD", t0, 38.0, 27.0, 3.0),  # abot 270 km away
    ]
    out = dedupe(rows)
    assert len(out) == 2


def test_magnitude_scale_difference_within_tolerance_collapses():
    """Same event, AFAD reports ML 4.3, USGS reports Mw 5.1 (delta 0.8 OK)."""
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        _ev("AFAD", t0,                          40.62, 29.02, 4.3, mag_type="ML"),
        _ev("USGS", t0 + timedelta(seconds=12), 40.63, 29.03, 5.1, mag_type="Mw"),
    ]
    out = dedupe(rows)
    assert len(out) == 1
    assert out[0]["preferred_source"] == "AFAD"
    assert out[0]["agreement_level"] == 2


def test_magnitude_scale_difference_beyond_tolerance_does_not_collapse():
    """If |dM| > DM_WINDOW (1.0), do not collapse."""
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        _ev("AFAD", t0,                          40.62, 29.02, 2.0),
        _ev("USGS", t0 + timedelta(seconds=12), 40.63, 29.03, 5.5),
    ]
    out = dedupe(rows)
    assert len(out) == 2


def test_preferred_source_picks_in_priority_order():
    """KOERI > EMSC > USGS when AFAD is absent."""
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        _ev("USGS",  t0,                         40.0, 29.0, 4.0),
        _ev("KOERI", t0 + timedelta(seconds=5), 40.01, 29.01, 4.1),
        _ev("EMSC",  t0 + timedelta(seconds=8), 40.0, 29.0, 4.2),
    ]
    out = dedupe(rows)
    assert len(out) == 1
    assert out[0]["preferred_source"] == "KOERI"


def test_event_uid_is_stable_across_runs():
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [_ev("AFAD", t0, 40.62, 29.02, 4.3)]
    a = dedupe(rows)[0]["event_uid"]
    b = dedupe(rows)[0]["event_uid"]
    assert a == b
    assert len(a) == 24


def test_source_values_records_each_source_payload():
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        _ev("AFAD",  t0,                         40.62, 29.02, 4.3, place="Yalova (TR)"),
        _ev("KOERI", t0 + timedelta(seconds=5), 40.63, 29.01, 4.1, place="MARMARA DENIZI"),
    ]
    out = dedupe(rows)
    sv = out[0]["source_values"]
    assert set(sv.keys()) == {"AFAD", "KOERI"}
    assert sv["AFAD"]["magnitude"] == 4.3
    assert sv["KOERI"]["magnitude"] == 4.1
    assert sv["KOERI"]["place"] == "MARMARA DENIZI"


def test_summary_histogram():
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = [
        #Cluster A (3 sources)
        _ev("AFAD",  t0,                         40.62, 29.02, 4.3),
        _ev("KOERI", t0 + timedelta(seconds=4),  40.63, 29.01, 4.1),
        _ev("EMSC",  t0 + timedelta(seconds=10), 40.62, 29.03, 4.4),
        #Cluster B (1 source, far away)
        _ev("USGS",  t0 + timedelta(minutes=30), 38.0, 27.0, 5.5),
    ]
    out = dedupe(rows)
    hist = dedupe_summary(out)
    assert hist == {3: 1, 1: 1}


def test_priority_constant_is_local_first():
    """Sanity: the priority order encodes 'AFAD/KOERI before global feeds'."""
    assert SOURCE_PRIORITY[0] == "AFAD"
    assert SOURCE_PRIORITY[1] == "KOERI"
    assert SOURCE_PRIORITY.index("AFAD") < SOURCE_PRIORITY.index("USGS")
