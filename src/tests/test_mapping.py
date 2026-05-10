"""thesea are per-source canonical-mapping tests.

These run on captured sample payloads from data/sample/. They guarantee
that as long as the upstream agency does not change its schema then our
parsed canonical record stays correct.
"""

from __future__ import annotations

import json
from datetime import timezone
from pathlib import Path

import pytest

from common.mapping import (
    afad_to_canonical,
    emsc_to_canonical,
    koeri_to_canonical,
    map_row,
    usgs_to_canonical,
)


SAMPLE = Path(__file__).resolve().parents[2] / "data" / "sample"

def test_afad_real_payload_maps_correctly():
    raw = json.loads((SAMPLE / "afad_2025-01-01.json").read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        raw = raw.get("result", raw)
    assert raw, "AFAD sample is empty"
    item = raw[0]
    out = afad_to_canonical(str(item["eventID"]), item)
    assert out is not None
    assert out["source"] == "AFAD"
    assert out["latitude"] == float(item["latitude"])
    assert out["longitude"] == float(item["longitude"])
    assert out["magnitude"] == float(item["magnitude"])
    #afad is local turkiye time
    assert out["event_time"].endswith("+00:00")


def test_afad_missing_coords_returns_none():
    out = afad_to_canonical("X", {"eventID": "X"})
    assert out is None

#kandilli
def test_koeri_payload_picks_priority_magnitude():
    payload = {
        "date": "2026-05-09", "time": "12:00:00",
        "latitude": 40.0, "longitude": 29.0, "depth_km": 7.0,
        "md": None, "ml": 4.1, "mw": 4.3, "place": "TEST",
    }
    out = koeri_to_canonical("KOERI-test", payload)
    assert out is not None
    assert out["magnitude"] == 4.3   # mw > ml
    assert out["mag_type"] == "MW"


def test_koeri_payload_falls_back_to_ml_when_mw_missing():
    payload = {
        "date": "2026-05-09", "time": "12:00:00",
        "latitude": 40.0, "longitude": 29.0, "depth_km": 7.0,
        "md": 3.9, "ml": 4.0, "mw": None, "place": "TEST",
    }
    out = koeri_to_canonical("KOERI-test", payload)
    assert out["magnitude"] == 4.0
    assert out["mag_type"] == "ML"

#emsc
def test_emsc_feature_maps_correctly():
    feature = {
        "id": "emsc-evt-1",
        "properties": {
            "lat": 40.5, "lon": 29.0, "depth": 12.3,
            "mag": 4.2, "magtype": "ML",
            "time": "2026-05-09T12:00:00.000Z",
            "flynn_region": "WESTERN TURKEY",
        },
    }
    out = emsc_to_canonical("emsc-evt-1", feature)
    assert out is not None
    assert out["latitude"] == 40.5
    assert out["place"]    == "WESTERN TURKEY"


#usgs
def test_usgs_geojson_maps_correctly():
    feature = {
        "id": "usgs-evt-1",
        "properties": {
            "mag": 5.1, "magType": "Mw",
            "time": 1715260800000,  # ms epoch
            "place": "TURKEY"
        },
        "geometry": {"type": "Point", "coordinates": [29.0, 40.5, 10.0]}
    }
    out = usgs_to_canonical("usgs-evt-1", feature)
    assert out is not None
    assert out["magnitude"] == 5.1
    assert out["depth_km"]  == 10.0
    assert out["latitude"]  == 40.5
    assert out["longitude"] == 29.0

#registry
def test_map_row_dispatches_correctly():
    feature = {
        "id": "x",
        "properties": {"lat": 40, "lon": 29, "mag": 3.0, "time": "2025-01-01T00:00:00Z"},
    }
    out = map_row("EMSC", "x", feature)
    assert out is not None and out["source"] == "EMSC"


def test_map_row_unknown_source_raises():
    with pytest.raises(ValueError):
        map_row("XXX", "1", {})
