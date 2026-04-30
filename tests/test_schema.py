"""
test_schema.py — validates that sim_publisher emits payloads that
conform to the canonical tag_event JSON Schema (sim/schema/tag_event.json).

These tests are the Phase 3 exit-criteria gate: if they pass, the simulator
output is safe to use as a proxy for real Minew G1 gateway traffic in Phase 4.

Run from the sim/ directory:
    pip install -r requirements.txt pytest
    pytest tests/
"""

from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path

import jsonschema
import pytest

# Allow importing sim_publisher regardless of working directory
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from sim_publisher import (  # noqa: E402
    SimConfig,
    SiteConfig,
    Zone,
    TagState,
    _advance,
    _build_payload,
    _make_tags,
    load_site,
    rssi_dbm,
)

SCHEMA_PATH           = ROOT / "schema" / "tag_event.json"
SITE_CONFIG_PATH      = ROOT / "config" / "sample_site.json"
SYNTHETIC_EVENTS_PATH = ROOT / "data"   / "synthetic_events.jsonl"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def schema() -> dict:
    assert SCHEMA_PATH.exists(), f"Schema file missing: {SCHEMA_PATH}"
    with SCHEMA_PATH.open() as f:
        return json.load(f)


@pytest.fixture(scope="session")
def site() -> SiteConfig:
    assert SITE_CONFIG_PATH.exists(), f"Site config missing: {SITE_CONFIG_PATH}"
    return load_site(SITE_CONFIG_PATH)


@pytest.fixture
def cfg(site) -> SimConfig:
    return SimConfig(
        broker="localhost",
        port=1883,
        tls=False,
        username=None,
        password=None,
        tenant="test-tenant",
        site=site,
        num_tags=100,
        rate=1.0,
        duration=60.0,
        load_mode=False,
        seed=42,
    )


@pytest.fixture
def cfg_load(site) -> SimConfig:
    return SimConfig(
        broker="localhost",
        port=1883,
        tls=False,
        username=None,
        password=None,
        tenant="load-tenant",
        site=site,
        num_tags=100,
        rate=10.0,
        duration=60.0,
        load_mode=True,
        seed=0,
    )


# ---------------------------------------------------------------------------
# Fixture / file sanity
# ---------------------------------------------------------------------------

def test_schema_file_exists():
    assert SCHEMA_PATH.exists()


def test_site_config_file_exists():
    assert SITE_CONFIG_PATH.exists()


def test_site_config_has_zones(site):
    assert len(site.zones) >= 1, "Site must define at least one zone"


# ---------------------------------------------------------------------------
# Single payload conformance
# ---------------------------------------------------------------------------

def test_single_payload_validates_against_schema(cfg, schema):
    """One tag event must satisfy the full JSON Schema."""
    rng = random.Random(42)
    tags = _make_tags(cfg, rng)
    _advance(tags[0], cfg, rng)
    payload = _build_payload(tags[0], cfg, rng)
    jsonschema.validate(payload, schema)   # raises ValidationError on failure


def test_all_required_fields_present(cfg, schema):
    rng = random.Random(0)
    tags = _make_tags(cfg, rng)
    for tag in tags[:10]:
        _advance(tag, cfg, rng)
        payload = _build_payload(tag, cfg, rng)
        for field in schema["required"]:
            assert field in payload, f"Required field '{field}' missing from payload"


# ---------------------------------------------------------------------------
# Type + range assertions (schema contract enforcement)
# ---------------------------------------------------------------------------

def test_rssi_is_integer(cfg):
    rng = random.Random(1)
    tags = _make_tags(cfg, rng)
    for tag in tags:
        _advance(tag, cfg, rng)
        payload = _build_payload(tag, cfg, rng)
        assert isinstance(payload["rssi"], int), \
            f"rssi must be int, got {type(payload['rssi'])}: {payload['rssi']}"


def test_rssi_in_valid_range(cfg):
    rng = random.Random(2)
    tags = _make_tags(cfg, rng)
    for tag in tags:
        _advance(tag, cfg, rng)
        payload = _build_payload(tag, cfg, rng)
        assert -120 <= payload["rssi"] <= 0, \
            f"rssi out of [-120, 0]: {payload['rssi']}"


def test_battery_pct_is_integer(cfg):
    rng = random.Random(3)
    tags = _make_tags(cfg, rng)
    for tag in tags:
        _advance(tag, cfg, rng)
        payload = _build_payload(tag, cfg, rng)
        assert isinstance(payload["batteryPct"], int), \
            f"batteryPct must be int, got {type(payload['batteryPct'])}"


def test_battery_pct_in_range(cfg):
    rng = random.Random(4)
    tags = _make_tags(cfg, rng)
    for tag in tags:
        _advance(tag, cfg, rng)
        payload = _build_payload(tag, cfg, rng)
        assert 0 <= payload["batteryPct"] <= 100, \
            f"batteryPct out of [0, 100]: {payload['batteryPct']}"


def test_ts_is_ms_epoch_integer(cfg):
    before = int(time.time() * 1000)
    rng = random.Random(5)
    tags = _make_tags(cfg, rng)
    _advance(tags[0], cfg, rng)
    payload = _build_payload(tags[0], cfg, rng)
    after = int(time.time() * 1000) + 50  # 50 ms slack for slow machines
    assert isinstance(payload["ts"], int), "ts must be int"
    assert before <= payload["ts"] <= after, \
        f"ts {payload['ts']} not in [{before}, {after}]"


def test_zone_id_is_non_empty_string(cfg):
    rng = random.Random(6)
    tags = _make_tags(cfg, rng)
    for tag in tags:
        _advance(tag, cfg, rng)
        payload = _build_payload(tag, cfg, rng)
        assert isinstance(payload["zoneId"], str) and payload["zoneId"], \
            "zoneId must be a non-empty string"


def test_floor_is_non_negative_integer(cfg):
    rng = random.Random(7)
    tags = _make_tags(cfg, rng)
    for tag in tags:
        _advance(tag, cfg, rng)
        payload = _build_payload(tag, cfg, rng)
        assert isinstance(payload["floor"], int) and payload["floor"] >= 0, \
            f"floor must be non-negative int, got {payload['floor']}"


# ---------------------------------------------------------------------------
# Physics sanity
# ---------------------------------------------------------------------------

def test_rssi_weakens_with_distance(cfg):
    """Closer distance → stronger (higher) RSSI on average."""
    rng = random.Random(99)
    # Sample many readings to average out Gaussian noise
    near = [rssi_dbm(1.0, cfg, rng) for _ in range(200)]
    far  = [rssi_dbm(20.0, cfg, rng) for _ in range(200)]
    assert sum(near) / len(near) > sum(far) / len(far), \
        "Log-distance model: near RSSI should be stronger than far RSSI on average"


# ---------------------------------------------------------------------------
# 100-tag × multi-tick full validation (Phase 3 exit criterion)
# ---------------------------------------------------------------------------

def test_100_tags_3_ticks_all_valid(cfg, schema):
    """100 tags × 3 ticks — every payload must satisfy the JSON Schema."""
    rng = random.Random(42)
    tags = _make_tags(cfg, rng)
    assert len(tags) == 100
    errors: list[str] = []
    for tick in range(3):
        for tag in tags:
            _advance(tag, cfg, rng)
            payload = _build_payload(tag, cfg, rng)
            try:
                jsonschema.validate(payload, schema)
            except jsonschema.ValidationError as e:
                errors.append(f"tick={tick} tag={tag.tag_id}: {e.message}")
    assert not errors, f"{len(errors)} validation failure(s):\n" + "\n".join(errors[:5])


# ---------------------------------------------------------------------------
# Load mode — schema must still hold under max-rate emission
# ---------------------------------------------------------------------------

def test_load_mode_payloads_valid(cfg_load, schema):
    rng = random.Random(0)
    tags = _make_tags(cfg_load, rng)
    for tag in tags:
        _advance(tag, cfg_load, rng)
        payload = _build_payload(tag, cfg_load, rng)
        jsonschema.validate(payload, schema)


# ---------------------------------------------------------------------------
# Battery drain behaviour
# ---------------------------------------------------------------------------

def test_battery_drains_over_time(cfg):
    rng = random.Random(11)
    tags = _make_tags(cfg, rng)
    initial = [t.battery_pct for t in tags]
    for _ in range(500):
        for tag in tags:
            _advance(tag, cfg, rng)
    final = [t.battery_pct for t in tags]
    assert all(f <= i for f, i in zip(final, initial)), \
        "Battery should only decrease over time"


# ---------------------------------------------------------------------------
# synthetic_events.jsonl — regression guard (FLO-25)
#
# generate_synthetic.py produces ≥10,000 deterministic events that represent
# a 3-hour simulated workday.  These tests validate the file against the
# canonical JSON Schema and act as a drift gate before Phase 4 integration.
# ---------------------------------------------------------------------------

def _iter_synthetic_events() -> list[dict]:
    """Load all non-empty lines from synthetic_events.jsonl."""
    assert SYNTHETIC_EVENTS_PATH.exists(), (
        f"synthetic_events.jsonl missing: {SYNTHETIC_EVENTS_PATH}\n"
        "Run: python generate_synthetic.py"
    )
    events = []
    with SYNTHETIC_EVENTS_PATH.open() as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                events.append((lineno, json.loads(raw)))
            except json.JSONDecodeError as exc:
                pytest.fail(f"Line {lineno}: invalid JSON — {exc}")
    return events


def test_synthetic_events_file_exists():
    """data/synthetic_events.jsonl must be present in the repo."""
    assert SYNTHETIC_EVENTS_PATH.exists(), (
        f"Missing {SYNTHETIC_EVENTS_PATH} — run: python generate_synthetic.py"
    )


def test_synthetic_events_min_count():
    """JSONL must contain at least 10,000 events (generate_synthetic.py guarantee)."""
    events = _iter_synthetic_events()
    assert len(events) >= 10_000, (
        f"Expected ≥10,000 events, found {len(events)}. "
        "Re-run: python generate_synthetic.py"
    )


def test_synthetic_events_all_valid(schema):
    """
    Every event in synthetic_events.jsonl must conform to tag_event.json schema.

    This is the Phase 3 exit-criteria regression gate: it catches schema drift
    between generate_synthetic.py and the ingest-fn contract before Phase 4.
    Fail-fast after the first 10 violations to keep CI output readable.
    """
    validator = jsonschema.Draft202012Validator(schema)
    events = _iter_synthetic_events()
    failures: list[str] = []

    for lineno, event in events:
        for err in validator.iter_errors(event):
            failures.append(f"line {lineno}: {err.message}")
        if len(failures) >= 10:
            break

    assert not failures, (
        f"{len(failures)} schema violation(s) in {SYNTHETIC_EVENTS_PATH.name} "
        f"(showing first {len(failures)}):\n" + "\n".join(failures)
    )


def test_synthetic_events_required_fields_all_present(schema):
    """
    Spot-check: every required field must exist on every event.
    Complements the full schema validation above.
    """
    required = schema["required"]
    events = _iter_synthetic_events()
    missing: list[str] = []

    for lineno, event in events[:500]:    # sample first 500 for speed
        for field in required:
            if field not in event:
                missing.append(f"line {lineno}: missing '{field}'")

    assert not missing, "\n".join(missing[:10])


def test_synthetic_events_zone_ids_match_site_config(site):
    """
    Regression guard: every zoneId in the JSONL must appear in sample_site.json.

    Catches drift when generate_synthetic.py zone IDs diverge from the site
    config that ft-sim and ingest-fn actually use.
    """
    valid_zone_ids = {z.id for z in site.zones}
    events = _iter_synthetic_events()
    unknown: set[str] = set()

    for _lineno, event in events:
        zid = event.get("zoneId", "")
        if zid not in valid_zone_ids:
            unknown.add(zid)

    assert not unknown, (
        f"JSONL contains zone IDs not in sample_site.json: {sorted(unknown)}\n"
        "Re-run: python generate_synthetic.py  (after updating ZONES constant)"
    )


def test_synthetic_events_rssi_all_in_range(schema):
    """
    Spot-check RSSI bounds across all events — a fast range guard distinct
    from full schema validation.
    """
    events = _iter_synthetic_events()
    out_of_range = [
        (ln, ev["rssi"])
        for ln, ev in events
        if not (-120 <= ev.get("rssi", 0) <= 0)
    ]
    assert not out_of_range, (
        f"{len(out_of_range)} event(s) with RSSI outside [-120, 0]:\n"
        + "\n".join(f"  line {ln}: rssi={v}" for ln, v in out_of_range[:5])
    )


def test_synthetic_events_ts_monotonically_non_decreasing():
    """
    Events are sorted by ts in the JSONL (generate_synthetic.py guarantees this).
    Catches accidental re-generation without sorting.
    """
    events = _iter_synthetic_events()
    prev_ts: int | None = None
    for lineno, event in events:
        ts = event.get("ts", 0)
        if prev_ts is not None and ts < prev_ts:
            pytest.fail(
                f"Timestamp decreased at line {lineno}: "
                f"{ts} < {prev_ts} (events must be sorted ascending by ts)"
            )
        prev_ts = ts
