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

SCHEMA_PATH = ROOT / "schema" / "tag_event.json"
SITE_CONFIG_PATH = ROOT / "config" / "sample_site.json"


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
