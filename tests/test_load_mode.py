"""
test_load_mode.py — Tests for sim_publisher load mode and load_runner.

Covers:
  - Load mode disables stochastic zone transitions
  - --dry-run emits STATS line and returns 0
  - 100 tags × 1 Hz × 5 s throughput via dry-run (scaled-down Phase 6 gate)
  - --tag-offset produces non-overlapping tag ID ranges
  - load_runner _parse_stats correctly extracts STATS line
  - load_runner report() pass/fail thresholds
"""

from __future__ import annotations

import asyncio
import random
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest

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
    run,
)
from load_runner import ProcessResult, _parse_stats, report  # noqa: E402

SITE_CONFIG_PATH = ROOT / "config" / "sample_site.json"
SIM_PUBLISHER    = ROOT / "sim_publisher.py"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def site() -> SiteConfig:
    return load_site(SITE_CONFIG_PATH)


def make_cfg(site, *, load_mode=False, dry_run=False, num_tags=10,
             rate=1.0, duration=1.0, seed=42, tag_offset=1) -> SimConfig:
    return SimConfig(
        broker="localhost",
        port=1883,
        tls=False,
        username=None,
        password=None,
        tenant="test-load",
        site=site,
        num_tags=num_tags,
        tag_offset=tag_offset,
        rate=rate,
        duration=duration,
        load_mode=load_mode,
        dry_run=dry_run,
        seed=seed,
    )


# ---------------------------------------------------------------------------
# Load mode: disables stochastic zone transitions
# ---------------------------------------------------------------------------

def test_load_mode_does_not_change_zone(site):
    """In load mode, zone must not change between advance() calls."""
    cfg = make_cfg(site, load_mode=True, num_tags=20)
    rng = random.Random(0)
    tags = _make_tags(cfg, rng)
    initial_zones = {t.tag_id: t.zone.id for t in tags}

    # Force all next_transition in the past so normal mode WOULD transition
    for tag in tags:
        tag.next_transition = time.monotonic() - 999

    for _ in range(5):
        for tag in tags:
            _advance(tag, cfg, rng)

    final_zones = {t.tag_id: t.zone.id for t in tags}
    assert initial_zones == final_zones, \
        "Load mode must freeze zone transitions"


def test_normal_mode_does_change_zone(site):
    """Sanity: normal mode DOES eventually change zones (given forced transition time)."""
    cfg = make_cfg(site, load_mode=False, num_tags=20)
    rng = random.Random(7)
    tags = _make_tags(cfg, rng)
    initial_zones = {t.tag_id: t.zone.id for t in tags}

    # Force all past-due transitions
    for tag in tags:
        tag.next_transition = time.monotonic() - 999

    for tag in tags:
        _advance(tag, cfg, rng)

    final_zones = {t.tag_id: t.zone.id for t in tags}
    changed = sum(1 for tid in initial_zones if initial_zones[tid] != final_zones[tid])
    assert changed > 0, "Normal mode must allow zone transitions"


# ---------------------------------------------------------------------------
# Tag offset: non-overlapping tag ID ranges
# ---------------------------------------------------------------------------

def test_tag_offset_produces_unique_ids(site):
    """Two configs with different tag_offset must produce disjoint tag ID sets."""
    cfg_a = make_cfg(site, num_tags=50, tag_offset=1)
    cfg_b = make_cfg(site, num_tags=50, tag_offset=51)
    rng_a = random.Random(1)
    rng_b = random.Random(2)
    ids_a = {t.tag_id for t in _make_tags(cfg_a, rng_a)}
    ids_b = {t.tag_id for t in _make_tags(cfg_b, rng_b)}
    assert ids_a.isdisjoint(ids_b), "Tag ID ranges must not overlap across processes"


def test_tag_offset_numbering(site):
    """tag_offset controls the starting number in tag IDs."""
    cfg = make_cfg(site, num_tags=5, tag_offset=101)
    rng = random.Random(0)
    tags = _make_tags(cfg, rng)
    ids = [t.tag_id for t in tags]
    assert ids == ["tag-0101", "tag-0102", "tag-0103", "tag-0104", "tag-0105"]


# ---------------------------------------------------------------------------
# Dry-run via subprocess — STATS line + exit code
# ---------------------------------------------------------------------------

def test_dry_run_emits_stats_line_and_exits_zero():
    """sim_publisher --dry-run must emit a STATS line and exit with code 0."""
    result = subprocess.run(
        [
            sys.executable, str(SIM_PUBLISHER),
            "--tenant-id", "bench",
            "--site",       str(SITE_CONFIG_PATH),
            "--tags",       "5",
            "--rate",       "2.0",
            "--duration",   "2.0",
            "--load",
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, f"Expected exit 0, got {result.returncode}\nstderr: {result.stderr}"
    stats = _parse_stats(result.stdout)
    assert stats, f"No STATS line in stdout:\n{result.stdout}"
    assert stats["errors"] == 0
    assert stats["published"] > 0


# ---------------------------------------------------------------------------
# Throughput gate — 100 tags × 1 Hz × 5 s (scaled-down Phase 6 proxy)
# Phase 6 full: 1000 tags × 1 Hz — tested via load_runner.
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_throughput_100_tags_1hz_5s():
    """
    FLO-23 pass-gate: 100 tags × 1 Hz × 5 s in dry-run + load mode must
    achieve ≥ 95 events/s (batched single-coroutine loop).

    Full 60 s run is exercised by the CI stress job:
      python load_runner.py --tags 100 --duration 60 --load --dry-run
    """
    duration = 5.0
    target_rate = 100.0      # 100 tags × 1 Hz

    result = subprocess.run(
        [
            sys.executable, str(SIM_PUBLISHER),
            "--tenant-id", "bench",
            "--site",       str(SITE_CONFIG_PATH),
            "--tags",       "100",
            "--rate",       "1.0",
            "--duration",   str(duration),
            "--load",        # enables batched loop
            "--dry-run",
            "--seed",       "42",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"Process failed\nstderr: {result.stderr}"
    stats = _parse_stats(result.stdout)
    assert stats, f"No STATS line found in:\n{result.stdout}"
    assert stats["errors"] == 0, f"Got {stats['errors']} errors"

    actual_rate = stats["rate"]
    assert actual_rate >= target_rate * 0.95, (
        f"Throughput {actual_rate:.1f} msg/s < 95% of target {target_rate} msg/s\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# Backward compat: --tenant still works
# ---------------------------------------------------------------------------

def test_tenant_flag_backward_compat():
    """Legacy --tenant flag must be accepted without error."""
    result = subprocess.run(
        [
            sys.executable, str(SIM_PUBLISHER),
            "--tenant",   "legacy-compat",
            "--site",     str(SITE_CONFIG_PATH),
            "--tags",     "2",
            "--rate",     "1.0",
            "--duration", "1.0",
            "--load",      # use batched loop so teardown is prompt
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, f"--tenant flag rejected:\n{result.stderr}"


# ---------------------------------------------------------------------------
# load_runner._parse_stats
# ---------------------------------------------------------------------------

def test_parse_stats_valid():
    output = (
        "15:00:00  INFO    something\n"
        "STATS published=6000 errors=0 duration=60.01 rate=99.98\n"
    )
    s = _parse_stats(output)
    assert s["published"] == 6000
    assert s["errors"]    == 0
    assert abs(s["duration"] - 60.01) < 0.01
    assert abs(s["rate"]     - 99.98) < 0.01


def test_parse_stats_missing():
    assert _parse_stats("no stats here") == {}


# ---------------------------------------------------------------------------
# load_runner.report — pass / fail logic
# ---------------------------------------------------------------------------

def _make_result(**kwargs) -> ProcessResult:
    defaults = dict(pid=1, tag_offset=1, num_tags=100,
                    published=6000, errors=0, duration=60.0,
                    rate=100.0, returncode=0)
    return ProcessResult(**{**defaults, **kwargs})


def test_report_passes_zero_errors(capsys):
    results = [_make_result(published=6000, errors=0, rate=100.0)]
    rc = report(results, target_rate=1.0, total_tags=100, duration=60.0)
    assert rc == 0


def test_report_fails_on_errors(capsys):
    results = [_make_result(errors=5, rate=100.0)]
    rc = report(results, target_rate=1.0, total_tags=100, duration=60.0)
    assert rc == 1


def test_report_fails_below_rate(capsys):
    # rate is 50 msg/s — below 95% of 100 target
    results = [_make_result(errors=0, rate=50.0)]
    rc = report(results, target_rate=1.0, total_tags=100, duration=60.0)
    assert rc == 1


def test_report_passes_at_95pct_rate(capsys):
    results = [_make_result(errors=0, rate=95.1)]
    rc = report(results, target_rate=1.0, total_tags=100, duration=60.0)
    assert rc == 0


def test_report_aggregates_multiple_workers(capsys):
    """Two workers at 500 msg/s each should pass 1000-tag target."""
    r1 = _make_result(pid=1, tag_offset=1,   num_tags=500, rate=500.0, errors=0)
    r2 = _make_result(pid=2, tag_offset=501, num_tags=500, rate=500.0, errors=0)
    rc = report([r1, r2], target_rate=1.0, total_tags=1000, duration=60.0)
    assert rc == 0
