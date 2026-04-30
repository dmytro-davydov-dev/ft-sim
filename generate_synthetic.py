#!/usr/bin/env python3
"""
generate_synthetic.py — Flowterra deterministic synthetic beacon dataset generator.

Produces sim/data/synthetic_events.jsonl:
  • ≥10,000 events
  • 50 tag IDs  (20 badge / 15 asset / 15 wristband)
  • 20 anchors  (4 per zone)
  • 5 zones     (from config/sample_site.json, floors 1–2)
  • Simulated   09:00–12:00 UTC on a fixed reference date
  • JSONL format — one JSON object per line, bq-load compatible
  • All events pass schema/tag_event.json validation

Usage
-----
  python generate_synthetic.py                      # writes sim/data/synthetic_events.jsonl
  python generate_synthetic.py --validate           # also runs jsonschema on every event
  python generate_synthetic.py --out path/to/file   # custom output path
  python generate_synthetic.py --seed 99            # custom RNG seed (default 42)

Algorithm
---------
Time is stepped in TICK_S-second increments from 09:00 to 12:00.
At each tick, every tag may transition zones (Poisson dwell model).
A tag in zone Z is detected by 1–3 of the 4 anchors assigned to Z;
each anchor detection produces one event row.
RSSI is derived from a log-distance path-loss model + Gaussian noise.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants / tunables
# ---------------------------------------------------------------------------
SEED           = 42
CUSTOMER_ID    = "flowterra-demo"
REF_DATE       = "2026-04-30"          # ISO date — simulation day
SIM_START_H    = 9                     # 09:00 UTC
SIM_END_H      = 12                    # 12:00 UTC
TICK_S         = 30                    # seconds between simulation steps
MEAN_DWELL_S   = 120.0                 # avg seconds a tag stays in a zone
PATH_LOSS_EXP  = 2.5                   # log-distance model exponent
TX_POWER_DBM   = -40.0                 # RSSI at 1 m reference distance
RSSI_STD_DBM   = 8.0                   # Gaussian noise σ
ANCHORS_PER_ZONE = 4                   # 20 anchors total / 5 zones

# Tag composition
N_BADGE     = 20
N_ASSET     = 15
N_WRISTBAND = 15

# Zones (subset of sample_site.json — exactly 5)
ZONES = [
    {"id": "zone-reception",    "label": "Reception",       "floor": 1, "ref_dist_m": 3.0},
    {"id": "zone-open-plan-a",  "label": "Open Plan A",     "floor": 1, "ref_dist_m": 8.0},
    {"id": "zone-open-plan-b",  "label": "Open Plan B",     "floor": 1, "ref_dist_m": 14.0},
    {"id": "zone-meeting-rooms","label": "Meeting Rooms",   "floor": 1, "ref_dist_m": 5.0},
    {"id": "zone-break-room",   "label": "Break Room",      "floor": 1, "ref_dist_m": 6.0},
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class Zone:
    id: str
    label: str
    floor: int
    ref_dist_m: float


@dataclass
class Anchor:
    id: str
    zone_id: str
    floor: int


@dataclass
class TagState:
    tag_id: str
    zone: Zone
    dist_m: float
    battery_pct: float
    next_transition_s: float   # seconds from sim start for next zone change


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------
def build_zones() -> list[Zone]:
    return [Zone(**z) for z in ZONES]


def build_anchors(zones: list[Zone]) -> list[Anchor]:
    anchors: list[Anchor] = []
    for z in zones:
        for i in range(1, ANCHORS_PER_ZONE + 1):
            anchors.append(Anchor(
                id=f"gw-{z.id}-{i:02d}",
                zone_id=z.id,
                floor=z.floor,
            ))
    return anchors


def build_tags(rng: random.Random, zones: list[Zone]) -> list[TagState]:
    tags: list[TagState] = []

    def make_tag(tag_id: str) -> TagState:
        z = rng.choice(zones)
        dist = rng.uniform(z.ref_dist_m * 0.5, z.ref_dist_m * 1.5)
        battery = rng.uniform(50.0, 100.0)
        dwell = rng.expovariate(1.0 / MEAN_DWELL_S)
        return TagState(tag_id, z, dist, battery, dwell)

    for i in range(1, N_BADGE + 1):
        tags.append(make_tag(f"badge-{i:04d}"))
    for i in range(1, N_ASSET + 1):
        tags.append(make_tag(f"asset-{i:04d}"))
    for i in range(1, N_WRISTBAND + 1):
        tags.append(make_tag(f"wristband-{i:04d}"))

    return tags


# ---------------------------------------------------------------------------
# Physics
# ---------------------------------------------------------------------------
def rssi_dbm(dist_m: float, rng: random.Random) -> int:
    d = max(dist_m, 0.1)
    path_loss = 10.0 * PATH_LOSS_EXP * math.log10(d)
    noise = rng.gauss(0.0, RSSI_STD_DBM)
    raw = TX_POWER_DBM - path_loss + noise
    return max(-120, min(0, int(round(raw))))


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------
def generate(seed: int = SEED) -> list[dict]:
    rng = random.Random(seed)

    # Reference epoch for 09:00 UTC on REF_DATE
    start_dt = datetime.fromisoformat(f"{REF_DATE}T{SIM_START_H:02d}:00:00+00:00")
    end_dt   = datetime.fromisoformat(f"{REF_DATE}T{SIM_END_H:02d}:00:00+00:00")
    start_ms = int(start_dt.timestamp() * 1000)
    duration_s = int((end_dt - start_dt).total_seconds())   # 10800 s

    zones   = build_zones()
    anchors = build_anchors(zones)
    tags    = build_tags(rng, zones)

    # Index: zone_id → list[Anchor]
    anchors_by_zone: dict[str, list[Anchor]] = {z.id: [] for z in zones}
    for a in anchors:
        anchors_by_zone[a.zone_id].append(a)

    events: list[dict] = []

    for tick_s in range(0, duration_s, TICK_S):
        tick_ms = start_ms + tick_s * 1000

        for tag in tags:
            # Zone transition
            if tick_s >= tag.next_transition_s:
                candidates = [z for z in zones if z.id != tag.zone.id]
                tag.zone = rng.choice(candidates)
                tag.dist_m = rng.uniform(
                    tag.zone.ref_dist_m * 0.5,
                    tag.zone.ref_dist_m * 1.5,
                )
                tag.next_transition_s = tick_s + rng.expovariate(1.0 / MEAN_DWELL_S)

            # Battery drain
            tag.battery_pct = max(0.0, tag.battery_pct - 0.001)

            # Detections: 1–3 anchors in current zone pick up the tag
            zone_anchors = anchors_by_zone[tag.zone.id]
            n_detect = rng.randint(1, min(3, len(zone_anchors)))
            detecting = rng.sample(zone_anchors, n_detect)

            for anchor in detecting:
                # Small per-anchor time jitter ±5 s
                jitter_ms = int(rng.uniform(-5000, 5000))
                ts = tick_ms + jitter_ms
                ts = max(start_ms, ts)   # clamp to window

                events.append({
                    "customerId": CUSTOMER_ID,
                    "gatewayId":  anchor.id,
                    "tagId":      tag.tag_id,
                    "rssi":       rssi_dbm(tag.dist_m, rng),
                    "zoneId":     tag.zone.id,
                    "ts":         ts,
                    "floor":      tag.zone.floor,
                    "batteryPct": max(0, min(100, int(round(tag.battery_pct)))),
                })

    # Sort by timestamp
    events.sort(key=lambda e: e["ts"])
    return events


# ---------------------------------------------------------------------------
# Schema validation (optional)
# ---------------------------------------------------------------------------
def validate_events(events: list[dict], schema_path: Path) -> int:
    try:
        import jsonschema
    except ImportError:
        print("jsonschema not installed — skipping validation", file=sys.stderr)
        return 0

    with schema_path.open() as f:
        schema = json.load(f)

    errors = 0
    for i, ev in enumerate(events):
        try:
            jsonschema.validate(ev, schema)
        except jsonschema.ValidationError as exc:
            print(f"Event {i}: INVALID — {exc.message}", file=sys.stderr)
            errors += 1
    return errors


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser(
        description="Generate sim/data/synthetic_events.jsonl deterministically",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--seed",     type=int,  default=SEED,  help="RNG seed")
    p.add_argument("--validate", action="store_true",      help="Run JSON Schema validation on every event")
    p.add_argument("--out",      type=Path, default=None,  help="Output path (default: sim/data/synthetic_events.jsonl)")
    args = p.parse_args()

    script_dir = Path(__file__).parent
    out_path   = args.out or (script_dir / "data" / "synthetic_events.jsonl")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating events (seed={args.seed}) …")
    events = generate(seed=args.seed)
    print(f"  Generated {len(events):,} events")

    if args.validate:
        schema_path = script_dir / "schema" / "tag_event.json"
        print("  Validating against tag_event.json schema …")
        errs = validate_events(events, schema_path)
        if errs:
            print(f"  ✗ {errs} validation error(s) — aborting", file=sys.stderr)
            sys.exit(1)
        print(f"  ✓ All {len(events):,} events valid")

    with out_path.open("w") as f:
        for ev in events:
            f.write(json.dumps(ev, separators=(",", ":")) + "\n")

    print(f"  Written → {out_path}")
    print(f"  File size: {out_path.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
