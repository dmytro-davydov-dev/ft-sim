#!/usr/bin/env python3
"""
sim_publisher.py — Flowterra BLE tag MQTT simulator.

Publishes synthetic tag-sighting events to an EMQX broker, producing
payloads identical to those a real Minew G1 BLE-to-MQTT gateway would send.

  Topic : iot-ingress/{customerId}/{gatewayId}
  Schema: sim/schema/tag_event.json
  QoS   : 1 (at-least-once)

Modes
-----
Normal  — tags move stochastically between zones (dwell time ~ Exp(λ)).
Load    — stochastic movement disabled; publishes at maximum rate for
          stress / load testing (--load flag).

Usage — local dev
-----------------
  python sim_publisher.py \\
      --broker localhost --port 1883 \\
      --tenant acme --site config/sample_site.json \\
      --tags 100 --rate 1.0 --duration 60 --seed 42

Usage — deployed EMQX (TLS + credentials)
------------------------------------------
  MQTT_PASSWORD=secret python sim_publisher.py \\
      --broker emqx.dev.flowterra.io --port 8883 --tls \\
      --username sim-user \\
      --tenant acme --site config/sample_site.json \\
      --tags 100 --rate 1.0 --duration 60

Usage — load / stress test
---------------------------
  python sim_publisher.py \\
      --broker localhost --port 1883 \\
      --tenant acme --site config/sample_site.json \\
      --tags 100 --rate 10.0 --duration 60 --load
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sim_publisher")


# ---------------------------------------------------------------------------
# Site config
# ---------------------------------------------------------------------------
@dataclass
class Zone:
    id: str
    label: str
    floor: int
    ref_dist_m: float   # typical tag-to-anchor distance in this zone


@dataclass
class SiteConfig:
    site_id: str
    gateway_id: str
    zones: list[Zone]


def load_site(path: Path) -> SiteConfig:
    """Load and parse a site config JSON file."""
    with path.open() as f:
        d = json.load(f)
    zones = [
        Zone(
            id=z["id"],
            label=z["label"],
            floor=z.get("floor", 1),
            ref_dist_m=z["ref_dist_m"],
        )
        for z in d["zones"]
    ]
    if not zones:
        raise ValueError(f"Site config at {path} has no zones defined.")
    return SiteConfig(
        site_id=d["siteId"],
        gateway_id=d["gatewayId"],
        zones=zones,
    )


# ---------------------------------------------------------------------------
# Simulation config
# ---------------------------------------------------------------------------
@dataclass
class SimConfig:
    # Connection
    broker: str
    port: int
    tls: bool
    username: Optional[str]
    password: Optional[str]

    # Identity
    tenant: str          # customerId
    site: SiteConfig

    # Load
    num_tags: int
    rate: float          # Hz per tag
    duration: float      # seconds to run (0 = run forever)
    load_mode: bool      # True → disable stochastic movement
    tag_offset: int = 1  # First tag number (for multi-process partitioning)

    # Reproducibility
    seed: Optional[int] = None

    # Physics — log-distance path-loss model
    path_loss_exp: float = 2.5      # n (2.0 free-space, 3–4 indoor)
    tx_power_dbm: float = -40.0     # RSSI at d₀ = 1 m (typical BLE)
    rssi_std_dbm: float = 15.0      # Gaussian noise σ

    # Zone dwell
    mean_dwell_s: float = 15.0      # avg seconds before zone transition

    # Battery
    battery_drain_per_event: float = 0.002   # % per publish event

    # Optional schema validation
    validate: bool = False

    # Dry-run — skip MQTT; just generate and count events (for benchmarking)
    dry_run: bool = False


# ---------------------------------------------------------------------------
# RSSI model: log-distance path-loss + Gaussian noise → integer dBm
# ---------------------------------------------------------------------------
def rssi_dbm(dist_m: float, cfg: SimConfig, rng: random.Random) -> int:
    dist_m = max(dist_m, 0.1)
    path_loss = 10.0 * cfg.path_loss_exp * math.log10(dist_m)
    noise = rng.gauss(0.0, cfg.rssi_std_dbm)
    return int(round(cfg.tx_power_dbm - path_loss + noise))


# ---------------------------------------------------------------------------
# Tag state
# ---------------------------------------------------------------------------
@dataclass
class TagState:
    tag_id: str
    zone: Zone
    dist_m: float
    battery_pct: float
    next_transition: float   # monotonic clock of next zone change


def _make_tags(cfg: SimConfig, rng: random.Random) -> list[TagState]:
    tags: list[TagState] = []
    for i in range(cfg.num_tags):
        zone = rng.choice(cfg.site.zones)
        dist = rng.uniform(zone.ref_dist_m * 0.5, zone.ref_dist_m * 1.5)
        battery = rng.uniform(40.0, 100.0)
        next_t = time.monotonic() + rng.expovariate(1.0 / cfg.mean_dwell_s)
        tag_num = cfg.tag_offset + i
        tags.append(TagState(
            tag_id=f"tag-{tag_num:04d}",
            zone=zone,
            dist_m=dist,
            battery_pct=battery,
            next_transition=next_t,
        ))
    return tags


def _advance(tag: TagState, cfg: SimConfig, rng: random.Random) -> None:
    """Advance tag state: zone transition (normal mode) + battery drain."""
    if not cfg.load_mode:
        now = time.monotonic()
        if now >= tag.next_transition:
            candidates = [z for z in cfg.site.zones if z.id != tag.zone.id]
            if candidates:
                tag.zone = rng.choice(candidates)
            tag.dist_m = rng.uniform(
                tag.zone.ref_dist_m * 0.5,
                tag.zone.ref_dist_m * 1.5,
            )
            tag.next_transition = now + rng.expovariate(1.0 / cfg.mean_dwell_s)
    tag.battery_pct = max(0.0, tag.battery_pct - cfg.battery_drain_per_event)


def _build_payload(tag: TagState, cfg: SimConfig, rng: random.Random) -> dict:
    """Build a tag_event payload matching sim/schema/tag_event.json."""
    return {
        "customerId": cfg.tenant,
        "siteId":     cfg.site.site_id,
        "gatewayId":  cfg.site.gateway_id,
        "tagId":      tag.tag_id,
        "rssi":       rssi_dbm(tag.dist_m, cfg, rng),
        "zoneId":     tag.zone.id,
        "ts":         int(time.time() * 1000),           # ms epoch UTC
        "floor":      tag.zone.floor,
        "batteryPct": max(0, min(100, int(round(tag.battery_pct)))),
    }


# ---------------------------------------------------------------------------
# Optional JSON Schema validation
# ---------------------------------------------------------------------------
_SCHEMA: Optional[dict] = None

def _validate_payload(payload: dict, schema_path: Path) -> None:
    import jsonschema
    global _SCHEMA
    if _SCHEMA is None:
        with schema_path.open() as f:
            _SCHEMA = json.load(f)
    jsonschema.validate(payload, _SCHEMA)


# ---------------------------------------------------------------------------
# MQTT client — paho-mqtt >= 2.0 callback API
# ---------------------------------------------------------------------------
def _make_client(cfg: SimConfig) -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"flowterra-sim-{cfg.tenant}",
    )
    if cfg.username:
        client.username_pw_set(cfg.username, cfg.password)
    if cfg.tls:
        import ssl as _ssl
        client.tls_set(cert_reqs=_ssl.CERT_NONE)  # self-signed cert — skip verification in dev

    def on_connect(client, userdata, connect_flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("MQTT connect failed: %s", reason_code)
        else:
            log.info("Connected → %s:%d  tls=%s", cfg.broker, cfg.port, cfg.tls)

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        if reason_code and reason_code.value != 0:
            log.warning("Unexpected disconnect: %s", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    return client


# ---------------------------------------------------------------------------
# Main async publish loop
# ---------------------------------------------------------------------------
async def _run_load_batch(
    cfg: SimConfig,
    tags: list[TagState],
    client,
    topic: str,
    schema_path: Path,
    start: float,
) -> tuple[int, int]:
    """
    Load-mode inner loop — batched, single-coroutine design.

    All tags are advanced and published in a tight synchronous inner loop,
    with a single asyncio.sleep() per tick.  This avoids the per-task
    context-switch overhead of the per-tag coroutine model and sustains
    ≥ N × rate events/s on a single asyncio event loop.
    """
    rng = random.Random(cfg.seed)
    interval = 1.0 / cfg.rate
    total_published = 0
    total_errors = 0

    while True:
        t0 = time.monotonic()
        if cfg.duration > 0 and (t0 - start) >= cfg.duration:
            break

        for tag in tags:
            _advance(tag, cfg, rng)
            payload = _build_payload(tag, cfg, rng)

            if cfg.validate and schema_path.exists():
                try:
                    _validate_payload(payload, schema_path)
                except Exception as exc:
                    log.error("Schema validation: %s  payload=%s", exc, payload)
                    total_errors += 1

            if cfg.dry_run:
                total_published += 1
            else:
                msg_info = client.publish(topic, json.dumps(payload), qos=1)
                if msg_info.rc == mqtt.MQTT_ERR_SUCCESS:
                    total_published += 1
                else:
                    total_errors += 1
                    log.warning("publish error rc=%d tag=%s", msg_info.rc, tag.tag_id)

        tick_elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0.0, interval - tick_elapsed))

    return total_published, total_errors


async def run(cfg: SimConfig) -> int:
    """Run the simulation. Returns exit code (0 = success, 1 = errors)."""
    schema_path = Path(__file__).parent / "schema" / "tag_event.json"
    rng = random.Random(cfg.seed)
    tags = _make_tags(cfg, rng)
    topic = f"iot-ingress/{cfg.tenant}/{cfg.site.gateway_id}"

    if cfg.dry_run:
        client = None
        log.info("DRY-RUN mode — MQTT publish skipped")
    else:
        client = _make_client(cfg)
        client.connect(cfg.broker, cfg.port, keepalive=60)
        client.loop_start()
        await asyncio.sleep(0.5)   # allow MQTT thread to complete handshake

    interval = 1.0 / cfg.rate
    start = time.monotonic()
    total_published = 0
    total_errors = 0
    mode = "LOAD+DRY" if (cfg.load_mode and cfg.dry_run) else \
           "LOAD"     if cfg.load_mode else \
           "DRY"      if cfg.dry_run   else "NORMAL"

    log.info(
        "[%s] %d tags × %.1f Hz × %.0fs → topic=%s  site=%s  offset=%d",
        mode, cfg.num_tags, cfg.rate, cfg.duration, topic,
        cfg.site.site_id, cfg.tag_offset,
    )

    try:
        if cfg.load_mode:
            # Batched single-coroutine loop: all tags fired per tick, one sleep.
            # Far lower asyncio overhead than N concurrent per-tag coroutines —
            # sustains 1k tags × 1 Hz from a single process.
            total_published, total_errors = await _run_load_batch(
                cfg, tags, client, topic, schema_path, start,
            )
        else:
            # Normal mode: per-tag coroutines with stochastic zone movement.
            done_event = asyncio.Event()

            async def publish_tag(tag: TagState, offset: float) -> None:
                nonlocal total_published, total_errors
                await asyncio.sleep(offset)
                while True:
                    t0 = time.monotonic()
                    if cfg.duration > 0 and (t0 - start) >= cfg.duration:
                        return

                    _advance(tag, cfg, rng)
                    payload = _build_payload(tag, cfg, rng)

                    if cfg.validate and schema_path.exists():
                        try:
                            _validate_payload(payload, schema_path)
                        except Exception as exc:
                            log.error("Schema validation: %s  payload=%s", exc, payload)
                            total_errors += 1

                    if cfg.dry_run:
                        total_published += 1
                    else:
                        msg_info = client.publish(topic, json.dumps(payload), qos=1)
                        if msg_info.rc == mqtt.MQTT_ERR_SUCCESS:
                            total_published += 1
                        else:
                            total_errors += 1
                            log.warning("publish error rc=%d tag=%s", msg_info.rc, tag.tag_id)

                    elapsed = time.monotonic() - t0
                    await asyncio.sleep(max(0.0, interval - elapsed))

            async def reporter() -> None:
                """Periodic progress log; exits promptly once done_event is set."""
                REPORT_INTERVAL = 10.0
                POLL_SLICE      = 0.5        # check done_event twice per second
                elapsed_since_report = 0.0
                while not done_event.is_set():
                    await asyncio.sleep(POLL_SLICE)
                    elapsed_since_report += POLL_SLICE
                    elapsed = time.monotonic() - start
                    if elapsed_since_report >= REPORT_INTERVAL:
                        elapsed_since_report = 0.0
                        rate = total_published / elapsed if elapsed > 0 else 0.0
                        log.info(
                            "t=%.0fs  published=%d  errors=%d  rate=%.1f msg/s",
                            elapsed, total_published, total_errors, rate,
                        )

            # Spread first ticks uniformly to avoid startup burst
            offsets = [i * interval / max(cfg.num_tags, 1) for i in range(cfg.num_tags)]
            pub_tasks = [
                asyncio.create_task(publish_tag(tag, offsets[i]))
                for i, tag in enumerate(tags)
            ]
            reporter_task = asyncio.create_task(reporter())
            try:
                await asyncio.gather(*pub_tasks)
            except asyncio.CancelledError:
                pass
            finally:
                done_event.set()
                await reporter_task

    except asyncio.CancelledError:
        pass
    finally:
        if client is not None:
            client.loop_stop()
            client.disconnect()

    elapsed = time.monotonic() - start
    actual_rate = total_published / elapsed if elapsed > 0 else 0.0
    log.info(
        "Done — published=%d  errors=%d  duration=%.1fs  rate=%.1f msg/s",
        total_published, total_errors, elapsed, actual_rate,
    )
    # Emit machine-readable summary line for load_runner to parse
    print(
        f"STATS published={total_published} errors={total_errors} "
        f"duration={elapsed:.2f} rate={actual_rate:.2f}",
        flush=True,
    )
    return 1 if total_errors > 0 else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args() -> SimConfig:
    p = argparse.ArgumentParser(
        description="Flowterra sim-publisher — BLE tag MQTT simulator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    conn = p.add_argument_group("Connection")
    conn.add_argument("--broker",   default="localhost", help="MQTT broker host")
    conn.add_argument("--port",     type=int, default=1883, help="MQTT port (8883 for TLS)")
    conn.add_argument("--tls",      action="store_true",
                      help="Enable TLS/SSL (required for deployed EMQX)")
    conn.add_argument("--username", default=None, help="MQTT username (EMQX ACL)")
    conn.add_argument("--password", default=None,
                      help="MQTT password (prefer env var MQTT_PASSWORD)")

    ident = p.add_argument_group("Identity")
    # --tenant-id is the canonical flag; --tenant kept for backward compatibility
    ident_tenant = ident.add_mutually_exclusive_group(required=True)
    ident_tenant.add_argument("--tenant-id", dest="tenant",
                              help="customerId injected into every event")
    ident_tenant.add_argument("--tenant",
                              help="Alias for --tenant-id (backward compatible)")
    ident.add_argument("--site", required=True, type=Path,
                       help="Path to site config JSON (e.g. config/sample_site.json)")

    load = p.add_argument_group("Load")
    load.add_argument("--tags",       type=int,   default=10,  dest="num_tags",
                      help="Number of simulated tags")
    load.add_argument("--tag-offset", type=int,   default=1,
                      help="Starting tag number — use for multi-process partitioning")
    load.add_argument("--rate",       type=float, default=1.0,
                      help="Publish rate in Hz (per tag)")
    load.add_argument("--duration",   type=float, default=60.0,
                      help="Run duration in seconds (0 = run forever)")
    load.add_argument("--load",       action="store_true", dest="load_mode",
                      help="Disable zone movement; emit at max rate for stress testing")
    load.add_argument("--dry-run",    action="store_true",
                      help="Generate events without publishing to MQTT (benchmarking)")
    load.add_argument("--seed",       type=int,   default=None,
                      help="RNG seed for reproducible runs")

    phys = p.add_argument_group("Physics")
    phys.add_argument("--path-loss-exp", type=float, default=2.5, metavar="N",
                      help="Log-distance path-loss exponent (2=free-space, 3–4=indoor)")
    phys.add_argument("--tx-power",      type=float, default=-40.0,
                      help="TX power / RSSI at 1m reference distance (dBm)")
    phys.add_argument("--rssi-std",      type=float, default=15.0,
                      help="Gaussian RSSI noise std dev (dBm)")
    phys.add_argument("--mean-dwell",    type=float, default=15.0,
                      help="Mean dwell time per zone (seconds)")

    p.add_argument("--validate", action="store_true",
                   help="Validate every payload against sim/schema/tag_event.json (slower)")

    a = p.parse_args()
    site = load_site(a.site)
    password = a.password or os.environ.get("MQTT_PASSWORD")

    return SimConfig(
        broker=a.broker,
        port=a.port,
        tls=a.tls,
        username=a.username,
        password=password,
        tenant=a.tenant,
        site=site,
        num_tags=a.num_tags,
        tag_offset=a.tag_offset,
        rate=a.rate,
        duration=a.duration,
        load_mode=a.load_mode,
        dry_run=a.dry_run,
        seed=a.seed,
        path_loss_exp=a.path_loss_exp,
        tx_power_dbm=a.tx_power,
        rssi_std_dbm=a.rssi_std,
        mean_dwell_s=a.mean_dwell,
        validate=a.validate,
    )


if __name__ == "__main__":
    cfg = _parse_args()
    try:
        code = asyncio.run(run(cfg))
        raise SystemExit(code)
    except KeyboardInterrupt:
        log.info("Interrupted.")
