#!/usr/bin/env python3
"""
load_runner.py — Flowterra multi-process load test orchestrator.

Splits a large tag population across N parallel sim_publisher processes,
aggregates publish/error stats, and exits non-zero if any messages were dropped.

Design rationale
----------------
A single Python asyncio process is typically CPU-limited to ~500–600 events/sec
on commodity hardware (GIL + event-loop overhead). When a target exceeds that
threshold, load_runner automatically partitions the tag range across multiple
processes so the aggregate throughput scales linearly.

Phase 6 target: 1,000 tags × 1 Hz = 1,000 events/sec
  → 2 processes × 500 tags each comfortably hits the target.

Usage
-----
  # 100 tags × 1 Hz × 60 s, single process, against a live broker
  python load_runner.py \\
      --broker localhost --port 1883 \\
      --tenant-id acme --site config/sample_site.json \\
      --tags 100 --rate 1.0 --duration 60 --load

  # 1000 tags × 1 Hz, 2 worker processes, dry-run (no broker needed)
  python load_runner.py \\
      --tenant-id acme --site config/sample_site.json \\
      --tags 1000 --rate 1.0 --duration 60 --load --dry-run --processes 2

  # Auto-determine process count (ceil(tags / MAX_TAGS_PER_PROCESS))
  python load_runner.py \\
      --tenant-id acme --site config/sample_site.json \\
      --tags 1000 --rate 1.0 --duration 60 --load --dry-run --auto-scale

Exit codes
----------
  0  All processes finished with zero errors and target throughput reached.
  1  One or more processes reported dropped messages or below-target rate.
  2  Argument / configuration error.
"""

from __future__ import annotations

import argparse
import math
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# Max tags a single sim_publisher process can handle at the target rate
# before GIL pressure causes drift.  Empirically safe on CPython 3.10.
MAX_TAGS_PER_PROCESS = 500


# ---------------------------------------------------------------------------
# Stats model
# ---------------------------------------------------------------------------
@dataclass
class ProcessResult:
    pid: int
    tag_offset: int
    num_tags: int
    published: int
    errors: int
    duration: float
    rate: float
    returncode: int


def _parse_stats(output: str) -> dict[str, float]:
    """Extract values from the STATS line emitted by sim_publisher."""
    m = re.search(
        r"STATS\s+"
        r"published=(\d+)\s+"
        r"errors=(\d+)\s+"
        r"duration=([\d.]+)\s+"
        r"rate=([\d.]+)",
        output,
    )
    if not m:
        return {}
    return {
        "published": int(m.group(1)),
        "errors":    int(m.group(2)),
        "duration":  float(m.group(3)),
        "rate":      float(m.group(4)),
    }


# ---------------------------------------------------------------------------
# Worker launcher
# ---------------------------------------------------------------------------
def _build_worker_cmd(
    *,
    script: Path,
    broker: str,
    port: int,
    tls: bool,
    username: str | None,
    tenant: str,
    site: Path,
    num_tags: int,
    tag_offset: int,
    rate: float,
    duration: float,
    load_mode: bool,
    dry_run: bool,
    seed: int | None,
) -> list[str]:
    cmd = [
        sys.executable, str(script),
        "--tenant-id", tenant,
        "--site",      str(site),
        "--tags",      str(num_tags),
        "--tag-offset", str(tag_offset),
        "--rate",      str(rate),
        "--duration",  str(duration),
    ]
    if not dry_run:
        cmd += ["--broker", broker, "--port", str(port)]
        if tls:
            cmd.append("--tls")
        if username:
            cmd += ["--username", username]
    if load_mode:
        cmd.append("--load")
    if dry_run:
        cmd.append("--dry-run")
    if seed is not None:
        cmd += ["--seed", str(seed)]
    return cmd


def run_workers(
    *,
    n_processes: int,
    script: Path,
    broker: str,
    port: int,
    tls: bool,
    username: str | None,
    tenant: str,
    site: Path,
    total_tags: int,
    rate: float,
    duration: float,
    load_mode: bool,
    dry_run: bool,
    seed: int | None,
    verbose: bool,
) -> list[ProcessResult]:
    """Spawn n_processes workers, wait for all to finish, return results."""
    tags_per_proc = math.ceil(total_tags / n_processes)
    procs: list[tuple[subprocess.Popen, int, int]] = []

    print(
        f"[load_runner] Launching {n_processes} worker(s) — "
        f"{total_tags} tags @ {rate} Hz × {duration}s"
    )

    for i in range(n_processes):
        tag_offset = 1 + i * tags_per_proc
        # Last worker gets the remainder
        num_tags = min(tags_per_proc, total_tags - i * tags_per_proc)
        if num_tags <= 0:
            break

        # Give each worker a distinct seed if a base seed is provided
        worker_seed = (seed + i) if seed is not None else None

        cmd = _build_worker_cmd(
            script=script,
            broker=broker,
            port=port,
            tls=tls,
            username=username,
            tenant=tenant,
            site=site,
            num_tags=num_tags,
            tag_offset=tag_offset,
            rate=rate,
            duration=duration,
            load_mode=load_mode,
            dry_run=dry_run,
            seed=worker_seed,
        )
        if verbose:
            print(f"  [worker-{i}] {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        procs.append((proc, tag_offset, num_tags))
        print(f"  [worker-{i}] PID={proc.pid}  tags={tag_offset}–{tag_offset + num_tags - 1}")

    # Collect results
    results: list[ProcessResult] = []
    for proc, tag_offset, num_tags in procs:
        stdout, stderr = proc.communicate()
        stats = _parse_stats(stdout)
        if verbose and stderr.strip():
            for line in stderr.strip().splitlines():
                print(f"    stderr: {line}")
        results.append(ProcessResult(
            pid=proc.pid,
            tag_offset=tag_offset,
            num_tags=num_tags,
            published=int(stats.get("published", 0)),
            errors=int(stats.get("errors", 0)),
            duration=float(stats.get("duration", 0.0)),
            rate=float(stats.get("rate", 0.0)),
            returncode=proc.returncode,
        ))

    return results


# ---------------------------------------------------------------------------
# Reporter + exit-criteria check
# ---------------------------------------------------------------------------
def report(
    results: list[ProcessResult],
    target_rate: float,
    total_tags: int,
    duration: float,
) -> int:
    """Print aggregate stats and return exit code (0 = pass, 1 = fail)."""
    total_published = sum(r.published for r in results)
    total_errors    = sum(r.errors    for r in results)
    avg_duration    = sum(r.duration  for r in results) / max(len(results), 1)
    agg_rate        = sum(r.rate      for r in results)
    expected_events = int(total_tags * target_rate * duration)

    print("\n" + "=" * 60)
    print(f"  Load runner summary — {len(results)} worker(s)")
    print("=" * 60)
    print(f"  Total published : {total_published:,}")
    print(f"  Total errors    : {total_errors}")
    print(f"  Aggregate rate  : {agg_rate:.1f} msg/s")
    print(f"  Expected events : ~{expected_events:,}")
    print(f"  Avg duration    : {avg_duration:.1f}s")
    print("-" * 60)
    for r in results:
        status = "✓" if r.errors == 0 and r.returncode == 0 else "✗"
        print(
            f"  {status} PID={r.pid}  tags={r.tag_offset}–{r.tag_offset + r.num_tags - 1}"
            f"  published={r.published}  errors={r.errors}"
            f"  rate={r.rate:.1f} msg/s  rc={r.returncode}"
        )
    print("=" * 60)

    # Pass criteria
    target_agg = total_tags * target_rate
    threshold = target_agg * 0.95
    if total_errors > 0:
        print(f"  FAIL  {total_errors} dropped message(s)")
        return 1
    if agg_rate < threshold:
        print(f"  FAIL  aggregate rate {agg_rate:.1f} msg/s < threshold {threshold:.1f} msg/s "
              f"(target {target_agg:.1f} × 95%)")
        return 1
    print(f"  PASS  zero drops, rate {agg_rate:.1f} msg/s ≥ threshold {threshold:.1f} msg/s "
          f"(target {target_agg:.1f} × 95%)")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Flowterra load runner — multi-process sim_publisher orchestrator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    conn = p.add_argument_group("Connection (ignored with --dry-run)")
    conn.add_argument("--broker",   default="localhost")
    conn.add_argument("--port",     type=int, default=1883)
    conn.add_argument("--tls",      action="store_true")
    conn.add_argument("--username", default=None)

    ident = p.add_argument_group("Identity")
    ident_grp = ident.add_mutually_exclusive_group(required=True)
    ident_grp.add_argument("--tenant-id", dest="tenant")
    ident_grp.add_argument("--tenant")
    ident.add_argument("--site", required=True, type=Path)

    load = p.add_argument_group("Load")
    load.add_argument("--tags",      type=int,   default=100, dest="total_tags",
                      help="Total number of simulated tags across all workers")
    load.add_argument("--rate",      type=float, default=1.0,
                      help="Publish rate per tag in Hz")
    load.add_argument("--duration",  type=float, default=60.0,
                      help="Test duration in seconds")
    load.add_argument("--load",      action="store_true", dest="load_mode",
                      help="Enable load mode (disable stochastic movement)")
    load.add_argument("--dry-run",   action="store_true",
                      help="Skip MQTT; measure raw generation throughput")
    load.add_argument("--seed",      type=int,   default=None)

    scale = p.add_argument_group("Scaling")
    scale_grp = scale.add_mutually_exclusive_group()
    scale_grp.add_argument(
        "--processes", type=int, default=1,
        help="Number of parallel worker processes",
    )
    scale_grp.add_argument(
        "--auto-scale", action="store_true",
        help=f"Auto-determine process count (ceil(tags / {MAX_TAGS_PER_PROCESS}))",
    )

    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument(
        "--publisher",
        type=Path,
        default=Path(__file__).parent / "sim_publisher.py",
        help="Path to sim_publisher.py",
    )

    return p.parse_args()


def main() -> None:
    args = _parse_args()

    if not args.publisher.exists():
        print(f"ERROR: sim_publisher not found at {args.publisher}", file=sys.stderr)
        sys.exit(2)

    n_procs = (
        math.ceil(args.total_tags / MAX_TAGS_PER_PROCESS)
        if args.auto_scale
        else args.processes
    )
    n_procs = max(1, n_procs)

    t0 = time.monotonic()
    results = run_workers(
        n_processes=n_procs,
        script=args.publisher,
        broker=args.broker,
        port=args.port,
        tls=args.tls,
        username=args.username,
        tenant=args.tenant,
        site=args.site,
        total_tags=args.total_tags,
        rate=args.rate,
        duration=args.duration,
        load_mode=args.load_mode,
        dry_run=args.dry_run,
        seed=args.seed,
        verbose=args.verbose,
    )
    wall_time = time.monotonic() - t0
    print(f"\n[load_runner] Wall time: {wall_time:.1f}s")

    rc = report(
        results,
        target_rate=args.rate,
        total_tags=args.total_tags,
        duration=args.duration,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
