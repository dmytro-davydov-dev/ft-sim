# ft-sim

Flowterra BLE tag simulator. Publishes synthetic tag-sighting events to EMQX over MQTT, producing payloads identical to those a real Minew G1 BLE-to-MQTT gateway would send.

Used in place of physical hardware from Phase 3 through Phase 6. Physical gateways replace the simulator at Phase 7 (Pilot Cutover).

---

## Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Payload schema](#payload-schema)
- [Site config](#site-config)
- [Running the simulator](#running-the-simulator)
  - [Normal mode (local dev)](#normal-mode-local-dev)
  - [Load / stress-test mode](#load--stress-test-mode)
  - [Multi-process load runner](#multi-process-load-runner)
  - [Deployed EMQX (TLS + credentials)](#deployed-emqx-tls--credentials)
  - [Schema validation mode](#schema-validation-mode)
  - [Dry-run (no broker)](#dry-run-no-broker)
- [Synthetic dataset](#synthetic-dataset)
- [CLI reference — sim_publisher.py](#cli-reference--sim_publisherpy)
- [CLI reference — load_runner.py](#cli-reference--load_runnerpy)
- [Tests](#tests)
- [Deploy as Cloud Run Job](#deploy-as-cloud-run-job)
- [Files](#files)

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | ≥ 3.10 | 3.10 is the tested baseline; 3.11+ works |
| pip | any | used only to install deps |
| paho-mqtt | ≥ 2.0, < 3 | installed via `requirements.txt` |
| jsonschema | ≥ 4.0, < 5 | installed via `requirements.txt` |
| Mosquitto (optional) | any | local plain-MQTT broker for offline dev |
| pytest (test only) | any | `pip install pytest` |

**Install Mosquitto for local testing (macOS):**

```bash
brew install mosquitto
brew services start mosquitto
# broker is now listening on localhost:1883
```

**Install Mosquitto for local testing (Ubuntu/Debian):**

```bash
sudo apt-get install -y mosquitto mosquitto-clients
sudo systemctl start mosquitto
```

The default Mosquitto config allows anonymous connections on port 1883, which matches the simulator defaults.

---

## Installation

```bash
cd ft-sim/
pip install -r requirements.txt
```

---

## Payload schema

```
Topic : iot-ingress/{customerId}/{gatewayId}
QoS   : 1 (at-least-once)

{
  "customerId": "acme",            // string
  "gatewayId":  "gw-hq-01",       // string
  "tagId":      "tag-0042",        // string
  "rssi":       -72,               // integer dBm  [-120, 0]
  "zoneId":     "zone-reception",  // string
  "ts":         1745000000000,     // integer — ms epoch UTC
  "floor":      1,                 // integer ≥ 0
  "batteryPct": 85                 // integer [0, 100]
}
```

Formal schema: `schema/tag_event.json`

---

## Site config

The `--site` argument accepts a JSON file describing the physical space. See `config/sample_site.json` for the full format:

```json
{
  "siteId": "site-hq-01",
  "gatewayId": "gw-hq-01",
  "zones": [
    { "id": "zone-reception", "label": "Reception", "floor": 1, "ref_dist_m": 3.0 }
  ]
}
```

`ref_dist_m` is the typical tag-to-gateway distance in that zone; RSSI is derived from it using the log-distance path-loss model. The sample config ships with 7 zones across 2 floors.

---

## Running the simulator

### Normal mode (local dev)

Tags move stochastically between zones. Each tag fires events at `--rate` Hz.

```bash
python sim_publisher.py \
  --broker localhost --port 1883 \
  --tenant acme --site config/sample_site.json \
  --tags 100 --rate 1.0 --duration 60 --seed 42
```

`--seed` makes the run fully reproducible. Omit it for random movement each time.

Run forever (until Ctrl-C):

```bash
python sim_publisher.py \
  --broker localhost --port 1883 \
  --tenant acme --site config/sample_site.json \
  --tags 10 --rate 1.0 --duration 0
```

### Load / stress-test mode

`--load` disables stochastic zone movement and publishes at the target rate as fast as possible. Use this for throughput benchmarking.

```bash
python sim_publisher.py \
  --broker localhost --port 1883 \
  --tenant acme --site config/sample_site.json \
  --tags 1000 --rate 1.0 --duration 600 --load
```

### Multi-process load runner

A single asyncio process saturates around 500–600 events/s due to GIL overhead. For Phase 6 targets (1,000 tags × 1 Hz = 1,000 events/s) use `load_runner.py`, which shards the tag range across multiple `sim_publisher` worker processes and aggregates the results.

```bash
# 1,000 tags × 1 Hz, auto-scale to 2 workers, 60 s
python load_runner.py \
  --tenant-id acme --site config/sample_site.json \
  --tags 1000 --rate 1.0 --duration 60 --load --auto-scale

# Explicit 2 workers, against a live broker
python load_runner.py \
  --broker localhost --port 1883 \
  --tenant-id acme --site config/sample_site.json \
  --tags 1000 --rate 1.0 --duration 60 --load --processes 2

# Dry-run (no broker needed) — measure raw generation throughput
python load_runner.py \
  --tenant-id acme --site config/sample_site.json \
  --tags 1000 --rate 1.0 --duration 60 --load --dry-run --auto-scale
```

`load_runner.py` exits **0** (pass) when zero messages are dropped and the aggregate rate is ≥ 95% of target; **1** on drops or under-rate; **2** on config error.

### Deployed EMQX (TLS + credentials)

```bash
export MQTT_PASSWORD=<secret>

python sim_publisher.py \
  --broker emqx.dev.flowterra.io --port 8883 --tls \
  --username sim-user \
  --tenant acme --site config/sample_site.json \
  --tags 100 --rate 1.0 --duration 60
```

Pass `--password` on the command line only in non-production contexts; prefer the `MQTT_PASSWORD` environment variable.

### Schema validation mode

Validates every payload against `schema/tag_event.json` before publishing. Adds overhead; useful in CI or when changing the payload shape.

```bash
python sim_publisher.py \
  --broker localhost --port 1883 \
  --tenant acme --site config/sample_site.json \
  --tags 10 --rate 1.0 --duration 10 --validate
```

### Dry-run (no broker)

Generates events without connecting to MQTT. Useful for benchmarking event generation throughput or CI environments without a broker.

```bash
python sim_publisher.py \
  --tenant acme --site config/sample_site.json \
  --tags 1000 --rate 10.0 --duration 10 --load --dry-run
```

---

## Synthetic dataset

`data/synthetic_events.jsonl` is a pre-generated deterministic dataset of ≥ 10,000 tag events simulating a 3-hour workday (09:00–12:00 UTC). It is used by the test suite (regression gate for Phase 4) and can be loaded into BigQuery for integration testing without a live broker.

**Regenerate the dataset** (deterministic, seed 42):

```bash
python generate_synthetic.py
# → writes data/synthetic_events.jsonl
# → prints event count and file size
```

**Regenerate with schema validation** (slower, recommended before committing):

```bash
python generate_synthetic.py --validate
```

**Custom output path or seed:**

```bash
python generate_synthetic.py --out /tmp/events.jsonl --seed 99
```

**Load into BigQuery** (dev environment):

```bash
bq load \
  --source_format=NEWLINE_DELIMITED_JSON \
  --autodetect \
  flowterra-dev:flowterra_raw.tag_events \
  data/synthetic_events.jsonl
```

The file is sorted by `ts` (ascending) and all events conform to `schema/tag_event.json`.

---

## CLI reference — sim_publisher.py

```
Connection:
  --broker          MQTT broker host               (default: localhost)
  --port            MQTT port                      (default: 1883; use 8883 with --tls)
  --tls             Enable TLS/SSL
  --username        MQTT username
  --password        MQTT password (prefer env var MQTT_PASSWORD)

Identity:
  --tenant-id       customerId injected into every event  [required]
  --tenant          Alias for --tenant-id (backward compatible)
  --site            Path to site config JSON              [required]

Load:
  --tags            Number of simulated tags         (default: 10)
  --tag-offset      First tag number                 (default: 1)
                    Use for multi-process partitioning (load_runner sets this automatically)
  --rate            Publish rate Hz per tag           (default: 1.0)
  --duration        Run seconds (0 = run forever)    (default: 60)
  --load            Stress-test mode — max rate, no zone movement
  --dry-run         Generate events without publishing to MQTT
  --seed            RNG seed for reproducible runs

Physics:
  --path-loss-exp   Log-distance exponent n           (default: 2.5)
                    2.0 = free-space, 3–4 = indoor
  --tx-power        RSSI at 1 m reference distance    (default: -40 dBm)
  --rssi-std        Gaussian RSSI noise std dev        (default: 15 dBm)
  --mean-dwell      Mean dwell time per zone           (default: 15 s)

Validation:
  --validate        Validate each payload against schema/tag_event.json (slower)
```

---

## CLI reference — load_runner.py

```
Connection (ignored with --dry-run):
  --broker          MQTT broker host               (default: localhost)
  --port            MQTT port                      (default: 1883)
  --tls             Enable TLS/SSL
  --username        MQTT username

Identity:
  --tenant-id       customerId  [required]
  --tenant          Alias for --tenant-id
  --site            Path to site config JSON  [required]

Load:
  --tags            Total simulated tags across all workers  (default: 100)
  --rate            Publish rate per tag in Hz               (default: 1.0)
  --duration        Test duration in seconds                 (default: 60)
  --load            Enable load mode (disable stochastic movement)
  --dry-run         Skip MQTT; measure raw generation throughput
  --seed            Base RNG seed (each worker gets seed + worker_index)

Scaling:
  --processes N     Number of parallel workers               (default: 1)
  --auto-scale      Auto-determine count: ceil(tags / 500)
                    (mutually exclusive with --processes)

Other:
  --verbose / -v    Print each worker command and stderr output
  --publisher PATH  Path to sim_publisher.py (default: sibling file)
```

**Pass criteria (exit 0):**
- Zero dropped messages across all workers
- Aggregate rate ≥ 95% of `total_tags × rate`

---

## Tests

Install pytest if you haven't already:

```bash
pip install pytest
```

Run the full test suite:

```bash
cd ft-sim/
pytest tests/ -v
```

**`tests/test_schema.py`** covers:

- `schema/tag_event.json` and `config/sample_site.json` file existence
- Single payload JSON Schema conformance
- All required fields present on every payload
- Type + range assertions: `rssi` ∈ [-120, 0], `batteryPct` ∈ [0, 100], `ts` is ms-epoch int, `zoneId` non-empty string, `floor` ≥ 0
- Physics sanity: RSSI weakens with distance
- 100-tag × 3-tick full Schema validation (Phase 3 exit criterion gate)
- Load-mode payloads remain schema-valid under max-rate emission
- Battery drains monotonically over time
- `data/synthetic_events.jsonl` — existence, ≥ 10,000 events, all-valid schema, required fields spot-check, zone IDs match site config, timestamps monotonically non-decreasing

**`tests/test_load_mode.py`** covers throughput and multi-process partitioning behaviour.

All tests passing is the Phase 3 exit criterion.

---

## Deploy as Cloud Run Job

Build and push the image:

```bash
docker build -t gcr.io/flowterra-dev/ft-sim:latest ft-sim/
docker push gcr.io/flowterra-dev/ft-sim:latest
```

Create the Cloud Run Job (one-time):

```bash
gcloud run jobs create flowterra-ft-sim \
  --image gcr.io/flowterra-dev/ft-sim:latest \
  --region europe-west1 \
  --set-env-vars BROKER=emqx.dev.flowterra.io,PORT=8883,TENANT=demo-tenant,TLS=true,TAGS=100,RATE=1.0,DURATION=60 \
  --set-secrets MQTT_PASSWORD=flowterra-sim-mqtt-password:latest \
  --task-timeout 120s
```

Execute a run against the deployed EMQX:

```bash
gcloud run jobs execute flowterra-ft-sim --region europe-west1
```

Override parameters for a one-off load test:

```bash
gcloud run jobs execute flowterra-ft-sim \
  --region europe-west1 \
  --update-env-vars TAGS=1000,RATE=1.0,DURATION=600
```

---

## Files

```
ft-sim/
├── sim_publisher.py          CLI entry point — single-process simulator
├── load_runner.py            Multi-process load test orchestrator
├── generate_synthetic.py     Deterministic JSONL dataset generator
├── requirements.txt          paho-mqtt + jsonschema
├── Dockerfile                Cloud Run Job image
├── README.md                 this file
├── schema/
│   └── tag_event.json        Canonical payload JSON Schema
├── config/
│   └── sample_site.json      7-zone sample site (2 floors)
├── data/
│   └── synthetic_events.jsonl  Pre-generated 3-hour dataset (≥10,000 events)
└── tests/
    ├── test_schema.py        pytest schema + physics conformance suite
    └── test_load_mode.py     pytest load mode + throughput suite
```
