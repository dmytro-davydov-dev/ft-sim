# ft-sim

Flowterra BLE tag simulator. Publishes synthetic tag-sighting events to EMQX over MQTT, producing payloads identical to those a real Minew G1 BLE-to-MQTT gateway would send.

Used in place of physical hardware from Phase 3 through Phase 6. Physical gateways replace the simulator at Phase 7 (Pilot Cutover).

## Payload schema

```
Topic : iot-ingress/{customerId}/{gatewayId}
QoS   : 1 (at-least-once)

{
  "customerId": "acme",          // string
  "gatewayId":  "gw-hq-01",     // string
  "tagId":      "tag-0042",      // string
  "rssi":       -72,             // integer dBm
  "zoneId":     "zone-reception",// string
  "ts":         1745000000000,   // integer — ms epoch UTC
  "floor":      1,               // integer
  "batteryPct": 85               // integer 0-100
}
```

Formal schema: `ft-sim/schema/tag_event.json`

## Setup

```bash
cd ft-sim/
pip install -r requirements.txt
```

## Run — local dev (plain MQTT)

```bash
python sim_publisher.py \
  --broker localhost --port 1883 \
  --tenant acme --site config/sample_site.json \
  --tags 100 --rate 1.0 --duration 60 --seed 42
```

## Run — deployed EMQX (TLS + credentials)

```bash
export MQTT_PASSWORD=<secret>

python sim_publisher.py \
  --broker emqx.dev.flowterra.io --port 8883 --tls \
  --username sim-user \
  --tenant acme --site config/sample_site.json \
  --tags 100 --rate 1.0 --duration 60
```

## Run — load / stress test (Phase 6)

Disables stochastic zone movement and publishes as fast as possible:

```bash
python sim_publisher.py \
  --broker localhost --port 1883 \
  --tenant acme --site config/sample_site.json \
  --tags 1000 --rate 1.0 --duration 600 --load
```

## Run — with schema validation

Validates every payload against `schema/tag_event.json` before publishing (adds overhead, useful in CI):

```bash
python sim_publisher.py \
  --broker localhost --port 1883 \
  --tenant acme --site config/sample_site.json \
  --tags 10 --rate 1.0 --duration 10 --validate
```

## CLI reference

```
Connection:
  --broker      MQTT broker host          (default: localhost)
  --port        MQTT port                 (default: 1883; use 8883 with --tls)
  --tls         Enable TLS/SSL
  --username    MQTT username
  --password    MQTT password (prefer env var MQTT_PASSWORD)

Identity:
  --tenant      customerId  [required]
  --site        Path to site config JSON  [required]

Load:
  --tags        Number of simulated tags  (default: 10)
  --rate        Publish rate Hz/tag       (default: 1.0)
  --duration    Run seconds (0=forever)   (default: 60)
  --load        Stress-test mode — max rate, no zone movement
  --seed        RNG seed for reproducibility

Physics:
  --path-loss-exp  Log-distance exponent n  (default: 2.5)
  --tx-power       RSSI at 1 m, dBm         (default: -40)
  --rssi-std       Gaussian noise σ dBm      (default: 15)
  --mean-dwell     Avg zone dwell time s     (default: 15)

  --validate    Validate each payload against schema/tag_event.json
```

## Tests

```bash
pip install pytest
pytest tests/
```

All tests pass → Phase 3 exit criterion met.

## Deploy as Cloud Run Job (dev / demo environments)

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

## Site config

The `--site` argument accepts a JSON file. See `config/sample_site.json` for the full format:

```json
{
  "siteId": "site-hq-01",
  "gatewayId": "gw-hq-01",
  "zones": [
    { "id": "zone-reception", "label": "Reception", "floor": 1, "ref_dist_m": 3.0 }
  ]
}
```

`ref_dist_m` is the typical tag-to-gateway distance in that zone; the RSSI is derived from it using the log-distance path-loss model.

## Files

```
ft-sim/
├── sim_publisher.py          CLI entry point
├── requirements.txt          paho-mqtt + jsonschema
├── Dockerfile                Cloud Run Job image
├── README.md                 this file
├── schema/
│   └── tag_event.json        canonical payload JSON Schema
├── config/
│   └── sample_site.json      7-zone sample site (2 floors)
└── tests/
    └── test_schema.py        pytest schema conformance tests
```
