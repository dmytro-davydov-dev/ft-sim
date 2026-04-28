# ft-sim — Cloud Run Job image
# Connects to a deployed EMQX broker (dev or demo) and runs the BLE tag simulation.
#
# Build:
#   docker build -t gcr.io/flowterra-dev/ft-sim:latest ft-sim/
#
# Run locally (plain MQTT):
#   docker run --rm \
#     -e TENANT=acme -e BROKER=localhost -e PORT=1883 \
#     gcr.io/flowterra-dev/ft-sim:latest
#
# Deploy to Cloud Run Jobs (dev env):
#   gcloud run jobs create flowterra-ft-sim \
#     --image gcr.io/flowterra-dev/ft-sim:latest \
#     --region europe-west1 \
#     --set-env-vars BROKER=emqx.dev.flowterra.io,PORT=8883,TENANT=demo-tenant,TLS=true \
#     --set-secrets MQTT_PASSWORD=flowterra-sim-mqtt-password:latest \
#     --task-timeout 120s
#
# Execute a run:
#   gcloud run jobs execute flowterra-ft-sim --region europe-west1

FROM python:3.11-slim

WORKDIR /ft-sim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ── Environment variable defaults (override at deploy time or via --set-env-vars) ──
ENV BROKER=localhost
ENV PORT=1883
ENV TLS=false
ENV USERNAME=""
ENV TENANT=demo
ENV TAGS=100
ENV RATE=1.0
ENV DURATION=60
ENV SEED=""
# MQTT_PASSWORD — injected via Secret Manager at deploy time (do not hardcode)

ENTRYPOINT ["/bin/sh", "-c"]
CMD ["exec python sim_publisher.py \
  --broker  \"$BROKER\" \
  --port    \"$PORT\" \
  --tenant  \"$TENANT\" \
  --site    config/sample_site.json \
  --tags    \"$TAGS\" \
  --rate    \"$RATE\" \
  --duration \"$DURATION\" \
  ${TLS:+$([ \"$TLS\" = 'true' ] && echo '--tls')} \
  ${USERNAME:+--username \"$USERNAME\"} \
  ${SEED:+--seed \"$SEED\"}"]
