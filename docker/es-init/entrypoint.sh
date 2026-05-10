#!/bin/sh
# Apply the mapping for the `quakes` index. Idempotent.
set -eu

ES_URL="${ES_URL:-http://elasticsearch:9200}"
ES_INDEX="${ES_INDEX:-quakes}"
MAPPING_FILE="${MAPPING_FILE:-/mappings/fact_earthquakes.json}"

echo "[es-init] waiting for $ES_URL ..."
for i in $(seq 1 60); do
  if curl -sf "$ES_URL/_cluster/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if curl -sf "$ES_URL/$ES_INDEX" >/dev/null 2>&1; then
  echo "[es-init] index '$ES_INDEX' already exists, skipping create"
else
  echo "[es-init] creating index '$ES_INDEX' from $MAPPING_FILE"
  curl -sf -X PUT "$ES_URL/$ES_INDEX" \
    -H "Content-Type: application/json" \
    --data-binary "@$MAPPING_FILE" \
  || { echo "[es-init] FAILED to create index"; exit 1; }
  echo
fi

echo "[es-init] done."
