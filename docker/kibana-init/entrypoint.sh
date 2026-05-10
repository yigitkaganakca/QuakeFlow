#!/bin/sh
# Wait for Kibana to be 'available' AND for the saved_objects API to
# actually accept POSTs (status:available is reported a few seconds before
# the API is wired up), then import the dashboards bundle.
set -eu

KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"
EXPORT_FILE="${EXPORT_FILE:-/exports/dashboards.ndjson}"

if [ ! -s "$EXPORT_FILE" ]; then
  echo "[kibana-init] no export file at $EXPORT_FILE - nothing to import; exiting OK."
  exit 0
fi

echo "[kibana-init] waiting for Kibana at $KIBANA_URL ..."
for i in $(seq 1 120); do
  status=$(curl -s "$KIBANA_URL/api/status" 2>/dev/null | tr -d '\n' | grep -o '"level":"[a-z]*"' | head -n1 || true)
  if echo "$status" | grep -q "available"; then
    echo "[kibana-init] Kibana status reports 'available'."
    break
  fi
  sleep 4
done

# The saved_objects API can return 503 for ~10s after the status endpoint
# reports 'available'. Retry the import a few times.
echo "[kibana-init] POST /api/saved_objects/_import (overwrite=true)"
attempts=10
i=0
while [ "$i" -lt "$attempts" ]; do
  i=$((i + 1))
  out=$(curl -s -w "\n%{http_code}" -X POST \
        "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
        -H "kbn-xsrf: true" \
        --form file=@"$EXPORT_FILE" 2>&1) || true
  code=$(echo "$out" | tail -n1)
  body=$(echo "$out" | sed '$d')
  if [ "$code" = "200" ]; then
    echo "$body"
    echo "[kibana-init] import OK (attempt $i)."
    exit 0
  fi
  echo "[kibana-init] attempt $i: HTTP $code; retrying in 5s ..."
  sleep 5
done

echo "[kibana-init] FAILED to import saved objects after $attempts attempts."
echo "[kibana-init] last response: $body"
exit 1
