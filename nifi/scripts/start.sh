#!/bin/bash
# QuakeFlow NiFi entrypoint wrapper.
#
# Spawns the flow installer in the background (it polls the NiFi REST API
# until NiFi is up then idempotently creates the QuakeFlow process group
# with the 4 source pipelines) and then hands off to NiFi's official
# entrypoint so logs show up the way operators expect exactly.
set -e

(
  # first wait for NiFi REST API and install the flow.
  python3 /opt/quakeflow/install_flow.py >> /opt/nifi/nifi-current/logs/quakeflow_install.log 2>&1 || true
) &

exec /opt/nifi/scripts/start.sh
