# Python service for one-shot historical pull and archive
# replay. Bringing this up via the `backfill` profile is the only way to
# load 30 days of data into Postgres on a fresh clone.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY docker/backfill/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# We copy the entire src/ tree so that backfill can `import common.*`
# the same way Airflow DAGs do.
COPY src /app/src

# When the container is invoked via `docker compose up backfill`, MODE
# is read from the environment (historical | replay).
CMD ["python", "-m", "backfill.ingest"]
