# this is the test runner image. Mirrors the backfill image's Python toolchain plus
# pytest + a couple of dev dependencies. Run via:
#   docker compose --profile tests up --build tests
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src:/app/dags

WORKDIR /app

COPY docker/tests/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src  /app/src
COPY dags /app/dags
COPY data /app/data

# defaul is run the entire test suite, fail fast.
CMD ["pytest", "-q", "--maxfail=1", "src/tests"]
