# Airflow image with the project's runtime deps. We extend the official
# image so that DAG parsing has access to requests / boto3 / elasticsearch /
# our `common` package without runtime pip installs.
FROM apache/airflow:2.10.3-python3.12

USER root
RUN apt-get update -y && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
