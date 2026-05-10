# Tiny init container that PUTs the Elasticsearch mapping for the `quakes`
# index. Idempotent: skipped if the index already exists.
FROM curlimages/curl:8.10.1

USER root
COPY docker/es-init/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
