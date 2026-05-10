# Init container that imports Kibana saved objects (dashboards, visualizations,
# data view) once Kibana is up. Idempotent (uses overwrite=true).
FROM curlimages/curl:8.10.1

USER root
COPY docker/kibana-init/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
