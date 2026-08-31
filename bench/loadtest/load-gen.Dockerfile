# The load-test load-gen image: the pinned grafana/k6 image plus bash.
# The scenario drivers (bench/loadtest/scenarios/*/run.sh) use bash
# features (indirect expansion ${!ADDR}, uppercase ${TUT^^}), but the
# stock k6 image is Alpine with only sh — the compose entrypoint is
# ["sh"] and `docker compose run load-gen bash /scenarios/...` fails
# with "sh: can't open 'bash'". Derived here so the k6 version stays
# pinned by digest in docker-compose.yaml and only bash is added.
FROM grafana/k6:0.54.0@sha256:1f40432b1cbe7234e977f96c362c9bc550a2d2b583d014dd8669fe40d3e9e755
USER root
RUN apk add --no-cache bash
USER 12345
