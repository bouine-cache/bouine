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
# No USER reset: the scenario drivers mkdir and write into the /results
# bind mount, which is the host's repo checkout (uid 1001 on the CI
# runner). Running as root lets the container write there; k6 itself
# needs no privilege. Compose could instead set user: "1001:1001", but
# the image-local root keeps the compose file portable across runner
# UIDs.
