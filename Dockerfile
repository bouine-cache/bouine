# syntax=docker/dockerfile:1

# ---- Build stage ----
# --platform=$BUILDPLATFORM: compile on the host CPU to avoid QEMU emulation.
# TARGETOS / TARGETARCH: populated automatically by BuildKit from --platform;
#   no defaults so the build fails loudly when called without a platform rather
#   than silently producing an amd64 binary on an arm64 host.
FROM --platform=$BUILDPLATFORM golang:1.27.1-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
# For reproducible builds, pass --build-arg SOURCE_DATE_EPOCH=<timestamp>
# and --build-arg DATE=<derived ISO timestamp>. When SOURCE_DATE_EPOCH
# is set and DATE is not, the build derives a stable timestamp from it.
ARG SOURCE_DATE_EPOCH=
ARG DATE=unknown
# Set STRIPFLAGS to empty to preserve debug symbols in the image.
ARG STRIPFLAGS=-s -w

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

# Derive DATE from SOURCE_DATE_EPOCH if DATE was not explicitly set.
RUN if [ -n "${SOURCE_DATE_EPOCH}" ] && [ "${DATE}" = "unknown" ]; then \
      DATE=$(date -u -d "@${SOURCE_DATE_EPOCH}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r ${SOURCE_DATE_EPOCH} +%Y-%m-%dT%H:%M:%SZ); \
      echo "DATE=$DATE" > /tmp/build_date.env; \
    else \
      echo "DATE=${DATE}" > /tmp/build_date.env; \
    fi

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
    -ldflags "${STRIPFLAGS} \
      -X github.com/bouine-cache/bouine/internal/buildinfo.Version=${VERSION} \
      -X github.com/bouine-cache/bouine/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/bouine-cache/bouine/internal/buildinfo.Date=$(cat /tmp/build_date.env | cut -d= -f2)" \
    -o /bouine ./cmd/bouine

# ---- Final stage ----
# Pinned digest ensures local and CI builds use the exact same base layer.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:f7f8f729987ad0fdf6b05eeeae94b26e6a0f613bdf46feea7fc40f7bd72953e6

COPY --from=build /bouine /bouine
COPY config/default.yaml /etc/bouine/config.yaml

USER nonroot:nonroot

# 80/tcp   — HTTP/1.1 + h2c data plane
# 443/tcp  — HTTPS/1.1 + HTTP/2 data plane
# 443/udp  — HTTP/3 (QUIC) data plane
# 9000/tcp — admin API (metrics, purge, dashboard)
EXPOSE 80 443 443/udp 9000

ENTRYPOINT ["/bouine"]
CMD ["serve", "--config", "/etc/bouine/config.yaml"]
