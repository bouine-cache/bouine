# syntax=docker/dockerfile:1

# ---- Build stage ----
# --platform=$BUILDPLATFORM: compile on the host CPU to avoid QEMU emulation.
# TARGETOS / TARGETARCH: populated automatically by BuildKit from --platform;
#   no defaults so the build fails loudly when called without a platform rather
#   than silently producing an amd64 binary on an arm64 host.
FROM --platform=$BUILDPLATFORM golang:1.26.5-bookworm@sha256:18aedc16aa19b3fd7ded7245fc14b109e054d65d22ed53c355c899582bbb2113 AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
    -ldflags "-s -w \
      -X github.com/bouine-cache/bouine/internal/buildinfo.Version=${VERSION} \
      -X github.com/bouine-cache/bouine/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/bouine-cache/bouine/internal/buildinfo.Date=${DATE}" \
    -o /bouine ./cmd/bouine

# ---- Final stage ----
# Pinned digest ensures local and CI builds use the exact same base layer.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:d29e660cc75a5b6b1334e03c5c81ccf9bc0884a002c6000dbf0fb96034814478

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
