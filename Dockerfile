# syntax=docker/dockerfile:1

# ---- Build stage ----
# --platform=$BUILDPLATFORM: compile on the host CPU to avoid QEMU emulation.
# TARGETOS / TARGETARCH: populated automatically by BuildKit from --platform;
#   no defaults so the build fails loudly when called without a platform rather
#   than silently producing an amd64 binary on an arm64 host.
FROM --platform=$BUILDPLATFORM golang:1.27rc2-bookworm@sha256:32434419ff305ed58e7f3e60d37473bad789aca82122c327083a1a699e2ebf5d AS build

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
FROM gcr.io/distroless/static-debian13:nonroot@sha256:963fa6c544fe5ce420f1f54fb88b6fb01479f054c8056d0f74cc2c6000df5240

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
