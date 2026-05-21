# syntax=docker/dockerfile:1

# ---- Build stage ----
# Cross-compile on the host architecture to avoid QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64
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
      -X github.com/thylong/bouine/internal/buildinfo.Version=${VERSION} \
      -X github.com/thylong/bouine/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/thylong/bouine/internal/buildinfo.Date=${DATE}" \
    -o /bouine ./cmd/bouine

# ---- Final stage ----
FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /bouine /bouine
COPY config/default.yaml /etc/bouine/config.yaml

USER nonroot:nonroot

EXPOSE 80 443 443/udp 9000 8443

ENTRYPOINT ["/bouine"]
CMD ["serve", "--config", "/etc/bouine/config.yaml"]
