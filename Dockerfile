# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM golang:1.26-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w \
      -X github.com/thylong/bouine/internal/buildinfo.Version=${VERSION} \
      -X github.com/thylong/bouine/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/thylong/bouine/internal/buildinfo.Date=${DATE}" \
    -o /bouine ./cmd/bouine

# ---- Final stage ----
FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /bouine /bouine

USER nonroot:nonroot

EXPOSE 80 443 443/udp 9000 8443

ENTRYPOINT ["/bouine"]
CMD ["serve", "--config", "/etc/bouine/config.yaml"]
