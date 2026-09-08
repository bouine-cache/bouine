# The origin used by the loadtest compose stack and `make loadtest-setup`.
# This file was removed with the in-process integration rewrite (d8ffa29)
# while bench/loadtest/docker-compose.yaml and the Makefile targets kept
# building it — the nightly load-test runner has failed before reaching
# that point since, so nothing noticed. Restored here; keep in sync with
# go.mod's toolchain when bumping.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY main.go .
# main.go imports fasthttp (net/http was eliminated repo-wide); the bare
# `go mod init` of the original file no longer compiles without fetching
# the dependency.
RUN go mod init test-origin  && go get github.com/valyala/fasthttp@v1.73.0  && go mod tidy  && go build -o /test-origin .

FROM alpine:3.22
COPY --from=build /test-origin /test-origin
EXPOSE 8080
ENTRYPOINT ["/test-origin"]
