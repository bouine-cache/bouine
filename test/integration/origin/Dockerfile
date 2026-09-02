FROM golang:1.27.0-alpine AS build
WORKDIR /src
COPY main.go .
RUN go mod init test-origin && \
    go get github.com/valyala/fasthttp@v1.73.0 && \
    go build -o /test-origin .

FROM alpine:3.21
COPY --from=build /test-origin /test-origin
EXPOSE 8080
ENTRYPOINT ["/test-origin"]
