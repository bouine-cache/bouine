FROM golang:1.26-alpine AS build
WORKDIR /src
COPY main.go .
RUN go mod init test-origin && go build -o /test-origin .

FROM alpine:3.21
COPY --from=build /test-origin /test-origin
EXPOSE 8080
ENTRYPOINT ["/test-origin"]
