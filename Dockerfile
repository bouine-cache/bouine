FROM golang:1.17 as builder

WORKDIR /usr/src/app

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/app ./api

FROM gcr.io/distroless/static-debian11:nonroot

COPY --from=builder /go/bin/app /
EXPOSE 8080/tcp
CMD ["/app"]
