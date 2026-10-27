FROM golang:1.26.10-alpine3.24 AS builder

RUN apk update
RUN apk add git

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN go build -o /app/mochi ./cmd/docker

FROM alpine:3.24.3

WORKDIR /
COPY --from=builder /app/mochi .

ENTRYPOINT [ "/mochi" ]
CMD ["/cmd/docker", "--config", "config.yaml"]