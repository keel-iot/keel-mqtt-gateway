FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY vendor-pkg/ ./vendor-pkg/
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /keel-mqtt-cluster \
    ./cmd/server

# ── Runtime image ─────────────────────────────────────────────────────────────
# Alpine (not distroless) so docker-compose can `docker exec ... drain` and so
# the raft data dir volume mount is writable by a non-root user we control.
FROM alpine:3.20
RUN addgroup -S keel && adduser -S -G keel keel \
    && mkdir -p /data/raft && chown -R keel:keel /data
COPY --from=builder /keel-mqtt-cluster /usr/local/bin/keel-mqtt-cluster
USER keel
EXPOSE 1883 8883 8085 9090 7000 7100 7946 8090
ENTRYPOINT ["keel-mqtt-cluster"]
