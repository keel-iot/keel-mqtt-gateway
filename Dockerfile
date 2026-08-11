FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY vendor-pkg/ ./vendor-pkg/
# thirdparty/mochi-mqtt-server is a local-path `replace` target (see
# go.mod / thirdparty/mochi-mqtt-server/PATCH.md) — go mod download
# reads its go.mod to resolve the replacement, so it must exist before
# this step, same as vendor-pkg above. Copied separately from the full
# `COPY . .` below so unrelated source changes don't bust this layer's
# cache.
COPY thirdparty/ ./thirdparty/
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /keel-mqtt-gateway \
    ./cmd/server

# ── Runtime image ─────────────────────────────────────────────────────────────
# Alpine (not distroless) so docker-compose can `docker exec ... drain` and so
# the raft data dir volume mount is writable by a non-root user we control.
FROM alpine:3.20
RUN addgroup -S keel && adduser -S -G keel keel \
    && mkdir -p /data/raft && chown -R keel:keel /data
COPY --from=builder /keel-mqtt-gateway /usr/local/bin/keel-mqtt-gateway
USER keel
EXPOSE 1883 8883 8085 9090 7000 7100 7946 8090
ENTRYPOINT ["keel-mqtt-gateway"]
