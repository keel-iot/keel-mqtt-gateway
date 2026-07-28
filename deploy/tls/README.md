# TLS listener — manual validation

Validates Phase 1: TLS termination in mochi-mqtt, live cert reload, and
readiness gating when the cert is missing/invalid.

## Setup

```
./deploy/tls/gen-certs.sh          # writes deploy/tls/certs/tls.{crt,key}, valid 1 day
docker compose -f docker-compose.tls.yml up -d --build
```

## 1. Readiness before a cert exists

Bring the cert dir up empty first to see the gate work:

```
rm -f deploy/tls/certs/tls.{crt,key}
docker compose -f docker-compose.tls.yml up -d --build
curl -si http://localhost:19090/readyz    # 503 — tls: certificate not ready
curl -si http://localhost:19090/healthz   # 200 — liveness is unaffected
```

## 2. Cert appears → Ready

```
./deploy/tls/gen-certs.sh
sleep 1                                    # fsnotify debounce (200ms) + a little slack
curl -si http://localhost:19090/readyz    # 200 — ok
```

## 3. Connect over TLS

```
mosquitto_pub -h localhost -p 18883 --cafile deploy/tls/certs/tls.crt \
  -i "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" \
  -u "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa@11111111-1111-1111-1111-111111111111" \
  -P "testpass123" \
  -t "t/self-test" -m "hello over tls"
```

(self-signed cert with CN=localhost/SAN localhost+127.0.0.1 — `--cafile` pins
it directly since there's no real CA to trust)

## 4. Rotate the cert without restarting the container

```
./deploy/tls/gen-certs.sh 30              # new keypair, valid 30 days
docker compose -f docker-compose.tls.yml logs gateway --tail 5
# "tls cert reloader: certificate loaded" with the new not_after date
```

Re-run the `mosquitto_pub` command from step 3 — new connections now
negotiate with the rotated cert. Confirm with:

```
echo | openssl s_client -connect localhost:18883 -servername localhost 2>/dev/null \
  | openssl x509 -noout -enddate
```

## 5. Expired cert → NotReady

Not practical to reproduce manually here (openssl's `req -x509` won't
backdate `notAfter` into the past via `-days`). Covered instead by
`internal/broker/cert_reloader_test.go`'s
`TestCertReloader_NotReadyWhenCertExpired`, which builds a cert with an
explicit past `NotAfter` directly and asserts `Ready() == false`.

## Caveat: bind-mount inotify propagation

The CertReloader watches the mounted directory via fsnotify (inotify under
Linux). This works reliably on native Linux Docker (including this WSL2
setup) and in real Kubernetes Secret volumes. Docker Desktop's non-Linux
bind-mount backends (gRPC-FUSE/VirtioFS on macOS) are known to propagate
inotify events unreliably for host bind mounts — if step 2/4 above don't
pick up a change within a few seconds on such a setup, restart the container
once to confirm the code path itself is correct, then treat it as an
environment limitation, not a CertReloader bug.
