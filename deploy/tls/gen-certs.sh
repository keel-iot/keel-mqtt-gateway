#!/usr/bin/env bash
# Generates a self-signed cert/key pair for docker-compose.tls.yml's local
# TLS validation. NOT for production use — real deployments get a cert from
# cert-manager/a real CA, mounted the same way (tls.crt + tls.key in a
# directory), which is exactly what makes this a valid stand-in for testing
# the reload path.
set -euo pipefail

cert_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
mkdir -p "$cert_dir"

days="${1:-1}"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout "$cert_dir/tls.key" -out "$cert_dir/tls.crt" \
  -days "$days" -nodes \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# world-readable: the gateway container runs as a non-root user whose uid
# won't match the host user that ran this script. A real K8s Secret volume
# is world-readable by default for the same reason, so this matches
# production rather than weakening this dev fixture.
chmod 644 "$cert_dir/tls.key" "$cert_dir/tls.crt"

echo "wrote $cert_dir/tls.crt (valid ${days}d) and $cert_dir/tls.key"
echo "re-run this script (optionally with a different <days> argument) to rotate the cert without restarting the gateway container"
