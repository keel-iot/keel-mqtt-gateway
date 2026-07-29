-- Baseline schema for everything keel-mqtt-gateway reads or writes directly.
-- Idempotent: CREATE TABLE IF NOT EXISTS is safe to run against a database
-- where these tables already exist with this same shape (e.g. created by
-- another keel service before this repo owned its own migrations) — it
-- will not alter an existing table.
CREATE TABLE IF NOT EXISTS devices.tenants (
    id   UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS devices.devices (
    id        UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES devices.tenants (id),
    fleet_id  UUID,
    last_seen TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS devices.device_credentials (
    device_id  UUID NOT NULL REFERENCES devices.devices (id),
    type       TEXT NOT NULL,
    value      TEXT NOT NULL,
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_device_credentials_lookup
    ON devices.device_credentials (device_id, type, value);

CREATE TABLE IF NOT EXISTS devices.tenant_gateway_config (
    tenant_id             UUID PRIMARY KEY,
    password_auth_enabled BOOLEAN NOT NULL DEFAULT true,
    jwt_auth_enabled      BOOLEAN NOT NULL DEFAULT false,
    cert_auth_enabled     BOOLEAN NOT NULL DEFAULT false,
    jwt_public_key_pem    TEXT,
    trusted_ca_pems       TEXT[],
    auto_provisioning     BOOLEAN NOT NULL DEFAULT false,
    max_connections       INT,
    max_bytes_per_day     BIGINT,
    tracing_enabled       BOOLEAN NOT NULL DEFAULT false
);
