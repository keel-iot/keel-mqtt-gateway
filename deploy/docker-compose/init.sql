-- Minimal schema for the docker-compose PoC: only what mqtt-gateway's
-- TenantConfigCache queries. AUTH_BACKEND=file bypasses devices.devices /
-- devices.device_credentials entirely (see internal/auth/file_provider.go),
-- so those tables are intentionally not created here.
CREATE SCHEMA IF NOT EXISTS devices;

CREATE TABLE IF NOT EXISTS devices.tenant_gateway_config (
    tenant_id              UUID PRIMARY KEY,
    password_auth_enabled  BOOLEAN NOT NULL DEFAULT true,
    jwt_auth_enabled       BOOLEAN NOT NULL DEFAULT false,
    cert_auth_enabled      BOOLEAN NOT NULL DEFAULT false,
    jwt_public_key_pem     TEXT,
    trusted_ca_pems        TEXT[],
    auto_provisioning      BOOLEAN NOT NULL DEFAULT false,
    max_connections        INT,
    max_bytes_per_day      BIGINT,
    tracing_enabled        BOOLEAN NOT NULL DEFAULT false
);
