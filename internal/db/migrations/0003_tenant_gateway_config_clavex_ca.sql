-- Adds live device-CA resolution alongside the existing static
-- trusted_ca_pems: when clavex_ca_url is set, the CA is fetched and cached
-- in memory (see internal/auth/device_ca_cache.go), never persisted here —
-- clavex_agent_token is a scoped, revocable read-only credential for that
-- fetch, not the CA material itself.
ALTER TABLE devices.tenant_gateway_config
    ADD COLUMN IF NOT EXISTS clavex_ca_url TEXT,
    ADD COLUMN IF NOT EXISTS clavex_agent_token TEXT;
