-- Adds JWKS support alongside the existing static jwt_public_key_pem:
-- when set, device JWTs are verified by resolving "kid" against this URL
-- instead of a single static key (see internal/auth/jwks_cache.go).
ALTER TABLE devices.tenant_gateway_config
    ADD COLUMN IF NOT EXISTS jwks_url TEXT;
