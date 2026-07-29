package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// ErrAuthNotConfigured is returned when a tenant has JWT auth enabled but
// neither JWKSURL nor JWTPublicKeyPEM is set — there is no key source to
// verify against.
var ErrAuthNotConfigured = errors.New("jwt: no key source configured for tenant")

// ErrInvalidKid is returned when a JWKS-configured tenant's token carries a
// "kid" that could not be resolved to a key — unknown kid, or the JWKS
// refresh itself failed with no cached fallback available.
var ErrInvalidKid = errors.New("jwt: kid not found in JWKS")

// AuthMethod distinguishes how a device presented its credentials.
type AuthMethod string

const (
	AuthMethodPassword    AuthMethod = "password"
	AuthMethodJWT         AuthMethod = "jwt"
	AuthMethodCertificate AuthMethod = "certificate"
)

// DetectAuthMethod returns the authentication method implied by the MQTT password.
// JWTs always start with "eyJ" (base64url of `{"`, the start of the JWT header).
func DetectAuthMethod(password []byte) AuthMethod {
	if len(password) > 4 && string(password[:3]) == "eyJ" {
		return AuthMethodJWT
	}
	return AuthMethodPassword
}

// ValidateJWT verifies a device-signed JWT against the tenant's configured
// key source — JWKS (per-kid, when cfg.JWKSURL is set) or a single static
// PEM key (cfg.JWTPublicKeyPEM) otherwise. Exactly one of the two must be
// configured; jwks may be nil only when cfg.JWKSURL is empty.
//
// Expected claims:
//   - sub: <deviceID>
//   - aud: "keel-gateway"
//   - iss: <deviceID>@<tenantID>  (optional, for extra binding)
//   - exp: required, must be in the future
//   - tid: <tenantID>  (custom claim)
//
// Supported algorithms: RS256/384/512 (RSA) and ES256/384/512 (EC).
// Returns nil on success, a descriptive error on failure.
func ValidateJWT(ctx context.Context, tenantID, deviceID string, tokenBytes []byte, cfg *TenantGatewayConfig, jwks *JWKSCache) error {
	keyFunc, err := jwtKeyFunc(ctx, tenantID, cfg, jwks)
	if err != nil {
		return err
	}
	return validateJWTClaims(tokenBytes, keyFunc, tenantID, deviceID, true)
}

// VerifyCertificate parses the device/tenant identity from the certificate
// CommonName and, when trustedCAPEMs is non-nil, verifies the certificate chain.
//
// CN convention: "<deviceID>@<tenantID>".
//
//   - trustedCAPEMs == nil  → CN parsing only, chain verification skipped.
//     Use this for a quick tenant-lookup before you have the CA list.
//   - trustedCAPEMs non-nil → full chain verification against the provided PEMs.
func VerifyCertificate(cert *x509.Certificate, trustedCAPEMs []string) (deviceID, tenantID string, err error) {
	cn := cert.Subject.CommonName
	parts := strings.SplitN(cn, "@", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("cert: CN %q is not in <deviceID>@<tenantID> format", cn)
	}
	deviceID, tenantID = parts[0], parts[1]

	if trustedCAPEMs == nil {
		// Caller only needs the identity from the CN — skip chain verification.
		return deviceID, tenantID, nil
	}
	if len(trustedCAPEMs) == 0 {
		return "", "", errors.New("cert: no trusted CAs configured for tenant")
	}

	pool := x509.NewCertPool()
	for _, caPEM := range trustedCAPEMs {
		if !pool.AppendCertsFromPEM([]byte(caPEM)) {
			return "", "", errors.New("cert: failed to parse a trusted CA PEM")
		}
	}

	opts := x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if _, err := cert.Verify(opts); err != nil {
		return "", "", fmt.Errorf("cert: verification failed: %w", err)
	}

	return deviceID, tenantID, nil
}

// ParseClientIdentifier parses the Google IoT Core-style MQTT client-id
// "tenants/<tenantID>/devices/<deviceID>" and returns the components.
// Returns ok=false for any other format so callers can fall back to the
// standard "<deviceID>@<tenantID>" username parsing.
func ParseClientIdentifier(clientID string) (tenantID, deviceID string, ok bool) {
	// Expected: "tenants/<tenantID>/devices/<deviceID>"
	parts := strings.SplitN(clientID, "/", 4)
	if len(parts) != 4 || parts[0] != "tenants" || parts[2] != "devices" {
		return "", "", false
	}
	tenantID = parts[1]
	deviceID = parts[3]
	if tenantID == "" || deviceID == "" {
		return "", "", false
	}
	return tenantID, deviceID, true
}

// ValidateJWTFromClientID verifies a JWT in the Google IoT Core client-id
// mode where tenantID and deviceID come from the MQTT client-id field rather
// than from the username. Key resolution (JWKS vs static PEM) is identical
// to ValidateJWT.
//
// Differences from ValidateJWT:
//   - The "tid" claim is NOT required (tenantID is already authenticated via the
//     client-id prefix "tenants/<tid>/...").
//   - The "sub" claim is checked when present but not required.
//   - All other validation rules (aud, exp, key algorithm) are identical.
func ValidateJWTFromClientID(ctx context.Context, tenantID, deviceID string, tokenBytes []byte, cfg *TenantGatewayConfig, jwks *JWKSCache) error {
	keyFunc, err := jwtKeyFunc(ctx, tenantID, cfg, jwks)
	if err != nil {
		return err
	}
	return validateJWTClaims(tokenBytes, keyFunc, tenantID, deviceID, false)
}

// jwtKeyFunc builds the jwt.Keyfunc for tenantID's configured key source.
// JWKSURL takes precedence over JWTPublicKeyPEM when both happen to be set.
func jwtKeyFunc(ctx context.Context, tenantID string, cfg *TenantGatewayConfig, jwks *JWKSCache) (jwt.Keyfunc, error) {
	switch {
	case cfg.JWKSURL != "":
		if jwks == nil {
			return nil, errors.New("jwt: tenant has JWKSURL configured but no JWKSCache was provided")
		}
		return func(t *jwt.Token) (any, error) {
			kid, _ := t.Header["kid"].(string)
			if kid == "" {
				return nil, fmt.Errorf("%w: token has no kid header", ErrInvalidKid)
			}
			key, err := jwks.Key(ctx, tenantID, cfg.JWKSURL, kid)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", ErrInvalidKid, err)
			}
			if err := checkSigningMethod(t, key); err != nil {
				return nil, err
			}
			return key, nil
		}, nil

	case cfg.JWTPublicKeyPEM != "":
		pubKey, err := parsePublicKeyPEM(cfg.JWTPublicKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("jwt: invalid tenant public key: %w", err)
		}
		return func(t *jwt.Token) (any, error) {
			if err := checkSigningMethod(t, pubKey); err != nil {
				return nil, err
			}
			return pubKey, nil
		}, nil

	default:
		return nil, ErrAuthNotConfigured
	}
}

// checkSigningMethod rejects an algorithm/key-type mismatch (e.g. a token
// claiming RS256 verified against an EC key) before the key is used.
func checkSigningMethod(t *jwt.Token, pubKey any) error {
	switch pubKey.(type) {
	case *rsa.PublicKey:
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return fmt.Errorf("jwt: unexpected signing method %q, expected RSA", t.Header["alg"])
		}
	case *ecdsa.PublicKey:
		if _, ok := t.Method.(*jwt.SigningMethodECDSA); !ok {
			return fmt.Errorf("jwt: unexpected signing method %q, expected ECDSA", t.Header["alg"])
		}
	default:
		return fmt.Errorf("jwt: unsupported key type %T", pubKey)
	}
	return nil
}

// validateJWTClaims runs the shared parse+claims-check logic for both
// ValidateJWT and ValidateJWTFromClientID. requireSub controls whether the
// "sub" claim must be present and match deviceID (true) or is optional but
// still checked when present (false, client-id mode).
func validateJWTClaims(tokenBytes []byte, keyFunc jwt.Keyfunc, tenantID, deviceID string, requireSub bool) error {
	parsed, err := jwt.ParseWithClaims(
		string(tokenBytes),
		&jwt.MapClaims{},
		keyFunc,
		jwt.WithAudience("keel-gateway"),
		jwt.WithIssuedAt(),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return fmt.Errorf("jwt: %w", err)
	}
	if !parsed.Valid {
		return errors.New("jwt: token is not valid")
	}

	claims, ok := parsed.Claims.(*jwt.MapClaims)
	if !ok {
		return errors.New("jwt: cannot read claims")
	}

	sub, _ := claims.GetSubject()
	if requireSub {
		if sub != deviceID {
			return fmt.Errorf("jwt: sub %q does not match device %q", sub, deviceID)
		}
	} else if sub != "" && sub != deviceID {
		return fmt.Errorf("jwt: sub %q does not match device %q", sub, deviceID)
	}

	// tid custom claim, when present, must match tenantID (required in
	// username mode's doc, but the original implementation only enforced
	// it when present — preserved as-is here).
	tid, _ := (*claims)["tid"].(string)
	if tid != "" && tid != tenantID {
		return fmt.Errorf("jwt: tid %q does not match tenant %q", tid, tenantID)
	}

	return nil
}

// parsePublicKeyPEM decodes the first PEM block and returns either an
// *rsa.PublicKey or an *ecdsa.PublicKey.
func parsePublicKeyPEM(pemData string) (any, error) {
	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, errors.New("no PEM block found")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("ParsePKIXPublicKey: %w", err)
	}

	switch pub.(type) {
	case *rsa.PublicKey, *ecdsa.PublicKey:
		return pub, nil
	default:
		return nil, fmt.Errorf("unsupported public key type: %T", pub)
	}
}
