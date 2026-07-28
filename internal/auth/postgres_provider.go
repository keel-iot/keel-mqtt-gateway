package auth

import (
	"context"

	"github.com/google/uuid"
)

// PostgresProvider wraps the existing Validator to implement the AuthProvider
// interface.  This is the default backend used in production.
type PostgresProvider struct {
	v *Validator
}

// NewPostgresProvider creates an AuthProvider backed by PostgreSQL via the
// given Validator.
func NewPostgresProvider(v *Validator) AuthProvider {
	return &PostgresProvider{v: v}
}

func (p *PostgresProvider) ValidatePassword(ctx context.Context, deviceID, token string) (*DeviceInfo, error) {
	return p.v.Validate(ctx, deviceID, token)
}

func (p *PostgresProvider) LookupByCN(ctx context.Context, deviceID, tenantID string) (*DeviceInfo, error) {
	return p.v.LookupByCN(ctx, deviceID, tenantID)
}

func (p *PostgresProvider) UpdateLastSeen(ctx context.Context, deviceID uuid.UUID) {
	p.v.UpdateLastSeen(ctx, deviceID)
}
