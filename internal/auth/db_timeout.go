package auth

import (
	"context"
	"time"
)

// withDBTimeout applies the optional database operation deadline while
// preserving the caller's cancellation. A zero timeout intentionally keeps
// the previous unbounded behaviour for deployments that have not configured
// DB_TIMEOUT yet.
func withDBTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
