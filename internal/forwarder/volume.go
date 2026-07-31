// Package forwarder now holds only the per-tenant daily data-volume quota
// (CheckAndRecordBytes) — a broker-core ACL/quota concern, used directly by
// internal/broker/hooks.go. Everything else that used to live here (keel's
// Redpanda topic taxonomy, twin-service envelope, Ditto/Hono compat) has
// moved to the standalone keel OutputConnector plugin — see keel-design-doc.md
// "Perimetro del componente".
package forwarder

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrDataVolumeExceeded is returned by CheckAndRecordBytes when the tenant has
// consumed more bytes than their daily quota.
var ErrDataVolumeExceeded = errors.New("forwarder: daily data volume limit exceeded")

// DailyBytesKey returns the Redis key used to track per-tenant daily byte
// consumption.  Format: "keel:gw:rl:bytes:<tenantID>:<YYYYMMDD>".
func DailyBytesKey(tenantID string, t time.Time) string {
	return fmt.Sprintf("keel:gw:rl:bytes:%s:%s", tenantID, t.UTC().Format("20060102"))
}

// luaCheckAndRecord atomically increments the counter by msgBytes and returns the
// new total, or -1 when the increment would exceed maxBytes.
//
// KEYS[1] — daily bytes counter key
// ARGV[1] — max bytes per day (int64, as string); 0 means unlimited
// ARGV[2] — bytes to add (int64, as string)
// ARGV[3] — TTL in seconds (48 h = 172800) to set when creating a new key
//
// Returns -1 when the limit would be exceeded; otherwise returns the new total.
var luaCheckAndRecord = redis.NewScript(`
local max   = tonumber(ARGV[1])
local add   = tonumber(ARGV[2])
local ttl   = tonumber(ARGV[3])
if max > 0 then
    local cur = tonumber(redis.call("GET", KEYS[1])) or 0
    if cur + add > max then
        return -1
    end
end
local new = redis.call("INCRBY", KEYS[1], add)
redis.call("EXPIRE", KEYS[1], ttl)
return new
`)

// CheckAndRecordBytes performs an atomic check-then-increment of the tenant's
// daily byte counter.
//
//   - rdb may be nil — in that case the function is a no-op (fail-open).
//   - maxBytes == 0 means unlimited; only the counter is incremented.
//   - On any Redis error the function is fail-open (returns nil) so that a
//     Redis outage never causes message loss.
//
// Returns ErrDataVolumeExceeded when the limit is hit, nil otherwise.
func CheckAndRecordBytes(ctx context.Context, rdb *redis.Client, tenantID string, msgBytes int, maxBytes int64) error {
	if rdb == nil {
		return nil
	}

	key := DailyBytesKey(tenantID, time.Now())
	const ttlSeconds = 48 * 60 * 60 // 48 h

	result, err := luaCheckAndRecord.Run(ctx, rdb, []string{key},
		maxBytes, msgBytes, ttlSeconds).Int64()
	if err != nil {
		// Fail-open: Redis error must not block traffic.
		return nil
	}
	if result == -1 {
		return ErrDataVolumeExceeded
	}
	return nil
}
