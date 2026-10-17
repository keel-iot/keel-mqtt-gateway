package broker

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Read-only: WATCH on the inflight hash still fences the subsequent enqueue.
// Search on the Redis side so a nearly-full queue never costs 65,535 network
// round trips. prefix is empty for legacy fields and "out:" for schema 2;
// legacy numeric fields also reserve IDs during a directional migration.
const nextOfflinePacketCounterScript = `
local counter = tonumber(ARGV[1])
local prefix = ARGV[2]
if prefix == '' and redis.call('HLEN', KEYS[1]) >= 65535 then
  return 0
end
for offset = 0, 65534 do
  local id = tostring((counter + offset) % 65535 + 1)
  if redis.call('HEXISTS', KEYS[1], prefix .. id) == 0 and
     (prefix == '' or redis.call('HEXISTS', KEYS[1], id) == 0) then
    return counter + offset + 1
  end
end
return 0
`

func nextOfflinePacketCounter(ctx context.Context, tx *redis.Tx, key string, counter int64, prefix string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	next, err := tx.Eval(ctx, nextOfflinePacketCounterScript, []string{key}, counter, prefix).Int64()
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return next, nil
}
