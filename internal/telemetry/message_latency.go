package telemetry

import (
	"strconv"
	"strings"
	"time"
)

// ObserveInfluxMessageAge observes the timestamp at the end of each Influx
// Line Protocol point. Since raw line protocol does not carry its precision,
// common Unix seconds/milliseconds/microseconds/nanoseconds magnitudes are
// detected automatically. Missing, invalid, or future timestamps are ignored.
func ObserveInfluxMessageAge(payload []byte, tenantID, qos string, now time.Time) {
	for _, rawLine := range strings.Split(string(payload), "\n") {
		line := strings.TrimSpace(rawLine)
		ts, ok := influxLineTimestamp(line)
		if !ok {
			continue
		}
		age := now.Sub(ts).Seconds()
		if age >= 0 {
			MessageAge.WithLabelValues(tenantID, qos).Observe(age)
		}
	}
}

func influxLineTimestamp(line string) (time.Time, bool) {
	if line == "" || strings.HasPrefix(line, "#") || !strings.ContainsRune(line, '=') {
		return time.Time{}, false
	}
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	abs := n
	if abs < 0 {
		abs = -abs
	}
	var ts time.Time
	switch {
	case abs < 1e11:
		ts = time.Unix(n, 0)
	case abs < 1e14:
		ts = time.Unix(0, n*int64(time.Millisecond))
	case abs < 1e17:
		ts = time.Unix(0, n*int64(time.Microsecond))
	default:
		ts = time.Unix(0, n)
	}
	return ts, true
}
