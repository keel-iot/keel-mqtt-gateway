package forwarder

import (
	"net/url"
	"strconv"
	"strings"
)

// PropertyBag holds Hono-style MQTT property bag values extracted from the
// tail of an MQTT topic.
//
// Format: "<topic>/?<key1>=<val1>&<key2>=<val2>"
// Spec: https://www.eclipse.org/hono/docs/concepts/mqtt-adapter/#property-bag
type PropertyBag struct {
	// ContentType is the MIME type for the payload, if set via "content-type".
	ContentType string
	// TTL is the time-to-live in seconds, if set via "hono-ttd" or "hono-ttl".
	// Zero means absent or invalid.
	TTL int
	// Raw contains all key-value pairs extracted from the property bag,
	// including content-type and hono-ttl/hono-ttd.
	Raw map[string]string
}

// ParsePropertyBag splits an MQTT topic into the canonical routing part
// (cleanTopic) and an optional Hono property bag.
//
// A property bag is signalled by "/?<query>" appearing anywhere in the topic.
// The returned cleanTopic has the "/?..." suffix removed.
//
// Examples:
//
//	"telemetry"                                                  → ("telemetry", empty)
//	"telemetry/?content-type=application%2Fjson"                 → ("telemetry", {ContentType:"application/json"})
//	"telemetry/t1/d1/?content-type=text%2Fplain&hono-ttl=30"    → ("telemetry/t1/d1", {ContentType:"text/plain", TTL:30})
func ParsePropertyBag(topic string) (cleanTopic string, bag PropertyBag) {
	idx := strings.Index(topic, "/?")
	if idx < 0 {
		return topic, PropertyBag{}
	}

	cleanTopic = topic[:idx]
	queryStr := topic[idx+2:] // skip "/?"

	values, err := url.ParseQuery(queryStr)
	if err != nil {
		// Malformed query — drop property bag but keep the clean topic.
		return cleanTopic, PropertyBag{}
	}

	raw := make(map[string]string, len(values))
	for k, vs := range values {
		if len(vs) > 0 {
			raw[k] = vs[0]
		}
	}

	bag.Raw = raw

	if ct, ok := raw["content-type"]; ok {
		bag.ContentType = ct
	}

	// "hono-ttl" takes precedence over the legacy "hono-ttd" alias.
	for _, key := range []string{"hono-ttl", "hono-ttd"} {
		if v, ok := raw[key]; ok {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				bag.TTL = n
				break
			}
		}
	}

	return cleanTopic, bag
}
