package acl

import "strings"

// matchTopic reports whether filter (an MQTT subscription filter, with +/#
// wildcards) matches topic (a concrete publish/subscribe topic that itself
// may contain + or # in this ACL context, since we also match subscribe
// filters requested by clients against rule filters — MQTT allows a client
// to subscribe with wildcards too, so "topic" here is really "the filter the
// client asked to use", checked against the rule's own filter).
//
// Follows the standard MQTT topic-matching algorithm (same semantics as
// mochi-mqtt's packets.TopicsIndex): "#" only valid as the last filter
// segment and matches it plus everything after; "+" matches exactly one
// segment; a leading "$" topic is never matched by a top-level wildcard.
func matchTopic(filter, topic string) bool {
	if filter == topic {
		return true
	}
	fParts := strings.Split(filter, "/")
	tParts := strings.Split(topic, "/")

	// A wildcard filter must never match a $-prefixed topic unless the
	// filter itself starts with the same literal segment.
	if len(tParts) > 0 && strings.HasPrefix(tParts[0], "$") {
		if len(fParts) == 0 || (fParts[0] != tParts[0] && (fParts[0] == "#" || fParts[0] == "+")) {
			return false
		}
	}

	i := 0
	for ; i < len(fParts); i++ {
		if fParts[i] == "#" {
			// "#" must be the final filter segment and matches any
			// remaining topic segments (including zero of them).
			return i == len(fParts)-1
		}
		if i >= len(tParts) {
			return false
		}
		if fParts[i] == "+" {
			continue
		}
		if fParts[i] != tParts[i] {
			return false
		}
	}
	return i == len(tParts)
}

// specificity scores a topic filter for rule-precedence comparisons: a
// higher score means a more specific match. Literal segments count more
// than "+", which counts more than "#"; longer filters (more segments)
// generally beat shorter ones because they encode more constraints. This
// is a simple weighted sum, not a formal partial order, but it produces the
// intuitive ranking the design calls for (e.g. "telemetry/%c/temp" — three
// literal segments after resolution — outranks "telemetry/#").
func specificity(filter string) int {
	parts := strings.Split(filter, "/")
	score := 0
	for _, p := range parts {
		switch p {
		case "#":
			score += 1
		case "+":
			score += 10
		default:
			score += 100
		}
	}
	return score
}
