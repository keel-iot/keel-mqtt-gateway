// Package lifecycle implements voluntary and monitored core-node exit.
//
// Voluntary exit (Drain) is meant to run inside the same process as the
// server, triggered via the management API's POST /api/cluster/drain —
// the `keel-gateway drain` CLI subcommand (wired for a future K8s
// preStop hook) is a thin HTTP client against that endpoint, not a
// separate raft participant.
package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/membership"
	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
)

const leaveTimeout = 5 * time.Second

// Drain performs a voluntary core-node exit:
//  1. if this node is raft leader, transfer leadership explicitly so the
//     cluster doesn't pay for an empty election timeout;
//  2. broadcast voluntary departure over gossip so peers stop counting
//     this node in CoreGRPCAddrs/NodesFor immediately instead of waiting
//     for SWIM failure detection.
//
// Does not remove the node from the raft configuration — a drained node
// is expected to come back (rolling update) and rejoin with its log
// intact. Use raft.Node.RemoveServer explicitly for a permanent departure.
func Drain(_ context.Context, raftNode *keelraft.Node, m *membership.Membership, log *slog.Logger) error {
	if raftNode != nil {
		wasLeader := raftNode.IsLeader()
		log.Info("lifecycle: drain starting", "was_leader", wasLeader)
		if wasLeader {
			if err := raftNode.LeadershipTransfer(); err != nil {
				log.Warn("lifecycle: leadership transfer failed, proceeding anyway", "error", err)
			} else {
				log.Info("lifecycle: leadership transferred")
			}
		}
	}

	if m != nil {
		if err := m.Leave(leaveTimeout); err != nil {
			return fmt.Errorf("lifecycle: gossip leave: %w", err)
		}
		log.Info("lifecycle: left gossip cluster")
	}

	return nil
}
