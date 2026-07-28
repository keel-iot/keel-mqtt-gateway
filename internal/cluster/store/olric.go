package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"time"

	"github.com/olric-data/olric"
	"github.com/olric-data/olric/config"
	sd "github.com/olric-data/olric/pkg/service_discovery"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

// OlricConfig configures an embedded Olric member. Olric binds two
// independent ports: BindPort for its own main (RESP) protocol, and
// GossipPort for its internal memberlist instance — separate from, and
// unaware of, this project's own membership package. Peers/PeersFunc
// values must be host:GossipPort addresses (memberlist discovery
// addresses), not host:BindPort.
type OlricConfig struct {
	BindAddr      string
	BindPort      int
	GossipPort    int
	AdvertiseAddr string // resolved to an IP internally (memberlist requires one); empty = BindAddr

	// Peers is a static, one-time peer seed, used when PeersFunc is nil.
	Peers []string
	// PeersFunc, when set, is re-invoked on every join retry attempt
	// during Start() (see JoinRetryInterval/MaxJoinAttempts) instead of
	// Olric using a single static Peers snapshot taken once before
	// Start() begins — implemented via Olric's ServiceDiscovery plugin
	// interface (github.com/olric-data/olric/pkg/service_discovery).
	// This widens, but does not remove, the startup race described
	// below: a peer that only becomes visible to PeersFunc *after* the
	// join budget (JoinRetryInterval * MaxJoinAttempts) is exhausted is
	// still never discovered by this member — Olric has no post-Start
	// incremental-join mechanism at all. Confirmed by code inspection:
	// internal/discovery.Discovery.Rejoin exists in olric-data/olric but
	// is dead code, never called from anywhere in that module, and there
	// is no equivalent wire-protocol command either. Takes precedence
	// over Peers when set.
	PeersFunc func() ([]string, error)

	// JoinRetryInterval and MaxJoinAttempts bound how long Olric spends
	// trying to join known peers before falling back to bootstrapping a
	// new single-node cluster. Olric attempts this on *every* startup,
	// even a legitimate first-node bootstrap with no peers at all.
	// Defaults (300ms / 5 attempts, ~1.5s total) suit near-simultaneous
	// startup (e.g. docker-compose). Widen both for staggered rollouts
	// (e.g. a K8s StatefulSet, where siblings can start seconds to
	// minutes apart) — paired with PeersFunc, each retry re-resolves the
	// current peer list, so a sibling that becomes visible partway
	// through a wider window is still found. StartTimeout is
	// automatically floored to comfortably exceed this budget, so
	// widening these two is enough on its own.
	JoinRetryInterval time.Duration
	MaxJoinAttempts   int

	DMapName string
	Log      *slog.Logger
	// StartTimeout bounds how long to wait for Olric's Started callback.
	// Default 20s, automatically raised if JoinRetryInterval *
	// MaxJoinAttempts would otherwise exceed it.
	StartTimeout time.Duration
}

// peerResolverDiscovery adapts a live-resolving func into Olric's
// ServiceDiscovery plugin interface so DiscoverPeers is called fresh on
// every join retry attempt. Passed directly as
// config.Config.ServiceDiscovery["plugin"] — olric-data/olric supports
// handing it an already-constructed instance this way, no plugin.Open /
// .so loading required (see internal/discovery.loadServiceDiscoveryPlugin).
type peerResolverDiscovery struct {
	resolve func() ([]string, error)
}

func (p *peerResolverDiscovery) Initialize() error                      { return nil }
func (p *peerResolverDiscovery) SetConfig(map[string]interface{}) error { return nil }
func (p *peerResolverDiscovery) SetLogger(*log.Logger)                  {}
func (p *peerResolverDiscovery) Register() error                        { return nil }
func (p *peerResolverDiscovery) Deregister() error                      { return nil }
func (p *peerResolverDiscovery) Close() error                           { return nil }
func (p *peerResolverDiscovery) DiscoverPeers() ([]string, error)       { return p.resolve() }

var _ sd.ServiceDiscovery = (*peerResolverDiscovery)(nil)

// OlricStore implements ClusterStore over olric-data/olric, either as an
// embedded member (core nodes — see NewEmbeddedOlricStore) or as a thin
// remote client that never joins Olric's own gossip ring (edge nodes —
// see NewRemoteOlricStore).
type OlricStore struct {
	db     *olric.Olric // nil for a remote (non-embedded) store
	client olric.Client
	dmap   olric.DMap
	ps     *olric.PubSub
}

// logWriter adapts an *slog.Logger to the io.Writer Olric wants for its
// own internal (very chatty) memberlist/gossip logging — same pattern as
// internal/cluster/membership's slogWriter.
type logWriter struct{ log *slog.Logger }

func (w logWriter) Write(p []byte) (int, error) {
	w.log.Debug("olric", "msg", string(p))
	return len(p), nil
}

// NewEmbeddedOlricStore creates, starts, and joins an embedded Olric
// member. See OlricConfig.PeersFunc's doc for the join/discovery model
// and its limits.
func NewEmbeddedOlricStore(cfg OlricConfig) (*OlricStore, error) {
	c := config.New("lan")
	c.BindAddr = cfg.BindAddr
	c.BindPort = cfg.BindPort

	if cfg.PeersFunc != nil {
		c.ServiceDiscovery = map[string]interface{}{
			"plugin": &peerResolverDiscovery{resolve: cfg.PeersFunc},
		}
	} else {
		c.Peers = cfg.Peers
	}

	joinRetryInterval := cfg.JoinRetryInterval
	if joinRetryInterval <= 0 {
		joinRetryInterval = 300 * time.Millisecond
	}
	maxJoinAttempts := cfg.MaxJoinAttempts
	if maxJoinAttempts <= 0 {
		maxJoinAttempts = 5
	}
	c.JoinRetryInterval = joinRetryInterval
	c.MaxJoinAttempts = maxJoinAttempts

	mc, err := config.NewMemberlistConfig("lan")
	if err != nil {
		return nil, fmt.Errorf("olric store: memberlist config: %w", err)
	}
	mc.BindAddr = cfg.BindAddr
	mc.BindPort = cfg.GossipPort
	// resolvedAdvertiseIP, when non-empty, is this member's real routable
	// IP — used below to build a pub/sub self-connect address that
	// actually works (see that comment for why cfg.BindAddr itself can't
	// be used when it's a wildcard).
	var resolvedAdvertiseIP string
	if cfg.AdvertiseAddr != "" {
		// memberlist gossips AdvertiseAddr as a raw IP, not a hostname — in
		// Docker/K8s the advertise address is usually a container/pod DNS
		// name, so resolve it here (mirrors membership.New's identical need).
		ip, err := net.ResolveIPAddr("ip", cfg.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("olric store: resolve advertise addr %q: %w", cfg.AdvertiseAddr, err)
		}
		resolvedAdvertiseIP = ip.String()
		mc.AdvertiseAddr = resolvedAdvertiseIP
		mc.AdvertisePort = cfg.GossipPort
	}
	c.MemberlistConfig = mc

	if cfg.Log != nil {
		c.LogOutput = logWriter{log: cfg.Log}
	}

	started := make(chan struct{})
	c.Started = func() { close(started) }

	db, err := olric.New(c)
	if err != nil {
		return nil, fmt.Errorf("olric store: new: %w", err)
	}

	startErr := make(chan error, 1)
	go func() {
		if err := db.Start(); err != nil {
			startErr <- err
		}
	}()

	timeout := cfg.StartTimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	// Widening the join budget without also widening StartTimeout would
	// make this fire before Olric itself gives up — floor it comfortably
	// above the join budget rather than relying on the caller to keep the
	// two in sync.
	if joinBudget := joinRetryInterval * time.Duration(maxJoinAttempts); joinBudget+5*time.Second > timeout {
		timeout = joinBudget + 5*time.Second
	}
	select {
	case <-started:
	case err := <-startErr:
		return nil, fmt.Errorf("olric store: start: %w", err)
	case <-time.After(timeout):
		return nil, fmt.Errorf("olric store: did not start within %s", timeout)
	}

	dmapName := cfg.DMapName
	if dmapName == "" {
		dmapName = "keel.routes"
	}
	// A freshly started member's internal client pool (used by NewPubSub
	// to round-robin-pick a connection when no address is given) is empty
	// until it has issued at least one addressed request — pin the
	// pub/sub connection to this member's own address to sidestep that
	// entirely, rather than depending on pool state that happens to get
	// populated as a side effect of other calls.
	//
	// The connect address can't be the literal configured BindAddr when
	// it's a wildcard ("0.0.0.0"): Olric's own config.SetupNetworkConfig
	// resolves a wildcard BindAddr down to one specific private interface
	// IP (via sockaddr.GetPrivateIP) *before* actually binding — the
	// server ends up listening on that one address only, not on all
	// interfaces and not on loopback, so neither "0.0.0.0:port" nor
	// "127.0.0.1:port" reach it from inside the same container. Reuse the
	// already-resolved advertise IP (resolvedAdvertiseIP, from the exact
	// same hostname Docker/K8s assigns this container) instead — it's the
	// same private IP Olric will have picked, since both are resolving
	// "this container's own address" from the same starting point.
	selfConnectAddr := resolvedAdvertiseIP
	if selfConnectAddr == "" {
		selfConnectAddr = cfg.BindAddr // already concrete (non-wildcard) when no AdvertiseAddr was given
	}
	selfAddr := fmt.Sprintf("%s:%d", selfConnectAddr, cfg.BindPort)
	return newOlricStore(db, db.NewEmbeddedClient(), dmapName, olric.ToAddress(selfAddr))
}

// NewRemoteOlricStore creates a thin, non-embedded client against an
// existing Olric cluster reachable at addrs — used by edge nodes, which
// have no reason to join Olric's gossip ring themselves (mirroring how
// they already reach the raft-backed session registry via RemoteRegistry
// / gRPC instead of running raft themselves).
func NewRemoteOlricStore(addrs []string, dmapName string) (*OlricStore, error) {
	client, err := olric.NewClusterClient(addrs)
	if err != nil {
		return nil, fmt.Errorf("olric store: new cluster client: %w", err)
	}
	if dmapName == "" {
		dmapName = "keel.routes"
	}
	var pubsubOpts []olric.PubSubOption
	if len(addrs) > 0 {
		pubsubOpts = append(pubsubOpts, olric.ToAddress(addrs[0]))
	}
	return newOlricStore(nil, client, dmapName, pubsubOpts...)
}

func newOlricStore(db *olric.Olric, client olric.Client, dmapName string, pubsubOpts ...olric.PubSubOption) (*OlricStore, error) {
	dm, err := client.NewDMap(dmapName)
	if err != nil {
		return nil, fmt.Errorf("olric store: new dmap %q: %w", dmapName, err)
	}
	ps, err := client.NewPubSub(pubsubOpts...)
	if err != nil {
		return nil, fmt.Errorf("olric store: new pubsub: %w", err)
	}
	return &OlricStore{db: db, client: client, dmap: dm, ps: ps}, nil
}

func (s *OlricStore) Put(ctx context.Context, key string, value []byte) error {
	return s.dmap.Put(ctx, key, value)
}

func (s *OlricStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := s.dmap.Get(ctx, key)
	if errors.Is(err, olric.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	b, err := resp.Byte()
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// deleteConcurrency caps fan-out for per-key deletes. Each key is its own
// Olric round-trip (see Delete's doc); without a cap a PurgeNode for a node
// holding thousands of filters would open thousands of goroutines at once.
const deleteConcurrency = 32

// Delete removes zero or more keys.
//
// It deliberately does NOT hand all keys to a single Olric DMap.Delete call.
// Olric's variadic deleteKeys (both olric-data/olric@v0.7.4 and
// tochemey/olric@v0.3.17) returns unconditionally after processing the first
// REMOTE partition owner in its per-owner loop — keys owned by any other
// remote member are silently skipped. Empirically confirmed: a 60-key
// Delete spanning 3 members left 49 keys behind (see
// TestMultiKeyDeleteAcrossPartitionOwners). A single-key Delete hits exactly
// one owner, so that code path has no multi-owner loop to short-circuit and
// is correct. This method fans the keys out as concurrent single-key deletes
// under a bounded errgroup instead. Correctness over a single round-trip —
// callers (PurgeNode, OnDisconnect batch) are infrequent and bounded by one
// client/node's own filter count.
func (s *OlricStore) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		_, err := s.dmap.Delete(ctx, keys[0])
		return err
	}
	sem := make(chan struct{}, deleteConcurrency)
	g, gctx := errgroup.WithContext(ctx)
	for _, k := range keys {
		key := k
		select {
		case sem <- struct{}{}:
		case <-gctx.Done():
			return g.Wait()
		}
		g.Go(func() error {
			defer func() { <-sem }()
			_, err := s.dmap.Delete(gctx, key)
			return err
		})
	}
	return g.Wait()
}

func (s *OlricStore) Scan(ctx context.Context) (KeyIterator, error) {
	it, err := s.dmap.Scan(ctx)
	if err != nil {
		return nil, err
	}
	return &olricKeyIterator{it: it}, nil
}

func (s *OlricStore) Publish(ctx context.Context, channel string, message []byte) error {
	_, err := s.ps.Publish(ctx, channel, message)
	return err
}

func (s *OlricStore) Subscribe(ctx context.Context, channel string) (Subscription, error) {
	rps := s.ps.Subscribe(ctx, channel)
	if _, err := rps.Receive(ctx); err != nil {
		_ = rps.Close()
		return nil, fmt.Errorf("olric store: subscribe %q: %w", channel, err)
	}

	out := make(chan []byte, 64)
	go func() {
		defer close(out)
		for msg := range rps.Channel() {
			select {
			case out <- []byte(msg.Payload):
			case <-ctx.Done():
				return
			}
		}
	}()
	return &olricSubscription{rps: rps, ch: out}, nil
}

// Close shuts down the local Olric member (embedded store) or just the
// connection (remote store).
func (s *OlricStore) Close(ctx context.Context) error {
	if s.db != nil {
		return s.db.Shutdown(ctx)
	}
	return s.client.Close(ctx)
}

type olricKeyIterator struct{ it olric.Iterator }

func (i *olricKeyIterator) Next() bool  { return i.it.Next() }
func (i *olricKeyIterator) Key() string { return i.it.Key() }
func (i *olricKeyIterator) Close()      { i.it.Close() }

type olricSubscription struct {
	rps *redis.PubSub
	ch  chan []byte
}

func (s *olricSubscription) Messages() <-chan []byte { return s.ch }
func (s *olricSubscription) Close() error            { return s.rps.Close() }
