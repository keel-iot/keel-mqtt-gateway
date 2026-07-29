package redisrouter

import (
	"bytes"
	"context"
	"net"
	"regexp"
	"testing"
	"time"
)

// fakeRedisServer is a minimal RESP-speaking TCP listener — enough for
// go-redis's connection init and Ping (all Router needs to verify
// reachability) without a real Redis binary or a new test dependency (no
// miniredis in go.mod). Two go-redis behaviors this must account for,
// confirmed by capturing the raw bytes a real client sends against this
// exact server:
//
//  1. go-redis always attempts a HELLO on every new connection regardless
//     of RESP2/RESP3 (see redis.go's initConn). A server that only ever
//     replies +PONG makes HELLO's reply look like a malformed map (RESP3
//     HELLO's real reply type), which go-redis treats as a hard connection
//     error rather than the "server predates HELLO, fall back to RESP2"
//     case it's designed to tolerate — that fallback only triggers on a
//     proper RESP error reply, so HELLO must be answered with one.
//  2. Immediately after that, go-redis pipelines two unrelated commands
//     (CLIENT SETINFO LIB-NAME/LIB-VER) in a single Write — observed as
//     one Read() on the server side containing both. Replying once per
//     Read() rather than once per command desyncs the reply stream (the
//     client's second queued reply then never arrives), which hangs every
//     subsequent command until the caller's context deadline — so replies
//     must be counted per pipelined command, not per Read().
type fakeRedisServer struct {
	ln net.Listener
}

// respCommandCount counts top-level RESP array headers ("*N\r\n") in buf —
// each one marks the start of one pipelined command. Redis command
// requests are always a flat array of bulk strings (no nested arrays), so
// this simple count is exact for anything a real client sends, without
// needing a full RESP parser.
var respArrayHeader = regexp.MustCompile(`\*\d+\r\n`)

func respCommandCount(buf []byte) int {
	return len(respArrayHeader.FindAll(buf, -1))
}

func startFakeRedisServer(t *testing.T) *fakeRedisServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeRedisServer{ln: ln}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *fakeRedisServer) addr() string { return s.ln.Addr().String() }

func (s *fakeRedisServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go func() {
			defer conn.Close()
			buf := make([]byte, 4096)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					return
				}
				data := buf[:n]
				count := respCommandCount(data)
				if count == 0 {
					count = 1 // defensive: always reply at least once to any input
				}
				var out bytes.Buffer
				for i := 0; i < count; i++ {
					// Only the very first command a fresh connection ever
					// sends is HELLO (see fakeRedisServer's doc point 1);
					// safe to key off "does this read contain HELLO" rather
					// than tracking per-command position.
					if i == 0 && bytes.Contains(bytes.ToUpper(data), []byte("HELLO")) {
						out.WriteString("-ERR unknown command 'HELLO'\r\n")
						continue
					}
					out.WriteString("+PONG\r\n")
				}
				if _, err := conn.Write(out.Bytes()); err != nil {
					return
				}
			}
		}()
	}
}

func TestNew_Succeeds(t *testing.T) {
	srv := startFakeRedisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, err := New(ctx, srv.addr(), "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()

	if r.Addr() != srv.addr() {
		t.Fatalf("expected addr %s, got %s", srv.addr(), r.Addr())
	}
	if r.Client() == nil {
		t.Fatalf("expected non-nil Client()")
	}
}

func TestNew_FailsOnUnreachableAddr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Port 1 is a well-known unassigned/reserved port unlikely to have
	// anything listening — connection should be refused quickly rather
	// than hanging for the full context timeout.
	_, err := New(ctx, "127.0.0.1:1", "")
	if err == nil {
		t.Fatalf("expected New to fail against an unreachable address")
	}
}

func TestRedirect_NoopWhenSameAddr(t *testing.T) {
	srv := startFakeRedisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, err := New(ctx, srv.addr(), "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()

	before := r.Client()
	if err := r.Redirect(ctx, srv.addr()); err != nil {
		t.Fatalf("Redirect to the same addr should not error, got: %v", err)
	}
	after := r.Client()
	if before != after {
		t.Fatalf("expected Redirect to the same addr to be a no-op (same *redis.Client), got a swapped client")
	}
}

func TestRedirect_SwapsToNewReachableAddr(t *testing.T) {
	srv1 := startFakeRedisServer(t)
	srv2 := startFakeRedisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, err := New(ctx, srv1.addr(), "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()

	before := r.Client()
	if err := r.Redirect(ctx, srv2.addr()); err != nil {
		t.Fatalf("Redirect: %v", err)
	}
	if r.Addr() != srv2.addr() {
		t.Fatalf("expected addr %s after redirect, got %s", srv2.addr(), r.Addr())
	}
	if r.Client() == before {
		t.Fatalf("expected Redirect to a different reachable addr to swap the underlying client")
	}
}

func TestRedirect_FailsAndKeepsOldAddrWhenUnreachable(t *testing.T) {
	srv := startFakeRedisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, err := New(ctx, srv.addr(), "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()

	before := r.Client()
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer shortCancel()
	if err := r.Redirect(shortCtx, "127.0.0.1:1"); err == nil {
		t.Fatalf("expected Redirect to an unreachable addr to fail")
	}
	if r.Addr() != srv.addr() {
		t.Fatalf("expected addr to remain %s after a failed redirect, got %s", srv.addr(), r.Addr())
	}
	if r.Client() != before {
		t.Fatalf("expected Client() to remain the original client after a failed redirect")
	}
}
