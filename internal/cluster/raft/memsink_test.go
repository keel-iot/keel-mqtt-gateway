package raft

import (
	"bytes"
	"io"
)

// memSink is a minimal in-memory hraft.SnapshotSink for exercising
// FSM.Snapshot/Restore round-trips without a real raft.FileSnapshotStore.
type memSink struct {
	buf bytes.Buffer
}

func newMemSink() *memSink { return &memSink{} }

func (s *memSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *memSink) Close() error                { return nil }
func (s *memSink) ID() string                  { return "test-snapshot" }
func (s *memSink) Cancel() error               { return nil }

func (s *memSink) reader() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(s.buf.Bytes()))
}
