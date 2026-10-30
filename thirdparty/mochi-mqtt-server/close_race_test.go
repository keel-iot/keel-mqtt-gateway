package mqtt

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/listeners"
)

func TestCloseWhileConnecting(t *testing.T) {
	for i := 0; i < 40; i++ {
		s := New(nil)
		tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
		if err := s.AddListener(tcp); err != nil {
			t.Fatalf("add listener: %v", err)
		}
		go func() { _ = s.Serve() }()
		addr := tcp.Address()

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if c, err := net.Dial("tcp", addr); err == nil {
					_ = c.Close()
				}
			}
		}()

		time.Sleep(time.Millisecond)
		_ = s.Close()
		close(stop)
		wg.Wait()
	}
}
