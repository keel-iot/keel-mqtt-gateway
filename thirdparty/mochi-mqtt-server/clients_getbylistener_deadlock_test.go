package mqtt

import (
	"sync"
	"testing"
	"time"
)

func TestClientsGetByListenerDoesNotRecurseRLock(t *testing.T) {
	cl := NewClients()
	for i := 0; i < 64; i++ {
		id := string(rune('a'+i%26)) + string(rune('0'+i/26))
		cl.Add(&Client{ID: id, Net: ClientConnection{Listener: "tcp1"}})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for n := 0; n < 2000; n++ {
					_ = cl.GetByListener("tcp1")
				}
			}()
		}
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := string(rune('a' + i))
				for n := 0; n < 2000; n++ {
					cl.Add(&Client{ID: id, Net: ClientConnection{Listener: "tcp1"}})
					cl.Delete(id)
				}
			}(i)
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("GetByListener deadlocked against a concurrent Delete")
	}
}
