package ledgerclient

import (
	"context"
	"sync"
	"testing"
)

func TestSyncBookieAdapterRoundTrip(t *testing.T) {
	mem := NewMemoryBookie()
	async := AsyncFromSync(mem)
	syncB := NewSyncBookie(async)

	key := []byte("k")
	if err := syncB.CreateLedger(7, key); err != nil {
		t.Fatal(err)
	}
	if err := syncB.AddEntry(7, 0, -1, []byte("hi"), key); err != nil {
		t.Fatal(err)
	}
	e, err := syncB.ReadEntry(7, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(e.Payload) != "hi" {
		t.Fatalf("payload=%q", e.Payload)
	}
}

func TestBookieAsyncCallbackOnce(t *testing.T) {
	mem := NewMemoryBookie()
	async := AsyncFromSync(mem)
	key := []byte("k")

	var wg sync.WaitGroup
	wg.Add(1)
	async.CreateLedgerAsync(context.Background(), 1, key, func(err error) {
		defer wg.Done()
		if err != nil {
			t.Errorf("create: %v", err)
		}
	})
	wg.Wait()

	wg.Add(1)
	async.AddEntryAsync(context.Background(), 1, 0, -1, []byte("x"), key, func(err error) {
		defer wg.Done()
		if err != nil {
			t.Errorf("add: %v", err)
		}
	})
	wg.Wait()
}
