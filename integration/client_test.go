//go:build integration

package integration_test

import (
	"fmt"
	"sync"
	"testing"

	ledgerclient "github.com/devotionalist/open-ledger-client-go"
)

func TestSingleWriterAppendReadClose(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	ledgerID := nextLedgerID()
	writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, []byte("k8s-secret"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := writer.Append([]byte("hello"))
	if err != nil || id != 0 {
		t.Fatalf("append0 %d %v", id, err)
	}
	id, err = writer.Append([]byte("world"))
	if err != nil || id != 1 {
		t.Fatalf("append1 %d %v", id, err)
	}
	entry, err := writer.Read(1)
	if err != nil || string(entry.Payload) != "world" {
		t.Fatalf("read %+v %v", entry, err)
	}
	last, err := writer.Close()
	if err != nil || last != 1 {
		t.Fatalf("close %d %v", last, err)
	}
	meta, err := metas.Get(ledgerID)
	if err != nil || meta.State != ledgerclient.StateClosed || meta.LastEntryID != 1 {
		t.Fatalf("meta %+v %v", meta, err)
	}
}

func TestSingleWriterManyEntries(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	ledgerID := nextLedgerID()
	writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, []byte("k8s-secret"))
	if err != nil {
		t.Fatal(err)
	}
	const n = 32
	for i := 0; i < n; i++ {
		id, err := writer.Append([]byte(fmt.Sprintf("e%d", i)))
		if err != nil || id != uint64(i) {
			t.Fatalf("append %d: id=%d err=%v", i, id, err)
		}
	}
	if writer.LAC() != n-1 {
		t.Fatalf("lac %d", writer.LAC())
	}
	for i := 0; i < n; i++ {
		entry, err := writer.Read(uint64(i))
		if err != nil || string(entry.Payload) != fmt.Sprintf("e%d", i) {
			t.Fatalf("read %d %+v %v", i, entry, err)
		}
	}
	last, err := writer.Close()
	if err != nil || last != n-1 {
		t.Fatalf("close %d %v", last, err)
	}
}

func TestTwoWritersTwoLedgers(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ledgerID := nextLedgerID()
			writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, key)
			if err != nil {
				errs <- fmt.Errorf("writer%d create: %w", w, err)
				return
			}
			for i := 0; i < 8; i++ {
				want := fmt.Sprintf("w%d-e%d", w, i)
				id, err := writer.Append([]byte(want))
				if err != nil || id != uint64(i) {
					errs <- fmt.Errorf("writer%d append %d: id=%d err=%v", w, i, id, err)
					return
				}
			}
			last, err := writer.Close()
			if err != nil || last != 7 {
				errs <- fmt.Errorf("writer%d close: last=%d err=%v", w, last, err)
				return
			}
			meta, err := metas.Get(ledgerID)
			if err != nil || meta.State != ledgerclient.StateClosed || meta.LastEntryID != 7 {
				errs <- fmt.Errorf("writer%d meta %+v %v", w, meta, err)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestRecoverEmpty(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	ledgerID := nextLedgerID()
	if _, err := ledgerclient.CreateOpenLedger(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, []byte("k8s-secret")); err != nil {
		t.Fatal(err)
	}
	last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last != -1 {
		t.Fatalf("recover %d %v", last, err)
	}
	meta, err := metas.Get(ledgerID)
	if err != nil || meta.State != ledgerclient.StateClosed {
		t.Fatalf("meta %+v %v", meta, err)
	}
}

func TestOldWriterBlockedAfterRecover(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()

	writerA, err := ledgerclient.Create(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writerA.Append([]byte("a0")); err != nil {
		t.Fatal(err)
	}
	metaA := writerA.Meta()

	last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last != 0 {
		t.Fatalf("recover %d %v", last, err)
	}

	_, err = ledgerclient.AppendEntry(c.bookies, metaA, 1, 0, []byte("zombie"))
	if ledgerclient.KindOf(err) != ledgerclient.KindFenced {
		t.Fatalf("want fenced, got %v", err)
	}

	entry, err := ledgerclient.ReadEntry(c.bookies, metaA, 0)
	if err != nil || string(entry.Payload) != "a0" {
		t.Fatalf("confirmed entry %+v %v", entry, err)
	}
}

func TestFenceBlocksAdd(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	// Dense E=Qw=3 so fencing 2 members is enough for Qa=2 coverage.
	ids := c.denseIDs()
	bookies := c.denseBookies()
	meta, err := ledgerclient.CreateOpenLedger(metas, bookies, ledgerID, c.quorumDense(), ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerclient.AppendEntry(bookies, meta, 0, -1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids[:2] {
		if err := bookies[id].FenceLedger(ledgerID, key); err != nil {
			t.Fatalf("fence %s: %v", id, err)
		}
	}
	_, err = ledgerclient.AppendEntry(bookies, meta, 1, 0, []byte("zombie"))
	if ledgerclient.KindOf(err) != ledgerclient.KindFenced {
		t.Fatalf("want fenced, got %v", err)
	}
}

func TestDirtyTailRepair(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 2)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ids, bookies := c.pair()
	q := ledgerclient.Quorum{E: 2, Qw: 2, Qa: 1}
	ledgerID := nextLedgerID()
	meta, err := ledgerclient.CreateOpenLedger(metas, bookies, ledgerID, q, ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerclient.AppendEntry(bookies, meta, 0, -1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := bookies[ids[0]].AddEntry(ledgerID, 1, 0, []byte("dirty"), key); err != nil {
		t.Fatal(err)
	}
	last, err := ledgerclient.RecoverLedger(metas, bookies, ledgerID)
	if err != nil || last != 1 {
		t.Fatalf("recover %d %v", last, err)
	}
	for _, id := range ids {
		entry, err := bookies[id].ReadEntry(ledgerID, 1)
		if err != nil || string(entry.Payload) != "dirty" {
			t.Fatalf("%s %+v %v", id, entry, err)
		}
	}
}

func TestConfirmedSurviveRecover(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := writer.Append([]byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last != 4 {
		t.Fatalf("recover %d %v", last, err)
	}
	last2, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last2 != 4 {
		t.Fatalf("second recover %d %v", last2, err)
	}
	closed, err := metas.Get(ledgerID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		entry, err := ledgerclient.ReadEntry(c.bookies, closed, uint64(i))
		if err != nil || string(entry.Payload) != fmt.Sprintf("c%d", i) {
			t.Fatalf("entry %d %+v %v", i, entry, err)
		}
	}
	assertAllFenced(t, c, ledgerID)
}

func TestConcurrentRecoverConverges(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	meta, err := ledgerclient.CreateOpenLedger(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerclient.AppendEntry(c.bookies, meta, 0, -1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerclient.AppendEntry(c.bookies, meta, 1, 0, []byte("y")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan int64, 2)
	errors := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
			if err != nil {
				errors <- err
				return
			}
			results <- last
		}()
	}
	wg.Wait()
	close(errors)
	close(results)
	for err := range errors {
		t.Fatal(err)
	}
	var lasts []int64
	for last := range results {
		lasts = append(lasts, last)
	}
	if len(lasts) != 2 || lasts[0] != 1 || lasts[1] != 1 {
		t.Fatalf("recover results %v", lasts)
	}
	stored, err := metas.Get(ledgerID)
	if err != nil || stored.State != ledgerclient.StateClosed || stored.LastEntryID != 1 {
		t.Fatalf("meta %+v %v", stored, err)
	}
}

func TestHoleStopsTheScan(t *testing.T) {
	c := openCluster(t)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	q := ledgerclient.Quorum{E: 1, Qw: 1, Qa: 1}
	ids := c.ids[:1]
	bookies := map[ledgerclient.BookieID]ledgerclient.Bookie{ids[0]: c.bookies[ids[0]]}
	ledgerID := nextLedgerID()
	meta, err := ledgerclient.CreateOpenLedger(metas, bookies, ledgerID, q, ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerclient.AppendEntry(bookies, meta, 0, -1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := bookies[ids[0]].AddEntry(ledgerID, 5, 0, []byte("skip"), key); err != nil {
		t.Fatal(err)
	}
	last, err := ledgerclient.RecoverLedger(metas, bookies, ledgerID)
	if err != nil || last != 0 {
		t.Fatalf("recover %d %v", last, err)
	}
}

func TestStripeWriteAndRecover(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 5)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	q := c.quorumCluster()
	if q.E <= q.Qw {
		t.Skip("need E > Qw for striping")
	}
	ledgerID := nextLedgerID()
	writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, q, c.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	const n = 24
	for i := 0; i < n; i++ {
		if _, err := writer.Append([]byte(fmt.Sprintf("s%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	for _, id := range c.ids {
		ids, err := c.bookies[id].ListEntries(ledgerID)
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) == 0 || len(ids) == n {
			t.Fatalf("%s unexpected full/empty list %v (E=%d Qw=%d)", id, ids, q.E, q.Qw)
		}
	}
	last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last != n-1 {
		t.Fatalf("recover %d %v", last, err)
	}
	closed, err := metas.Get(ledgerID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		entry, err := ledgerclient.ReadEntry(c.bookies, closed, uint64(i))
		if err != nil || string(entry.Payload) != fmt.Sprintf("s%d", i) {
			t.Fatalf("entry %d %+v %v", i, entry, err)
		}
	}
}
