//go:build integration

package integration_test

import (
	"fmt"
	"testing"

	ledgerclient "github.com/devotionalist/open-ledger-client-go"
)

// R01: confirmed entries survive recover; second recover idempotent; all fenced.
func TestR01_RecoverConfirmedTriple(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	const n = 64
	for i := 0; i < n; i++ {
		if _, err := writer.Append([]byte(fmt.Sprintf("r01-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last != n-1 {
		t.Fatalf("recover %d %v", last, err)
	}
	last2, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last2 != n-1 {
		t.Fatalf("recover2 %d %v", last2, err)
	}
	assertClosedReadable(t, metas, c.bookies, ledgerID, n, "r01-%d")
	assertAllFenced(t, c, ledgerID)
}

// R02: write → kill one bookie → Ready → recover with full ensemble.
func TestR02_RecoverAfterBookiePodKill(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	if c.ports[0] == 0 {
		t.Skip("pod-delete fault needs kubectl port-forward mode")
	}
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	const n = 48
	for i := 0; i < n; i++ {
		if _, err := writer.Append([]byte(fmt.Sprintf("r02-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	c.deletePod(t, "bookie-0")

	last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last != n-1 {
		t.Fatalf("recover after kill %d %v", last, err)
	}
	assertClosedReadable(t, metas, c.bookies, ledgerID, n, "r02-%d")
}

// R03: with one ensemble member excluded, keep appending (Qa=2); then recover on the full set.
func TestR03_WriteDuringOutageThenRecover(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, c.quorumCluster(), c.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	const before = 20
	for i := 0; i < before; i++ {
		if _, err := writer.Append([]byte(fmt.Sprintf("r03-%d", i))); err != nil {
			t.Fatalf("pre-append %d: %v", i, err)
		}
	}

	meta := writer.Meta()
	live := c.live("bookie-0")
	lac := writer.LAC()
	const during = 30
	for i := before; i < before+during; i++ {
		payload := []byte(fmt.Sprintf("r03-%d", i))
		acks, err := ledgerclient.AppendEntry(live, meta, uint64(i), lac, payload)
		if err != nil {
			t.Fatalf("outage append %d: %v", i, err)
		}
		if acks < int(meta.Quorum.Qa) {
			t.Fatalf("outage append %d acks=%d", i, acks)
		}
		lac = int64(i)
	}

	if c.ports[0] != 0 {
		victim := "bookie-1"
		if len(c.ids) > 4 {
			victim = "bookie-4"
		}
		c.deletePod(t, victim)
	}

	last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	want := before + during - 1
	if err != nil || last != int64(want) {
		t.Fatalf("recover %d want %d err %v", last, want, err)
	}
	assertClosedReadable(t, metas, c.bookies, ledgerID, before+during, "r03-%d")
}

// R04: dense ensemble — recover with one member omitted → IN_RECOVERY; restore → CLOSED.
func TestR04_RecoverBlockedUntilEnsembleReturns(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	ids := c.denseIDs()
	bookies := c.denseBookies()
	writer, err := ledgerclient.Create(metas, bookies, ledgerID, c.quorumDense(), ids, key)
	if err != nil {
		t.Fatal(err)
	}
	const n = 16
	for i := 0; i < n; i++ {
		if _, err := writer.Append([]byte(fmt.Sprintf("r04-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	partial := make(map[ledgerclient.BookieID]ledgerclient.Bookie, len(bookies)-1)
	for id, b := range bookies {
		if id == ids[len(ids)-1] {
			continue
		}
		partial[id] = b
	}
	_, err = ledgerclient.RecoverLedger(metas, partial, ledgerID)
	if ledgerclient.KindOf(err) != ledgerclient.KindNotEnoughBookies {
		t.Fatalf("want NotEnoughBookies with one missing, got %v", err)
	}
	meta, err := metas.Get(ledgerID)
	if err != nil || meta.State != ledgerclient.StateInRecovery {
		t.Fatalf("want IN_RECOVERY, got %+v %v", meta, err)
	}

	last, err := ledgerclient.RecoverLedger(metas, bookies, ledgerID)
	if err != nil || last != n-1 {
		t.Fatalf("recover after restore %d %v", last, err)
	}
	assertClosedReadable(t, metas, bookies, ledgerID, n, "r04-%d")
}

// R05: dirty tail on dense E=Qw=3,Qa=2 repaired across the write set.
func TestR05_DirtyTailRepairTriple(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 3)
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	ids := c.denseIDs()
	bookies := c.denseBookies()
	meta, err := ledgerclient.CreateOpenLedger(metas, bookies, ledgerID, c.quorumDense(), ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerclient.AppendEntry(bookies, meta, 0, -1, []byte("r05-0")); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerclient.AppendEntry(bookies, meta, 1, 0, []byte("r05-1")); err != nil {
		t.Fatal(err)
	}
	if err := bookies[ids[0]].AddEntry(ledgerID, 2, 0, []byte("r05-dirty"), key); err != nil {
		t.Fatal(err)
	}
	last, err := ledgerclient.RecoverLedger(metas, bookies, ledgerID)
	if err != nil || last != 2 {
		t.Fatalf("recover %d %v", last, err)
	}
	for _, id := range ids {
		entry, err := bookies[id].ReadEntry(ledgerID, 2)
		if err != nil || string(entry.Payload) != "r05-dirty" {
			t.Fatalf("%s %+v %v", id, entry, err)
		}
	}
}

// R06: striped E=8,Qw=3 write, kill a bookie, restore, recover.
func TestR06_StripeRecoverAfterKill(t *testing.T) {
	c := openCluster(t)
	requireMinBookies(t, c, 5)
	if c.ports[0] == 0 {
		t.Skip("pod-delete fault needs kubectl port-forward mode")
	}
	q := c.quorumCluster()
	if q.E <= q.Qw {
		t.Skip("need E > Qw")
	}
	metas := ledgerclient.NewMemoryMetaStore()
	key := []byte("k8s-secret")
	ledgerID := nextLedgerID()
	writer, err := ledgerclient.Create(metas, c.bookies, ledgerID, q, c.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	const n = 36
	for i := 0; i < n; i++ {
		if _, err := writer.Append([]byte(fmt.Sprintf("r06-%d", i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	victim := c.ids[len(c.ids)-1]
	c.deletePod(t, victim)
	last, err := ledgerclient.RecoverLedger(metas, c.bookies, ledgerID)
	if err != nil || last != n-1 {
		t.Fatalf("recover %d %v", last, err)
	}
	assertClosedReadable(t, metas, c.bookies, ledgerID, n, "r06-%d")
}

func assertClosedReadable(
	t *testing.T,
	metas ledgerclient.LedgerMetaStore,
	bookies map[ledgerclient.BookieID]ledgerclient.Bookie,
	ledgerID uint64,
	n int,
	fmtPayload string,
) {
	t.Helper()
	meta, err := metas.Get(ledgerID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.State != ledgerclient.StateClosed || meta.LastEntryID != int64(n-1) {
		t.Fatalf("meta %+v", meta)
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf(fmtPayload, i)
		entry, err := ledgerclient.ReadEntry(bookies, meta, uint64(i))
		if err != nil || string(entry.Payload) != want {
			t.Fatalf("entry %d want %q got %+v err=%v", i, want, entry, err)
		}
	}
}

func assertAllFenced(t *testing.T, c *cluster, ledgerID uint64) {
	t.Helper()
	for _, id := range c.ids {
		fenced, err := c.bookies[id].IsFenced(ledgerID)
		if err != nil || !fenced {
			t.Fatalf("%s fenced=%v err=%v", id, fenced, err)
		}
	}
}
