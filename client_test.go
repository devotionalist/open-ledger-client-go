package ledgerclient

import (
	"fmt"
	"sync"
	"testing"
)

func TestLACDoesNotSkipHoles(t *testing.T) {
	done := map[uint64]struct{}{0: {}, 1: {}, 3: {}}
	if got := AdvanceLAC(-1, done); got != 1 {
		t.Fatalf("lac %d", got)
	}
	done[2] = struct{}{}
	if got := AdvanceLAC(1, done); got != 3 {
		t.Fatalf("lac %d", got)
	}
}

func TestWriterAppendsAndCloses(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	writer, err := Create(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := writer.Append([]byte("a"))
	if err != nil || id != 0 {
		t.Fatalf("append0 %d %v", id, err)
	}
	id, err = writer.Append([]byte("b"))
	if err != nil || id != 1 {
		t.Fatalf("append1 %d %v", id, err)
	}
	if writer.LAC() != 1 {
		t.Fatalf("lac %d", writer.LAC())
	}
	entry, err := writer.Read(1)
	if err != nil || string(entry.Payload) != "b" {
		t.Fatalf("read %+v %v", entry, err)
	}
	last, err := writer.Close()
	if err != nil || last != 1 {
		t.Fatalf("close %d %v", last, err)
	}
	meta, err := metas.Get(1)
	if err != nil || meta.State != StateClosed {
		t.Fatalf("state %+v %v", meta, err)
	}
}

func TestDirtyTailIsRepaired(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	meta, err := CreateOpenLedger(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendEntry(cluster.bookies, meta, 0, -1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendEntry(cluster.bookies, meta, 1, 0, []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.nodes[0].AddEntry(1, 2, 0, []byte("dirty"), []byte("k")); err != nil {
		t.Fatal(err)
	}
	last, err := RecoverLedger(metas, cluster.bookies, 1)
	if err != nil || last != 2 {
		t.Fatalf("recover %d %v", last, err)
	}
	for i, node := range cluster.nodes {
		entry, err := node.ReadEntry(1, 2)
		if err != nil || string(entry.Payload) != "dirty" {
			t.Fatalf("bookie %d %+v %v", i, entry, err)
		}
	}
}

func TestHoleStopsTheScan(t *testing.T) {
	cluster := newCluster(1)
	metas := NewMemoryMetaStore()
	meta, err := CreateOpenLedger(metas, cluster.bookies, 1, QuorumOne, cluster.ids, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendEntry(cluster.bookies, meta, 0, -1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.nodes[0].AddEntry(1, 5, 0, []byte("skip"), []byte("k")); err != nil {
		t.Fatal(err)
	}
	last, err := RecoverLedger(metas, cluster.bookies, 1)
	if err != nil || last != 0 {
		t.Fatalf("recover %d %v", last, err)
	}
}

func TestFencedQuorumBlocksOldWriter(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	key := []byte("k")
	meta, err := CreateOpenLedger(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendEntry(cluster.bookies, meta, 0, -1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.nodes[0].FenceLedger(1, key); err != nil {
		t.Fatal(err)
	}
	if err := cluster.nodes[1].FenceLedger(1, key); err != nil {
		t.Fatal(err)
	}
	_, err = AppendEntry(cluster.bookies, meta, 1, 0, []byte("zombie"))
	if KindOf(err) != KindFenced {
		t.Fatalf("zombie %v", err)
	}
	if _, err := cluster.nodes[2].ReadEntry(1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.nodes[0].ReadEntry(1, 1); err == nil {
		t.Fatal("fenced bookie stored the entry")
	}
}

func TestStriping(t *testing.T) {
	cluster := newCluster(5)
	metas := NewMemoryMetaStore()
	meta, err := CreateOpenLedger(metas, cluster.bookies, 1, Quorum{E: 5, Qw: 3, Qa: 2}, cluster.ids, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendEntry(cluster.bookies, meta, 0, -1, []byte("e0")); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendEntry(cluster.bookies, meta, 1, 0, []byte("e1")); err != nil {
		t.Fatal(err)
	}
	assertList(t, cluster.nodes[0], 0)
	assertList(t, cluster.nodes[1], 0, 1)
	assertList(t, cluster.nodes[3], 1)
	assertList(t, cluster.nodes[4])
	last, err := RecoverLedger(metas, cluster.bookies, 1)
	if err != nil || last != 1 {
		t.Fatalf("recover %d %v", last, err)
	}
}

func TestMissingBookiesLeaveRecoveryOpen(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	if _, err := CreateOpenLedger(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, []byte("k")); err != nil {
		t.Fatal(err)
	}
	_, err := RecoverLedger(metas, map[BookieID]Bookie{}, 1)
	if KindOf(err) != KindNotEnoughBookies {
		t.Fatalf("recover %v", err)
	}
	meta, err := metas.Get(1)
	if err != nil || meta.State != StateInRecovery {
		t.Fatalf("state %+v %v", meta, err)
	}
	last, err := RecoverLedger(metas, cluster.bookies, 1)
	if err != nil || last != -1 {
		t.Fatalf("retry %d %v", last, err)
	}
}

func TestTwoWritersTwoLedgers(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	key := []byte("k")
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ledgerID := uint64(100 + w)
			writer, err := Create(metas, cluster.bookies, ledgerID, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, key)
			if err != nil {
				errs <- err
				return
			}
			for i := 0; i < 10; i++ {
				id, err := writer.Append([]byte(fmt.Sprintf("w%d-%d", w, i)))
				if err != nil || id != uint64(i) {
					errs <- fmt.Errorf("w%d append %d: %d %v", w, i, id, err)
					return
				}
			}
			last, err := writer.Close()
			if err != nil || last != 9 {
				errs <- fmt.Errorf("w%d close %d %v", w, last, err)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestOldWriterBlockedAfterRecover(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	key := []byte("k")
	writer, err := Create(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append([]byte("a0")); err != nil {
		t.Fatal(err)
	}
	metaA := writer.Meta()
	last, err := RecoverLedger(metas, cluster.bookies, 1)
	if err != nil || last != 0 {
		t.Fatalf("recover %d %v", last, err)
	}
	_, err = AppendEntry(cluster.bookies, metaA, 1, 0, []byte("zombie"))
	if KindOf(err) != KindFenced {
		t.Fatalf("zombie %v", err)
	}
	entry, err := ReadEntry(cluster.bookies, metaA, 0)
	if err != nil || string(entry.Payload) != "a0" {
		t.Fatalf("read %+v %v", entry, err)
	}
}

func TestConcurrentRecoverConverges(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	meta, err := CreateOpenLedger(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendEntry(cluster.bookies, meta, 0, -1, []byte("x")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan int64, 4)
	errors := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last, err := RecoverLedger(metas, cluster.bookies, 1)
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
	for last := range results {
		if last != 0 {
			t.Fatalf("last %d", last)
		}
	}
	stored, err := metas.Get(1)
	if err != nil || stored.State != StateClosed || stored.LastEntryID != 0 {
		t.Fatalf("meta %+v %v", stored, err)
	}
}

func TestConfirmedEntriesSurviveRecovery(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	writer, err := Create(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := writer.Append([]byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	last, err := RecoverLedger(metas, cluster.bookies, 1)
	if err != nil || last != 3 {
		t.Fatalf("recover %d %v", last, err)
	}
	last2, err := RecoverLedger(metas, cluster.bookies, 1)
	if err != nil || last2 != 3 {
		t.Fatalf("second recover %d %v", last2, err)
	}
	for _, node := range cluster.nodes {
		if fenced, err := node.IsFenced(1); err != nil || !fenced {
			t.Fatalf("fenced %v %v", fenced, err)
		}
	}
}

func TestPipelineEnsembleChangeBlockedWhilePending(t *testing.T) {
	cluster := newCluster(4)
	metas := NewMemoryMetaStore()
	first := cluster.ids[:3]
	w, err := CreatePipeline(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, first, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	e0, err := w.Submit([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Confirm(e0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Submit([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if w.PendingCount() != 1 {
		t.Fatalf("pending %d", w.PendingCount())
	}
	next := cluster.ids[1:]
	if err := w.ChangeEnsemble(next); KindOf(err) != KindInvalidArgument {
		t.Fatalf("expected block, got %v", err)
	}
	if _, err := w.Confirm(1); err != nil {
		t.Fatal(err)
	}
	if err := w.ChangeEnsemble(next); err != nil {
		t.Fatal(err)
	}
	e2, err := w.Append([]byte("c"))
	if err != nil || e2 != 2 {
		t.Fatalf("append %d %v", e2, err)
	}
	if _, err := cluster.nodes[0].ReadEntry(1, 2); err == nil {
		t.Fatal("old bookie should not hold entry 2")
	}
	got, err := cluster.nodes[3].ReadEntry(1, 2)
	if err != nil || string(got.Payload) != "c" {
		t.Fatalf("new ensemble %v %v", got, err)
	}
}

// BK #2614: coverage can leave a bookie unfenced; recover must fence it before read.
func TestRecoveryReadsFenceUncoveredBookies(t *testing.T) {
	cluster := newCluster(3)
	metas := NewMemoryMetaStore()
	key := []byte("k")
	meta, err := CreateOpenLedger(metas, cluster.bookies, 1, Quorum{E: 3, Qw: 3, Qa: 2}, cluster.ids, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendEntry(cluster.bookies, meta, 0, -1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	// Qa=2 coverage needs 2 fences; leave bookie-2 unfenced.
	if err := cluster.nodes[0].FenceLedger(1, key); err != nil {
		t.Fatal(err)
	}
	if err := cluster.nodes[1].FenceLedger(1, key); err != nil {
		t.Fatal(err)
	}
	if fenced, err := cluster.nodes[2].IsFenced(1); err != nil || fenced {
		t.Fatalf("bookie-2 should start unfenced: %v %v", fenced, err)
	}

	last, err := RecoverLedger(metas, cluster.bookies, 1)
	if err != nil || last != 0 {
		t.Fatalf("recover %d %v", last, err)
	}
	if fenced, err := cluster.nodes[2].IsFenced(1); err != nil || !fenced {
		t.Fatalf("recovery probe must fence uncovered bookie: %v %v", fenced, err)
	}
	if err := cluster.nodes[2].AddEntry(1, 1, 0, []byte("late"), key); KindOf(err) != KindFenced {
		t.Fatalf("late add on uncovered bookie: %v", err)
	}
	stored, err := metas.Get(1)
	if err != nil || stored.State != StateClosed || stored.LastEntryID != 0 {
		t.Fatalf("meta %+v %v", stored, err)
	}
}

type cluster struct {
	nodes   []*MemoryBookie
	ids     []string
	bookies map[BookieID]Bookie
}

func newCluster(n int) cluster {
	c := cluster{
		nodes:   make([]*MemoryBookie, n),
		ids:     make([]string, n),
		bookies: map[BookieID]Bookie{},
	}
	for i := 0; i < n; i++ {
		c.nodes[i] = NewMemoryBookie()
		c.ids[i] = "b" + string(rune('0'+i))
		c.bookies[c.ids[i]] = c.nodes[i]
	}
	return c
}

func assertList(t *testing.T, bookie *MemoryBookie, want ...uint64) {
	t.Helper()
	got, err := bookie.ListEntries(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("list %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("list %v want %v", got, want)
		}
	}
}
