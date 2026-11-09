package ledgerclient

import "testing"

func TestMemoryBookieGapsFenceAndRecoveryAdd(t *testing.T) {
	bookie := NewMemoryBookie()
	key := []byte("k")
	if err := bookie.CreateLedger(1, key); err != nil {
		t.Fatal(err)
	}
	if err := bookie.AddEntry(1, 0, -1, []byte("a"), key); err != nil {
		t.Fatal(err)
	}
	if err := bookie.AddEntry(1, 2, 0, []byte("c"), key); err != nil {
		t.Fatal(err)
	}
	if _, err := bookie.ReadEntry(1, 1); KindOf(err) != KindNoSuchEntry {
		t.Fatalf("gap: %v", err)
	}
	ids, err := bookie.ListEntries(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 0 || ids[1] != 2 {
		t.Fatalf("ids %v", ids)
	}
	lac, err := bookie.ReadLastConfirmed(1)
	if err != nil || lac != 0 {
		t.Fatalf("lac %d %v", lac, err)
	}
	if err := bookie.SetExplicitLAC(1, 2, key); err != nil {
		t.Fatal(err)
	}
	lac, err = bookie.ReadLastConfirmed(1)
	if err != nil || lac != 2 {
		t.Fatalf("explicit lac %d %v", lac, err)
	}
	if err := bookie.FenceLedger(1, key); err != nil {
		t.Fatal(err)
	}
	if err := bookie.AddEntry(1, 3, 2, []byte("x"), key); KindOf(err) != KindFenced {
		t.Fatalf("fenced add: %v", err)
	}
	if err := bookie.RecoveryAddEntry(1, 4, 2, []byte("r"), key); err != nil {
		t.Fatal(err)
	}
	entry, err := bookie.ReadEntry(1, 4)
	if err != nil || string(entry.Payload) != "r" {
		t.Fatalf("recovery add %+v %v", entry, err)
	}
}

func TestFenceUnknownLedgerBlocksLaterAdd(t *testing.T) {
	bookie := NewMemoryBookie()
	key := []byte("k")
	if err := bookie.FenceLedger(9, key); err != nil {
		t.Fatal(err)
	}
	if err := bookie.CreateLedger(9, key); err != nil {
		t.Fatal(err)
	}
	fenced, err := bookie.IsFenced(9)
	if err != nil || !fenced {
		t.Fatalf("fenced %v %v", fenced, err)
	}
	if err := bookie.AddEntry(9, 0, -1, []byte("a"), key); KindOf(err) != KindFenced {
		t.Fatalf("add after fence: %v", err)
	}
}
