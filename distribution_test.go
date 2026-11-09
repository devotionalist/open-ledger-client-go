package ledgerclient

import "testing"

func TestWriteSetStripes(t *testing.T) {
	ensemble := []string{"b0", "b1", "b2", "b3", "b4"}
	got, err := WriteSet(ensemble, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, got, "b0", "b1", "b2")
	got, err = WriteSet(ensemble, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, got, "b1", "b2", "b3")
	got, err = WriteSet(ensemble, 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, got, "b4", "b0", "b1")
}

func TestFenceCoverage(t *testing.T) {
	ensemble := []string{"b0", "b1", "b2"}
	q := Quorum{E: 3, Qw: 3, Qa: 2}
	two := map[string]struct{}{"b0": {}, "b1": {}}
	one := map[string]struct{}{"b0": {}}
	if !WriteQuorumCovered(ensemble, q, two) {
		t.Fatal("two fences should cover Qa=2")
	}
	if WriteQuorumCovered(ensemble, q, one) {
		t.Fatal("one fence should not cover Qa=2")
	}
	if !WriteQuorumCovered(ensemble, Quorum{E: 3, Qw: 3, Qa: 3}, one) {
		t.Fatal("Qa=Qw needs one fence")
	}
}

func assertIDs(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
