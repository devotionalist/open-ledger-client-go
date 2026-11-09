package ledgerclient

// WriteSet returns the Qw bookies for entryID, starting at entryID % E.
func WriteSet(ensemble []BookieID, qw uint32, entryID uint64) ([]BookieID, error) {
	ensembleSize := len(ensemble)
	if ensembleSize == 0 || qw == 0 || int(qw) > ensembleSize {
		return nil, invalid("write set requires 1 <= Qw <= ensemble size")
	}
	start := int(entryID % uint64(ensembleSize))
	out := make([]BookieID, 0, qw)
	for offset := uint32(0); offset < qw; offset++ {
		out = append(out, ensemble[(start+int(offset))%ensembleSize])
	}
	return out, nil
}

// FragmentFor returns the fragment that owns entryID.
func FragmentFor(meta LedgerMeta, entryID uint64) (Fragment, error) {
	if len(meta.Fragments) == 0 {
		return Fragment{}, invalid("ledger has no fragments")
	}
	if entryID < meta.Fragments[0].FirstEntryID {
		return Fragment{}, invalid("entry id precedes first fragment")
	}
	chosen := meta.Fragments[0]
	for _, frag := range meta.Fragments {
		if frag.FirstEntryID <= entryID {
			chosen = frag
		} else {
			break
		}
	}
	return chosen, nil
}

// WriteQuorumCovered reports whether every write-quorum window contains at least (Qw-Qa)+1 fenced bookies.
func WriteQuorumCovered(ensemble []BookieID, quorum Quorum, fenced map[BookieID]struct{}) bool {
	if quorum.Validate() != nil || len(ensemble) != int(quorum.E) {
		return false
	}
	need := int(quorum.Qw - quorum.Qa + 1)
	ensembleSize := len(ensemble)
	for start := 0; start < ensembleSize; start++ {
		hits := 0
		for offset := uint32(0); offset < quorum.Qw; offset++ {
			if _, ok := fenced[ensemble[(start+int(offset))%ensembleSize]]; ok {
				hits++
			}
		}
		if hits < need {
			return false
		}
	}
	return true
}
