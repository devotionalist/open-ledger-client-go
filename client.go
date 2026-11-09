package ledgerclient

import "fmt"

const casAttempts = 16

// AdvanceLAC moves LastAddConfirmed only through a contiguous confirmed prefix.
func AdvanceLAC(lac int64, completed map[uint64]struct{}) int64 {
	var next uint64
	if lac >= 0 {
		next = uint64(lac) + 1
	}
	current := lac
	for {
		if _, ok := completed[next]; !ok {
			return current
		}
		current = int64(next)
		if next == ^uint64(0) {
			return current
		}
		next++
	}
}

// CreateOpenLedger writes metadata, then createLedger on every ensemble member.
func CreateOpenLedger(metas LedgerMetaStore, bookies map[BookieID]Bookie, ledgerID uint64, quorum Quorum, ensemble []BookieID, masterKey []byte) (LedgerMeta, error) {
	fresh, err := NewOpenLedger(ledgerID, quorum, ensemble, masterKey)
	if err != nil {
		return LedgerMeta{}, err
	}
	stored, err := metas.Create(fresh)
	if err != nil {
		if KindOf(err) != KindAlreadyExists {
			return LedgerMeta{}, err
		}
		existing, getErr := metas.Get(ledgerID)
		if getErr != nil {
			return LedgerMeta{}, getErr
		}
		if !sameParams(existing, fresh) {
			return LedgerMeta{}, errKind(KindMetaConflict, ledgerID, 0, fmt.Sprintf("ledger %d already exists with different parameters", ledgerID))
		}
		stored = existing
	}
	if err := CreateLedgerOnEnsemble(bookies, stored.Fragments[0].Ensemble, ledgerID, stored.MasterKey); err != nil {
		return LedgerMeta{}, err
	}
	return stored, nil
}

// CreateLedgerOnEnsemble calls createLedger on each bookie in ensemble.
func CreateLedgerOnEnsemble(bookies map[BookieID]Bookie, ensemble []BookieID, ledgerID uint64, masterKey []byte) error {
	for _, id := range ensemble {
		bookie, ok := bookies[id]
		if !ok {
			return errKind(KindNotEnoughBookies, ledgerID, 0, "")
		}
		if err := bookie.CreateLedger(ledgerID, masterKey); err != nil {
			return err
		}
	}
	return nil
}

// AppendEntry writes one entry to its write quorum. The result is the number of journal acks.
func AppendEntry(bookies map[BookieID]Bookie, meta LedgerMeta, entryID uint64, lac int64, payload []byte) (int, error) {
	if meta.State != StateOpen {
		return 0, invalid("append requires ledger metadata OPEN")
	}
	frag, err := FragmentFor(meta, entryID)
	if err != nil {
		return 0, err
	}
	targets, err := WriteSet(frag.Ensemble, meta.Quorum.Qw, entryID)
	if err != nil {
		return 0, err
	}
	acks := 0
	sawFence := false
	for _, id := range targets {
		bookie, ok := bookies[id]
		if !ok {
			continue
		}
		err := bookie.AddEntry(meta.LedgerID, entryID, lac, payload, meta.MasterKey)
		if err == nil {
			acks++
			continue
		}
		switch KindOf(err) {
		case KindFenced:
			sawFence = true
		case KindConflict, KindUnauthorized:
			return 0, err
		}
	}
	if acks >= int(meta.Quorum.Qa) {
		return acks, nil
	}
	if sawFence {
		return 0, errKind(KindFenced, meta.LedgerID, 0, "")
	}
	return 0, errKind(KindNotEnoughBookies, meta.LedgerID, 0, "")
}

// ReadEntry reads one entry from its write set. One matching copy is enough.
func ReadEntry(bookies map[BookieID]Bookie, meta LedgerMeta, entryID uint64) (Entry, error) {
	entry, ok, err := probeEntry(bookies, meta, entryID, false)
	if err != nil {
		return Entry{}, err
	}
	if !ok {
		return Entry{}, errKind(KindNoSuchEntry, meta.LedgerID, entryID, "")
	}
	return entry, nil
}

// ChangeEnsemble appends a fragment while metadata is OPEN.
// It does not create the ledger on the new bookies.
func ChangeEnsemble(metas LedgerMetaStore, ledgerID, firstEntryID uint64, ensemble []BookieID) (LedgerMeta, error) {
	for attempt := 0; attempt < casAttempts; attempt++ {
		meta, err := metas.Get(ledgerID)
		if err != nil {
			return LedgerMeta{}, err
		}
		if meta.State != StateOpen {
			return LedgerMeta{}, invalid("ensemble change requires ledger metadata OPEN")
		}
		if err := validateEnsemble(ensemble, meta.Quorum); err != nil {
			return LedgerMeta{}, err
		}
		if len(meta.Fragments) == 0 {
			return LedgerMeta{}, invalid("ledger has no fragments")
		}
		last := meta.Fragments[len(meta.Fragments)-1]
		if firstEntryID <= last.FirstEntryID {
			return LedgerMeta{}, invalid("new fragment must start after the current fragment")
		}
		next := meta.clone()
		next.Fragments = append(next.Fragments, Fragment{
			FirstEntryID: firstEntryID,
			Ensemble:     append([]BookieID(nil), ensemble...),
		})
		stored, err := metas.CompareAndSwap(meta.MetaVersion, next)
		if err == nil {
			return stored, nil
		}
		if KindOf(err) != KindVersionConflict {
			return LedgerMeta{}, err
		}
	}
	return LedgerMeta{}, errKind(KindMetaConflict, ledgerID, 0, fmt.Sprintf("ensemble change on %d did not converge", ledgerID))
}

// CloseLedger CAS-es metadata to CLOSED. It does not fence bookies.
func CloseLedger(metas LedgerMetaStore, ledgerID uint64, lastEntryID int64) (int64, error) {
	if lastEntryID < -1 {
		return 0, invalid("last_entry_id must be >= -1")
	}
	for attempt := 0; attempt < casAttempts; attempt++ {
		meta, err := metas.Get(ledgerID)
		if err != nil {
			return 0, err
		}
		switch meta.State {
		case StateClosed:
			if meta.LastEntryID == lastEntryID {
				return lastEntryID, nil
			}
			return 0, errKind(KindMetaConflict, ledgerID, 0, fmt.Sprintf("ledger %d closed at %d not %d", ledgerID, meta.LastEntryID, lastEntryID))
		case StateInRecovery:
			return 0, invalid("ledger is in recovery; finish RecoverLedger")
		case StateOpen:
			next := meta.clone()
			next.State = StateClosed
			next.LastEntryID = lastEntryID
			if _, err := metas.CompareAndSwap(meta.MetaVersion, next); err != nil {
				if KindOf(err) == KindVersionConflict {
					continue
				}
				return 0, err
			}
			return lastEntryID, nil
		}
	}
	return 0, errKind(KindMetaConflict, ledgerID, 0, fmt.Sprintf("close of %d did not converge", ledgerID))
}

// RecoverLedger fences until coverage, reads forward from the max LAC, repairs, then CAS-es CLOSED.
func RecoverLedger(metas LedgerMetaStore, bookies map[BookieID]Bookie, ledgerID uint64) (int64, error) {
	for attempt := 0; attempt < casAttempts; attempt++ {
		last, done, err := recoverOnce(metas, bookies, ledgerID)
		if err != nil {
			return 0, err
		}
		if done {
			return last, nil
		}
	}
	return 0, errKind(KindMetaConflict, ledgerID, 0, fmt.Sprintf("recover of %d did not converge", ledgerID))
}

func recoverOnce(metas LedgerMetaStore, bookies map[BookieID]Bookie, ledgerID uint64) (int64, bool, error) {
	meta, err := metas.Get(ledgerID)
	if err != nil {
		return 0, false, err
	}
	if meta.State == StateClosed {
		return meta.LastEntryID, true, nil
	}
	if meta.State == StateOpen {
		next := meta.clone()
		next.State = StateInRecovery
		if _, err := metas.CompareAndSwap(meta.MetaVersion, next); err != nil {
			if KindOf(err) == KindVersionConflict {
				return 0, false, nil
			}
			return 0, false, err
		}
	}

	meta, err = metas.Get(ledgerID)
	if err != nil {
		return 0, false, err
	}
	if meta.State == StateClosed {
		return meta.LastEntryID, true, nil
	}
	if err := fenceToCoverage(bookies, meta); err != nil {
		return 0, false, err
	}
	maxLAC, err := maxLAC(bookies, meta)
	if err != nil {
		return 0, false, err
	}
	last, err := scanTail(bookies, meta, maxLAC)
	if err != nil {
		return 0, false, err
	}

	meta, err = metas.Get(ledgerID)
	if err != nil {
		return 0, false, err
	}
	if meta.State == StateClosed {
		return meta.LastEntryID, true, nil
	}
	if meta.State != StateInRecovery {
		return 0, false, nil
	}
	next := meta.clone()
	next.State = StateClosed
	next.LastEntryID = last
	if _, err := metas.CompareAndSwap(meta.MetaVersion, next); err != nil {
		if KindOf(err) != KindVersionConflict {
			return 0, false, err
		}
		current, getErr := metas.Get(ledgerID)
		if getErr != nil {
			return 0, false, getErr
		}
		if current.State == StateClosed {
			return current.LastEntryID, true, nil
		}
		return 0, false, nil
	}
	return last, true, nil
}

func fenceToCoverage(bookies map[BookieID]Bookie, meta LedgerMeta) error {
	fenced := map[BookieID]struct{}{}
	seen := map[BookieID]struct{}{}
	for _, frag := range meta.Fragments {
		for _, id := range frag.Ensemble {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			bookie, ok := bookies[id]
			if !ok {
				continue
			}
			err := bookie.FenceLedger(meta.LedgerID, meta.MasterKey)
			if err == nil {
				fenced[id] = struct{}{}
				continue
			}
			if KindOf(err) == KindUnauthorized {
				return errKind(KindUnauthorized, meta.LedgerID, 0, "")
			}
		}
	}
	for _, frag := range meta.Fragments {
		if !WriteQuorumCovered(frag.Ensemble, meta.Quorum, fenced) {
			return errKind(KindNotEnoughBookies, meta.LedgerID, 0, "")
		}
	}
	return nil
}

func maxLAC(bookies map[BookieID]Bookie, meta LedgerMeta) (int64, error) {
	if len(meta.Fragments) == 0 {
		return 0, invalid("ledger has no fragments")
	}
	ensemble := meta.Fragments[len(meta.Fragments)-1].Ensemble
	heard := 0
	max := int64(-1)
	for _, id := range ensemble {
		bookie, ok := bookies[id]
		if !ok {
			continue
		}
		lac, err := bookie.ReadLastConfirmed(meta.LedgerID)
		if err == nil {
			heard++
			if lac > max {
				max = lac
			}
			continue
		}
		if KindOf(err) == KindNotFound {
			heard++
		}
	}
	if heard < int(meta.Quorum.Qa) {
		return 0, errKind(KindNotEnoughBookies, meta.LedgerID, 0, "")
	}
	if max < -1 {
		max = -1
	}
	return max, nil
}

func scanTail(bookies map[BookieID]Bookie, meta LedgerMeta, maxLAC int64) (int64, error) {
	last := maxLAC
	var entryID uint64
	if maxLAC >= 0 {
		entryID = uint64(maxLAC) + 1
	}
	for {
		if entryID > uint64(^uint64(0)>>1) {
			return 0, invalid("entry id exceeds i64")
		}
		// RecoveryReadsFence (BK #2614): fence each write-set member before the probe.
		entry, ok, err := probeEntry(bookies, meta, entryID, true)
		if err != nil {
			return 0, err
		}
		if !ok {
			return last, nil
		}
		if err := repair(bookies, meta, entry); err != nil {
			return 0, err
		}
		last = int64(entryID)
		if entryID == ^uint64(0) {
			return 0, invalid("entry id overflow")
		}
		entryID++
	}
}

// probeEntry returns (entry, true) when a copy is found.
// When fence is true, fence each contacted bookie before reading so a negative
// answer cannot later flip to a successful add (BK #2614 / RecoveryReadsFence).
func probeEntry(bookies map[BookieID]Bookie, meta LedgerMeta, entryID uint64, fence bool) (Entry, bool, error) {
	frag, err := FragmentFor(meta, entryID)
	if err != nil {
		return Entry{}, false, err
	}
	targets, err := WriteSet(frag.Ensemble, meta.Quorum.Qw, entryID)
	if err != nil {
		return Entry{}, false, err
	}
	var found Entry
	have := false
	anyDown := false
	anyAbsence := false
	for _, id := range targets {
		bookie, ok := bookies[id]
		if !ok {
			anyDown = true
			continue
		}
		if fence {
			if err := bookie.FenceLedger(meta.LedgerID, meta.MasterKey); err != nil {
				if KindOf(err) == KindUnauthorized {
					return Entry{}, false, err
				}
				anyDown = true
				continue
			}
		}
		entry, err := bookie.ReadEntry(meta.LedgerID, entryID)
		if err == nil {
			if have {
				if !bytesEqual(found.Payload, entry.Payload) {
					return Entry{}, false, errKind(KindConflict, meta.LedgerID, entryID, "")
				}
			} else {
				found = entry
				have = true
			}
			continue
		}
		switch KindOf(err) {
		case KindNoSuchEntry, KindNotFound:
			anyAbsence = true
		default:
			anyDown = true
		}
	}
	if have {
		return found, true, nil
	}
	if anyDown || !anyAbsence {
		return Entry{}, false, errKind(KindNotEnoughBookies, meta.LedgerID, 0, "")
	}
	return Entry{}, false, nil
}

func repair(bookies map[BookieID]Bookie, meta LedgerMeta, entry Entry) error {
	frag, err := FragmentFor(meta, entry.EntryID)
	if err != nil {
		return err
	}
	targets, err := WriteSet(frag.Ensemble, meta.Quorum.Qw, entry.EntryID)
	if err != nil {
		return err
	}
	acks := 0
	for _, id := range targets {
		bookie, ok := bookies[id]
		if !ok {
			continue
		}
		err := bookie.RecoveryAddEntry(meta.LedgerID, entry.EntryID, entry.LAC, entry.Payload, meta.MasterKey)
		if err == nil {
			acks++
			continue
		}
		if KindOf(err) == KindConflict {
			return err
		}
	}
	if acks < int(meta.Quorum.Qa) {
		return errKind(KindNotEnoughBookies, meta.LedgerID, 0, "")
	}
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Writer is a single writer. Each append waits for Qa before the next entry id is issued.
type Writer struct {
	metas   LedgerMetaStore
	bookies map[BookieID]Bookie
	meta    LedgerMeta
	next    uint64
	lac     int64
}

// Create opens a ledger and returns a writer positioned at entry 0.
func Create(metas LedgerMetaStore, bookies map[BookieID]Bookie, ledgerID uint64, quorum Quorum, ensemble []BookieID, masterKey []byte) (*Writer, error) {
	meta, err := CreateOpenLedger(metas, bookies, ledgerID, quorum, ensemble, masterKey)
	if err != nil {
		return nil, err
	}
	return &Writer{metas: metas, bookies: bookies, meta: meta, next: 0, lac: -1}, nil
}

// Meta returns the metadata this writer last observed.
func (w *Writer) Meta() LedgerMeta { return w.meta.clone() }

// LAC is the LastAddConfirmed this writer has released.
func (w *Writer) LAC() int64 { return w.lac }

// NextEntryID is the next entry id that will be assigned.
func (w *Writer) NextEntryID() uint64 { return w.next }

// Append writes one payload. The entry piggybacks the previous LAC.
func (w *Writer) Append(payload []byte) (uint64, error) {
	entryID := w.next
	if _, err := AppendEntry(w.bookies, w.meta, entryID, w.lac, payload); err != nil {
		return 0, err
	}
	if entryID == ^uint64(0) {
		return 0, invalid("entry id overflow")
	}
	w.next = entryID + 1
	w.lac = int64(entryID)
	return entryID, nil
}

// Read reads one entry through the current ensemble.
func (w *Writer) Read(entryID uint64) (Entry, error) {
	return ReadEntry(w.bookies, w.meta, entryID)
}

// ChangeEnsemble switches the ensemble at firstEntryID and creates the ledger on the new bookies.
// firstEntryID must equal NextEntryID so no issued entry is retargeted (BK #4924).
func (w *Writer) ChangeEnsemble(firstEntryID uint64, ensemble []BookieID) error {
	if firstEntryID != w.next {
		return invalid(fmt.Sprintf(
			"ensemble change firstEntryID %d must equal next %d (no pending/issued gap)",
			firstEntryID, w.next,
		))
	}
	meta, err := ChangeEnsemble(w.metas, w.meta.LedgerID, firstEntryID, ensemble)
	if err != nil {
		return err
	}
	newest := meta.Fragments[len(meta.Fragments)-1]
	if err := CreateLedgerOnEnsemble(w.bookies, newest.Ensemble, w.meta.LedgerID, w.meta.MasterKey); err != nil {
		return err
	}
	w.meta = meta
	return nil
}

// Close seals metadata at the writer's LAC.
func (w *Writer) Close() (int64, error) {
	return CloseLedger(w.metas, w.meta.LedgerID, w.lac)
}

// PipelineWriter tracks in-flight pipelined adds so ensemble changes cannot
// retarget entries already sent to the old write set (BK #4194 / #4924).
type PipelineWriter struct {
	metas     LedgerMetaStore
	bookies   map[BookieID]Bookie
	meta      LedgerMeta
	next      uint64
	lac       int64
	pending   map[uint64]struct{}
	completed map[uint64]struct{}
}

// CreatePipeline opens a ledger and returns a pipeline writer at entry 0.
func CreatePipeline(metas LedgerMetaStore, bookies map[BookieID]Bookie, ledgerID uint64, quorum Quorum, ensemble []BookieID, masterKey []byte) (*PipelineWriter, error) {
	meta, err := CreateOpenLedger(metas, bookies, ledgerID, quorum, ensemble, masterKey)
	if err != nil {
		return nil, err
	}
	return &PipelineWriter{
		metas:     metas,
		bookies:   bookies,
		meta:      meta,
		next:      0,
		lac:       -1,
		pending:   map[uint64]struct{}{},
		completed: map[uint64]struct{}{},
	}, nil
}

// PendingCount is the number of submitted but unconfirmed entries.
func (w *PipelineWriter) PendingCount() int { return len(w.pending) }

// LAC is the contiguous confirmed prefix.
func (w *PipelineWriter) LAC() int64 { return w.lac }

// NextEntryID is the next id that Submit will assign.
func (w *PipelineWriter) NextEntryID() uint64 { return w.next }

// Submit assigns an id, marks it pending, and sends the add.
func (w *PipelineWriter) Submit(payload []byte) (uint64, error) {
	entryID := w.next
	w.pending[entryID] = struct{}{}
	if entryID == ^uint64(0) {
		delete(w.pending, entryID)
		return 0, invalid("entry id overflow")
	}
	w.next = entryID + 1
	if _, err := AppendEntry(w.bookies, w.meta, entryID, w.lac, payload); err != nil {
		delete(w.pending, entryID)
		w.next = entryID
		return 0, err
	}
	return entryID, nil
}

// Confirm removes a pending entry and advances LAC contiguously.
func (w *PipelineWriter) Confirm(entryID uint64) (int64, error) {
	if _, ok := w.pending[entryID]; !ok {
		return 0, invalid(fmt.Sprintf("entry %d is not pending", entryID))
	}
	delete(w.pending, entryID)
	w.completed[entryID] = struct{}{}
	w.lac = AdvanceLAC(w.lac, w.completed)
	for id := range w.completed {
		if int64(id) <= w.lac {
			delete(w.completed, id)
		}
	}
	return w.lac, nil
}

// Append is submit+confirm.
func (w *PipelineWriter) Append(payload []byte) (uint64, error) {
	id, err := w.Submit(payload)
	if err != nil {
		return 0, err
	}
	if _, err := w.Confirm(id); err != nil {
		return 0, err
	}
	return id, nil
}

// ChangeEnsemble starts a new fragment at NextEntryID. Pending must be empty.
func (w *PipelineWriter) ChangeEnsemble(ensemble []BookieID) error {
	if len(w.pending) > 0 {
		return invalid(fmt.Sprintf(
			"ensemble change blocked by %d pending add(s); drain or abandon first",
			len(w.pending),
		))
	}
	meta, err := ChangeEnsemble(w.metas, w.meta.LedgerID, w.next, ensemble)
	if err != nil {
		return err
	}
	newest := meta.Fragments[len(meta.Fragments)-1]
	if err := CreateLedgerOnEnsemble(w.bookies, newest.Ensemble, w.meta.LedgerID, w.meta.MasterKey); err != nil {
		return err
	}
	w.meta = meta
	return nil
}
