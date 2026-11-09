package ledgerclient

import (
	"bytes"
	"sort"
	"sync"
)

type ledgerMem struct {
	masterKey   []byte
	fenced      bool
	explicitLAC int64
	entries     map[uint64]Entry
}

// MemoryBookie is an in-process bookie with the same add, fence, and recovery-add rules as open-ledger.
// It is not durable.
type MemoryBookie struct {
	mu      sync.Mutex
	ledgers map[uint64]*ledgerMem
}

// NewMemoryBookie returns an empty bookie.
func NewMemoryBookie() *MemoryBookie {
	return &MemoryBookie{ledgers: map[uint64]*ledgerMem{}}
}

func (b *MemoryBookie) CreateLedger(ledgerID uint64, masterKey []byte) error {
	if len(masterKey) == 0 {
		return invalid("master key must not be empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if led, ok := b.ledgers[ledgerID]; ok {
		if bytes.Equal(led.masterKey, masterKey) {
			return nil
		}
		return errKind(KindUnauthorized, ledgerID, 0, "")
	}
	b.ledgers[ledgerID] = newLedger(masterKey, false)
	return nil
}

func (b *MemoryBookie) AddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error {
	return b.add(ledgerID, entryID, lac, payload, masterKey, false)
}

func (b *MemoryBookie) RecoveryAddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error {
	return b.add(ledgerID, entryID, lac, payload, masterKey, true)
}

func (b *MemoryBookie) add(ledgerID, entryID uint64, lac int64, payload, masterKey []byte, recovery bool) error {
	if len(masterKey) == 0 {
		return invalid("master key must not be empty")
	}
	if lac < -1 {
		return invalid("lac must be >= -1")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		return errKind(KindNotFound, ledgerID, 0, "")
	}
	if !bytes.Equal(led.masterKey, masterKey) {
		return errKind(KindUnauthorized, ledgerID, 0, "")
	}
	if !recovery && led.fenced {
		return errKind(KindFenced, ledgerID, 0, "")
	}
	if existing, ok := led.entries[entryID]; ok {
		if !bytes.Equal(existing.Payload, payload) {
			return errKind(KindConflict, ledgerID, entryID, "")
		}
		if existing.LAC == lac {
			return nil
		}
	}
	led.entries[entryID] = Entry{
		LedgerID: ledgerID,
		EntryID:  entryID,
		LAC:      lac,
		Payload:  bytes.Clone(payload),
	}
	return nil
}

func (b *MemoryBookie) ReadEntry(ledgerID, entryID uint64) (Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		return Entry{}, errKind(KindNotFound, ledgerID, 0, "")
	}
	entry, ok := led.entries[entryID]
	if !ok {
		return Entry{}, errKind(KindNoSuchEntry, ledgerID, entryID, "")
	}
	entry.Payload = bytes.Clone(entry.Payload)
	return entry, nil
}

func (b *MemoryBookie) ReadEntries(ledgerID, fromEntryID uint64, max int) ([]Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		return nil, errKind(KindNotFound, ledgerID, 0, "")
	}
	ids := sortedIDs(led.entries)
	out := make([]Entry, 0)
	for _, id := range ids {
		if id < fromEntryID {
			continue
		}
		entry := led.entries[id]
		entry.Payload = bytes.Clone(entry.Payload)
		out = append(out, entry)
		if max >= 0 && len(out) >= max {
			break
		}
	}
	return out, nil
}

func (b *MemoryBookie) ListEntries(ledgerID uint64) ([]uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		return nil, errKind(KindNotFound, ledgerID, 0, "")
	}
	return sortedIDs(led.entries), nil
}

func (b *MemoryBookie) FenceLedger(ledgerID uint64, masterKey []byte) error {
	if len(masterKey) == 0 {
		return invalid("master key must not be empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		b.ledgers[ledgerID] = newLedger(masterKey, true)
		return nil
	}
	if !bytes.Equal(led.masterKey, masterKey) {
		return errKind(KindUnauthorized, ledgerID, 0, "")
	}
	led.fenced = true
	return nil
}

func (b *MemoryBookie) SetExplicitLAC(ledgerID uint64, lac int64, masterKey []byte) error {
	if len(masterKey) == 0 {
		return invalid("master key must not be empty")
	}
	if lac < -1 {
		return invalid("lac must be >= -1")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		return errKind(KindNotFound, ledgerID, 0, "")
	}
	if !bytes.Equal(led.masterKey, masterKey) {
		return errKind(KindUnauthorized, ledgerID, 0, "")
	}
	if led.fenced {
		return errKind(KindFenced, ledgerID, 0, "")
	}
	led.explicitLAC = lac
	return nil
}

func (b *MemoryBookie) ReadLastConfirmed(ledgerID uint64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		return 0, errKind(KindNotFound, ledgerID, 0, "")
	}
	piggy := int64(-1)
	var maxID uint64
	var have bool
	for id, entry := range led.entries {
		if !have || id > maxID {
			maxID = id
			piggy = entry.LAC
			have = true
		}
	}
	if led.explicitLAC > piggy {
		return led.explicitLAC, nil
	}
	return piggy, nil
}

func (b *MemoryBookie) LastEntryID(ledgerID uint64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		return 0, errKind(KindNotFound, ledgerID, 0, "")
	}
	var maxID uint64
	var have bool
	for id := range led.entries {
		if !have || id > maxID {
			maxID = id
			have = true
		}
	}
	if !have {
		return -1, nil
	}
	return int64(maxID), nil
}

func (b *MemoryBookie) IsFenced(ledgerID uint64) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	led, ok := b.ledgers[ledgerID]
	if !ok {
		return false, errKind(KindNotFound, ledgerID, 0, "")
	}
	return led.fenced, nil
}

func (b *MemoryBookie) DeleteLedger(ledgerID uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.ledgers[ledgerID]; !ok {
		return errKind(KindNotFound, ledgerID, 0, "")
	}
	delete(b.ledgers, ledgerID)
	return nil
}

func newLedger(masterKey []byte, fenced bool) *ledgerMem {
	return &ledgerMem{
		masterKey:   bytes.Clone(masterKey),
		fenced:      fenced,
		explicitLAC: -1,
		entries:     map[uint64]Entry{},
	}
}

func sortedIDs(entries map[uint64]Entry) []uint64 {
	ids := make([]uint64, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
