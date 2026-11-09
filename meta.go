package ledgerclient

import (
	"bytes"
	"sync"
)

// LedgerMeta is authoritative ledger metadata.
type LedgerMeta struct {
	LedgerID    uint64
	Quorum      Quorum
	State       MetaState
	LastEntryID int64
	Fragments   []Fragment
	MasterKey   []byte
	MetaVersion uint64
}

// NewOpenLedger builds an OPEN ledger whose first fragment starts at entry 0.
func NewOpenLedger(ledgerID uint64, quorum Quorum, ensemble []BookieID, masterKey []byte) (LedgerMeta, error) {
	if err := quorum.Validate(); err != nil {
		return LedgerMeta{}, err
	}
	if len(masterKey) == 0 {
		return LedgerMeta{}, invalid("master key must not be empty")
	}
	if err := validateEnsemble(ensemble, quorum); err != nil {
		return LedgerMeta{}, err
	}
	return LedgerMeta{
		LedgerID:    ledgerID,
		Quorum:      quorum,
		State:       StateOpen,
		LastEntryID: -1,
		Fragments: []Fragment{{
			FirstEntryID: 0,
			Ensemble:     append([]BookieID(nil), ensemble...),
		}},
		MasterKey: bytes.Clone(masterKey),
	}, nil
}

func (m LedgerMeta) clone() LedgerMeta {
	out := m
	out.MasterKey = bytes.Clone(m.MasterKey)
	out.Fragments = make([]Fragment, len(m.Fragments))
	for i, frag := range m.Fragments {
		out.Fragments[i] = Fragment{
			FirstEntryID: frag.FirstEntryID,
			Ensemble:     append([]BookieID(nil), frag.Ensemble...),
		}
	}
	return out
}

func validateEnsemble(ensemble []BookieID, quorum Quorum) error {
	if len(ensemble) != int(quorum.E) {
		return invalid("ensemble length must equal E")
	}
	seen := map[string]struct{}{}
	for _, id := range ensemble {
		if id == "" {
			return invalid("ensemble bookie ids must be unique and non-empty")
		}
		if _, ok := seen[id]; ok {
			return invalid("ensemble bookie ids must be unique and non-empty")
		}
		seen[id] = struct{}{}
	}
	return nil
}

func sameParams(a, b LedgerMeta) bool {
	if a.Quorum != b.Quorum || !bytes.Equal(a.MasterKey, b.MasterKey) || len(a.Fragments) != len(b.Fragments) {
		return false
	}
	for i := range a.Fragments {
		if a.Fragments[i].FirstEntryID != b.Fragments[i].FirstEntryID {
			return false
		}
		if len(a.Fragments[i].Ensemble) != len(b.Fragments[i].Ensemble) {
			return false
		}
		for j := range a.Fragments[i].Ensemble {
			if a.Fragments[i].Ensemble[j] != b.Fragments[i].Ensemble[j] {
				return false
			}
		}
	}
	return true
}

// LedgerMetaStore is the CAS store for [LedgerMeta].
type LedgerMetaStore interface {
	Get(ledgerID uint64) (LedgerMeta, error)
	Create(meta LedgerMeta) (LedgerMeta, error)
	CompareAndSwap(expectedVersion uint64, meta LedgerMeta) (LedgerMeta, error)
}

// MemoryMetaStore is process-local metadata for tests and single-process embeds.
type MemoryMetaStore struct {
	mu      sync.Mutex
	ledgers map[uint64]LedgerMeta
}

// NewMemoryMetaStore returns an empty store.
func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{ledgers: map[uint64]LedgerMeta{}}
}

func (s *MemoryMetaStore) Get(ledgerID uint64) (LedgerMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.ledgers[ledgerID]
	if !ok {
		return LedgerMeta{}, errKind(KindNotFound, ledgerID, 0, "")
	}
	return meta.clone(), nil
}

func (s *MemoryMetaStore) Create(meta LedgerMeta) (LedgerMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ledgers[meta.LedgerID]; ok {
		return LedgerMeta{}, errKind(KindAlreadyExists, meta.LedgerID, 0, "")
	}
	stored := meta.clone()
	stored.MetaVersion = 1
	s.ledgers[meta.LedgerID] = stored.clone()
	return stored, nil
}

func (s *MemoryMetaStore) CompareAndSwap(expectedVersion uint64, meta LedgerMeta) (LedgerMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.ledgers[meta.LedgerID]
	if !ok {
		return LedgerMeta{}, errKind(KindNotFound, meta.LedgerID, 0, "")
	}
	if current.MetaVersion != expectedVersion {
		return LedgerMeta{}, errKind(KindVersionConflict, meta.LedgerID, 0, "")
	}
	stored := meta.clone()
	stored.MetaVersion = expectedVersion + 1
	s.ledgers[meta.LedgerID] = stored.clone()
	return stored, nil
}
