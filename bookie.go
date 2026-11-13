package ledgerclient

// Bookie is the blocking single-bookie surface (sync adapter over [BookieAsync]).
// Prefer [BookieAsync] / GRPCBookie Async* methods on hot paths; use Bookie at
// process edges and in-process tests.
//
// AddEntry and RecoveryAddEntry succeed only after that replica's journal is durable.
// Entry ids on one bookie may have gaps. The same payload is idempotent.
type Bookie interface {
	CreateLedger(ledgerID uint64, masterKey []byte) error
	AddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error
	RecoveryAddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error
	ReadEntry(ledgerID, entryID uint64) (Entry, error)
	ReadEntries(ledgerID, fromEntryID uint64, max int) ([]Entry, error)
	ListEntries(ledgerID uint64) ([]uint64, error)
	FenceLedger(ledgerID uint64, masterKey []byte) error
	SetExplicitLAC(ledgerID uint64, lac int64, masterKey []byte) error
	ReadLastConfirmed(ledgerID uint64) (int64, error)
	LastEntryID(ledgerID uint64) (int64, error)
	IsFenced(ledgerID uint64) (bool, error)
	DeleteLedger(ledgerID uint64) error
}
