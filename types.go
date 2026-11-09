package ledgerclient

// LedgerID is a cluster-wide ledger identifier.
type LedgerID = uint64

// EntryID is an entry id within a ledger. One bookie may store a non-contiguous subset.
type EntryID = uint64

// BookieID is a bookie identity.
type BookieID = string

// Quorum is ensemble size, write quorum, and ack quorum.
type Quorum struct {
	E  uint32
	Qw uint32
	Qa uint32
}

// QuorumOne is the single-replica quorum.
var QuorumOne = Quorum{E: 1, Qw: 1, Qa: 1}

// Validate checks E >= Qw >= Qa >= 1.
func (q Quorum) Validate() error {
	if q.E == 0 || q.Qw == 0 || q.Qa == 0 {
		return invalid("E/Qw/Qa must be >= 1")
	}
	if q.E < q.Qw || q.Qw < q.Qa {
		return invalid("require E >= Qw >= Qa")
	}
	return nil
}

// Fragment is one ensemble generation. Historical fragments are never rewritten.
type Fragment struct {
	FirstEntryID uint64
	Ensemble     []BookieID
}

// MetaState is the authoritative ledger state stored in metadata.
type MetaState int

const (
	// StateOpen accepts adds from the current writer.
	StateOpen MetaState = iota
	// StateInRecovery means fencing and tail recovery are in progress.
	StateInRecovery
	// StateClosed means the tail is sealed.
	StateClosed
)

// Entry is one ledger record.
type Entry struct {
	LedgerID uint64
	EntryID  uint64
	LAC      int64
	Payload  []byte
}
