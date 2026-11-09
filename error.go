package ledgerclient

import (
	"errors"
	"fmt"
)

// Kind classifies a client or bookie failure.
type Kind int

const (
	// KindAlreadyExists means the ledger id is already in metadata.
	KindAlreadyExists Kind = iota + 1
	// KindNotFound means the ledger is absent.
	KindNotFound
	// KindNoSuchEntry means this bookie does not store the entry.
	KindNoSuchEntry
	// KindConflict means the same entry id was stored with a different payload.
	KindConflict
	// KindFenced means a normal add hit a fenced ledger.
	KindFenced
	// KindUnauthorized means the master key did not match.
	KindUnauthorized
	// KindVersionConflict means a metadata compare-and-swap lost the race.
	KindVersionConflict
	// KindMetaConflict means a metadata update was rejected.
	KindMetaConflict
	// KindNotEnoughBookies means quorum coverage or Qa was not reached.
	KindNotEnoughBookies
	// KindInvalidArgument means quorum, ensemble, key, or entry arguments were rejected.
	KindInvalidArgument
)

// OpError is a typed failure from metadata, quorum operations, or a bookie call.
type OpError struct {
	Kind     Kind
	LedgerID uint64
	EntryID  uint64
	Msg      string
}

func (e *OpError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return fmt.Sprintf("ledger %d: %s", e.LedgerID, e.Kind.String())
}

func (k Kind) String() string {
	switch k {
	case KindAlreadyExists:
		return "already exists"
	case KindNotFound:
		return "not found"
	case KindNoSuchEntry:
		return "no such entry"
	case KindConflict:
		return "payload conflict"
	case KindFenced:
		return "fenced"
	case KindUnauthorized:
		return "unauthorized"
	case KindVersionConflict:
		return "version conflict"
	case KindMetaConflict:
		return "metadata conflict"
	case KindNotEnoughBookies:
		return "not enough bookies"
	case KindInvalidArgument:
		return "invalid argument"
	default:
		return "unknown"
	}
}

// KindOf reports the kind of err, or 0 when err is not an [OpError].
func KindOf(err error) Kind {
	var op *OpError
	if errors.As(err, &op) {
		return op.Kind
	}
	return 0
}

func errKind(kind Kind, ledgerID, entryID uint64, msg string) error {
	return &OpError{Kind: kind, LedgerID: ledgerID, EntryID: entryID, Msg: msg}
}

func invalid(msg string) error {
	return errKind(KindInvalidArgument, 0, 0, "invalid argument: "+msg)
}
