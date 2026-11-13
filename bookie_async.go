package ledgerclient

import "context"

// Done is the completion callback for RPCs that return only success/failure.
// Invoked exactly once; must not block the Callback receive / CQ path.
type Done func(err error)

// EntryDone completes a single-entry read.
type EntryDone func(entry Entry, err error)

// EntriesDone completes a multi-entry read.
type EntriesDone func(entries []Entry, err error)

// IDsDone completes a list of entry ids.
type IDsDone func(ids []uint64, err error)

// Int64Done completes an int64 result (LAC / last entry id).
type Int64Done func(v int64, err error)

// BoolDone completes a bool result.
type BoolDone func(v bool, err error)

// BookieAsync is the Callback-API surface for one bookie.
//
// Callers schedule work and return immediately; completion arrives on the
// callback (gRPC C++ ClientUnaryReactor / OpenKV client-async equivalent).
// Do not perform further synchronous RPCs or sleep inside the callback.
type BookieAsync interface {
	CreateLedgerAsync(ctx context.Context, ledgerID uint64, masterKey []byte, done Done)
	AddEntryAsync(ctx context.Context, ledgerID, entryID uint64, lac int64, payload, masterKey []byte, done Done)
	RecoveryAddEntryAsync(ctx context.Context, ledgerID, entryID uint64, lac int64, payload, masterKey []byte, done Done)
	ReadEntryAsync(ctx context.Context, ledgerID, entryID uint64, done EntryDone)
	ReadEntriesAsync(ctx context.Context, ledgerID, fromEntryID uint64, max int, done EntriesDone)
	ListEntriesAsync(ctx context.Context, ledgerID uint64, done IDsDone)
	FenceLedgerAsync(ctx context.Context, ledgerID uint64, masterKey []byte, done Done)
	SetExplicitLACAsync(ctx context.Context, ledgerID uint64, lac int64, masterKey []byte, done Done)
	ReadLastConfirmedAsync(ctx context.Context, ledgerID uint64, done Int64Done)
	LastEntryIDAsync(ctx context.Context, ledgerID uint64, done Int64Done)
	IsFencedAsync(ctx context.Context, ledgerID uint64, done BoolDone)
	DeleteLedgerAsync(ctx context.Context, ledgerID uint64, done Done)
}

// SyncBookie adapts BookieAsync to the blocking [Bookie] interface.
// Prefer BookieAsync on hot paths; use SyncBookie only at process edges.
type SyncBookie struct {
	Inner BookieAsync
}

// NewSyncBookie wraps an async bookie as a blocking Bookie.
func NewSyncBookie(inner BookieAsync) *SyncBookie {
	return &SyncBookie{Inner: inner}
}

func (s *SyncBookie) wait(run func(done Done)) error {
	ch := make(chan error, 1)
	run(func(err error) { ch <- err })
	return <-ch
}

func (s *SyncBookie) CreateLedger(ledgerID uint64, masterKey []byte) error {
	return s.wait(func(done Done) {
		s.Inner.CreateLedgerAsync(context.Background(), ledgerID, masterKey, done)
	})
}

func (s *SyncBookie) AddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error {
	return s.wait(func(done Done) {
		s.Inner.AddEntryAsync(context.Background(), ledgerID, entryID, lac, payload, masterKey, done)
	})
}

func (s *SyncBookie) RecoveryAddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error {
	return s.wait(func(done Done) {
		s.Inner.RecoveryAddEntryAsync(context.Background(), ledgerID, entryID, lac, payload, masterKey, done)
	})
}

func (s *SyncBookie) ReadEntry(ledgerID, entryID uint64) (Entry, error) {
	ch := make(chan struct {
		e   Entry
		err error
	}, 1)
	s.Inner.ReadEntryAsync(context.Background(), ledgerID, entryID, func(e Entry, err error) {
		ch <- struct {
			e   Entry
			err error
		}{e, err}
	})
	r := <-ch
	return r.e, r.err
}

func (s *SyncBookie) ReadEntries(ledgerID, fromEntryID uint64, max int) ([]Entry, error) {
	ch := make(chan struct {
		e   []Entry
		err error
	}, 1)
	s.Inner.ReadEntriesAsync(context.Background(), ledgerID, fromEntryID, max, func(e []Entry, err error) {
		ch <- struct {
			e   []Entry
			err error
		}{e, err}
	})
	r := <-ch
	return r.e, r.err
}

func (s *SyncBookie) ListEntries(ledgerID uint64) ([]uint64, error) {
	ch := make(chan struct {
		ids []uint64
		err error
	}, 1)
	s.Inner.ListEntriesAsync(context.Background(), ledgerID, func(ids []uint64, err error) {
		ch <- struct {
			ids []uint64
			err error
		}{ids, err}
	})
	r := <-ch
	return r.ids, r.err
}

func (s *SyncBookie) FenceLedger(ledgerID uint64, masterKey []byte) error {
	return s.wait(func(done Done) {
		s.Inner.FenceLedgerAsync(context.Background(), ledgerID, masterKey, done)
	})
}

func (s *SyncBookie) SetExplicitLAC(ledgerID uint64, lac int64, masterKey []byte) error {
	return s.wait(func(done Done) {
		s.Inner.SetExplicitLACAsync(context.Background(), ledgerID, lac, masterKey, done)
	})
}

func (s *SyncBookie) ReadLastConfirmed(ledgerID uint64) (int64, error) {
	ch := make(chan struct {
		v   int64
		err error
	}, 1)
	s.Inner.ReadLastConfirmedAsync(context.Background(), ledgerID, func(v int64, err error) {
		ch <- struct {
			v   int64
			err error
		}{v, err}
	})
	r := <-ch
	return r.v, r.err
}

func (s *SyncBookie) LastEntryID(ledgerID uint64) (int64, error) {
	ch := make(chan struct {
		v   int64
		err error
	}, 1)
	s.Inner.LastEntryIDAsync(context.Background(), ledgerID, func(v int64, err error) {
		ch <- struct {
			v   int64
			err error
		}{v, err}
	})
	r := <-ch
	return r.v, r.err
}

func (s *SyncBookie) IsFenced(ledgerID uint64) (bool, error) {
	ch := make(chan struct {
		v   bool
		err error
	}, 1)
	s.Inner.IsFencedAsync(context.Background(), ledgerID, func(v bool, err error) {
		ch <- struct {
			v   bool
			err error
		}{v, err}
	})
	r := <-ch
	return r.v, r.err
}

func (s *SyncBookie) DeleteLedger(ledgerID uint64) error {
	return s.wait(func(done Done) {
		s.Inner.DeleteLedgerAsync(context.Background(), ledgerID, done)
	})
}

// AsyncFromSync adapts a blocking Bookie to BookieAsync (callback on a new goroutine).
func AsyncFromSync(b Bookie) BookieAsync {
	return syncAsAsync{b: b}
}

type syncAsAsync struct{ b Bookie }

func (a syncAsAsync) CreateLedgerAsync(ctx context.Context, ledgerID uint64, masterKey []byte, done Done) {
	go func() {
		select {
		case <-ctx.Done():
			done(ctx.Err())
		default:
			done(a.b.CreateLedger(ledgerID, masterKey))
		}
	}()
}

func (a syncAsAsync) AddEntryAsync(ctx context.Context, ledgerID, entryID uint64, lac int64, payload, masterKey []byte, done Done) {
	go func() {
		select {
		case <-ctx.Done():
			done(ctx.Err())
		default:
			done(a.b.AddEntry(ledgerID, entryID, lac, payload, masterKey))
		}
	}()
}

func (a syncAsAsync) RecoveryAddEntryAsync(ctx context.Context, ledgerID, entryID uint64, lac int64, payload, masterKey []byte, done Done) {
	go func() {
		select {
		case <-ctx.Done():
			done(ctx.Err())
		default:
			done(a.b.RecoveryAddEntry(ledgerID, entryID, lac, payload, masterKey))
		}
	}()
}

func (a syncAsAsync) ReadEntryAsync(ctx context.Context, ledgerID, entryID uint64, done EntryDone) {
	go func() {
		select {
		case <-ctx.Done():
			done(Entry{}, ctx.Err())
		default:
			e, err := a.b.ReadEntry(ledgerID, entryID)
			done(e, err)
		}
	}()
}

func (a syncAsAsync) ReadEntriesAsync(ctx context.Context, ledgerID, fromEntryID uint64, max int, done EntriesDone) {
	go func() {
		select {
		case <-ctx.Done():
			done(nil, ctx.Err())
		default:
			e, err := a.b.ReadEntries(ledgerID, fromEntryID, max)
			done(e, err)
		}
	}()
}

func (a syncAsAsync) ListEntriesAsync(ctx context.Context, ledgerID uint64, done IDsDone) {
	go func() {
		select {
		case <-ctx.Done():
			done(nil, ctx.Err())
		default:
			ids, err := a.b.ListEntries(ledgerID)
			done(ids, err)
		}
	}()
}

func (a syncAsAsync) FenceLedgerAsync(ctx context.Context, ledgerID uint64, masterKey []byte, done Done) {
	go func() {
		select {
		case <-ctx.Done():
			done(ctx.Err())
		default:
			done(a.b.FenceLedger(ledgerID, masterKey))
		}
	}()
}

func (a syncAsAsync) SetExplicitLACAsync(ctx context.Context, ledgerID uint64, lac int64, masterKey []byte, done Done) {
	go func() {
		select {
		case <-ctx.Done():
			done(ctx.Err())
		default:
			done(a.b.SetExplicitLAC(ledgerID, lac, masterKey))
		}
	}()
}

func (a syncAsAsync) ReadLastConfirmedAsync(ctx context.Context, ledgerID uint64, done Int64Done) {
	go func() {
		select {
		case <-ctx.Done():
			done(0, ctx.Err())
		default:
			v, err := a.b.ReadLastConfirmed(ledgerID)
			done(v, err)
		}
	}()
}

func (a syncAsAsync) LastEntryIDAsync(ctx context.Context, ledgerID uint64, done Int64Done) {
	go func() {
		select {
		case <-ctx.Done():
			done(0, ctx.Err())
		default:
			v, err := a.b.LastEntryID(ledgerID)
			done(v, err)
		}
	}()
}

func (a syncAsAsync) IsFencedAsync(ctx context.Context, ledgerID uint64, done BoolDone) {
	go func() {
		select {
		case <-ctx.Done():
			done(false, ctx.Err())
		default:
			v, err := a.b.IsFenced(ledgerID)
			done(v, err)
		}
	}()
}

func (a syncAsAsync) DeleteLedgerAsync(ctx context.Context, ledgerID uint64, done Done) {
	go func() {
		select {
		case <-ctx.Done():
			done(ctx.Err())
		default:
			done(a.b.DeleteLedger(ledgerID))
		}
	}()
}
