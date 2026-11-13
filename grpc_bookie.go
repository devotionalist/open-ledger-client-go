package ledgerclient

import (
	"context"
	"fmt"
	"time"

	"github.com/devotionalist/open-ledger-client-go/internal/bookiepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// GRPCBookie talks to one openledgerd over gRPC using the Callback API:
// Async* methods schedule the unary RPC and invoke the completion callback;
// the blocking [Bookie] methods are thin Sync adapters for existing callers.
type GRPCBookie struct {
	client  bookiepb.BookieClient
	conn    *grpc.ClientConn
	timeout time.Duration
}

// DialBookie connects to an openledgerd Bookie service (Callback / async reaction).
func DialBookie(ctx context.Context, addr string) (*GRPCBookie, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial bookie %s: %w", addr, err)
	}
	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil, ctx.Err()
	default:
	}
	return &GRPCBookie{
		client:  bookiepb.NewBookieClient(conn),
		conn:    conn,
		timeout: 10 * time.Second,
	}, nil
}

// Close closes the underlying gRPC connection.
func (b *GRPCBookie) Close() error {
	if b.conn == nil {
		return nil
	}
	return b.conn.Close()
}

func (b *GRPCBookie) rpcCtx(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, b.timeout)
}

func mapRPC(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	for _, any := range st.Proto().GetDetails() {
		be := &bookiepb.BookieError{}
		if err := proto.Unmarshal(any.GetValue(), be); err != nil {
			continue
		}
		if be.Kind == bookiepb.BookieError_KIND_UNSPECIFIED && be.Message == "" {
			continue
		}
		return errKind(kindFromPB(be.Kind), be.LedgerId, be.EntryId, firstNonEmpty(be.Message, st.Message()))
	}
	return fmt.Errorf("bookie rpc: %s", st.Message())
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func kindFromPB(k bookiepb.BookieError_Kind) Kind {
	switch k {
	case bookiepb.BookieError_KIND_ALREADY_EXISTS:
		return KindAlreadyExists
	case bookiepb.BookieError_KIND_NOT_FOUND:
		return KindNotFound
	case bookiepb.BookieError_KIND_NO_SUCH_ENTRY:
		return KindNoSuchEntry
	case bookiepb.BookieError_KIND_CONFLICT:
		return KindConflict
	case bookiepb.BookieError_KIND_FENCED:
		return KindFenced
	case bookiepb.BookieError_KIND_UNAUTHORIZED:
		return KindUnauthorized
	case bookiepb.BookieError_KIND_VERSION_CONFLICT:
		return KindVersionConflict
	case bookiepb.BookieError_KIND_META_CONFLICT:
		return KindMetaConflict
	case bookiepb.BookieError_KIND_NOT_ENOUGH_BOOKIES:
		return KindNotEnoughBookies
	case bookiepb.BookieError_KIND_INVALID_ARGUMENT:
		return KindInvalidArgument
	default:
		return KindInvalidArgument
	}
}

func entryFromPB(e *bookiepb.Entry) Entry {
	if e == nil {
		return Entry{}
	}
	return Entry{
		LedgerID: e.LedgerId,
		EntryID:  e.EntryId,
		LAC:      e.Lac,
		Payload:  append([]byte(nil), e.Payload...),
	}
}

func (b *GRPCBookie) goRPC(parent context.Context, call func(ctx context.Context) error, done Done) {
	go func() {
		ctx, cancel := b.rpcCtx(parent)
		defer cancel()
		done(mapRPC(call(ctx)))
	}()
}

// --- BookieAsync (Callback API) ---

func (b *GRPCBookie) CreateLedgerAsync(ctx context.Context, ledgerID uint64, masterKey []byte, done Done) {
	b.goRPC(ctx, func(c context.Context) error {
		_, err := b.client.CreateLedger(c, &bookiepb.CreateLedgerRequest{
			LedgerId:  ledgerID,
			MasterKey: masterKey,
		})
		return err
	}, done)
}

func (b *GRPCBookie) AddEntryAsync(ctx context.Context, ledgerID, entryID uint64, lac int64, payload, masterKey []byte, done Done) {
	b.goRPC(ctx, func(c context.Context) error {
		_, err := b.client.AddEntry(c, &bookiepb.AddEntryRequest{
			LedgerId:  ledgerID,
			EntryId:   entryID,
			Lac:       lac,
			Payload:   payload,
			MasterKey: masterKey,
		})
		return err
	}, done)
}

func (b *GRPCBookie) RecoveryAddEntryAsync(ctx context.Context, ledgerID, entryID uint64, lac int64, payload, masterKey []byte, done Done) {
	b.goRPC(ctx, func(c context.Context) error {
		_, err := b.client.RecoveryAddEntry(c, &bookiepb.RecoveryAddEntryRequest{
			LedgerId:  ledgerID,
			EntryId:   entryID,
			Lac:       lac,
			Payload:   payload,
			MasterKey: masterKey,
		})
		return err
	}, done)
}

func (b *GRPCBookie) ReadEntryAsync(ctx context.Context, ledgerID, entryID uint64, done EntryDone) {
	go func() {
		c, cancel := b.rpcCtx(ctx)
		defer cancel()
		resp, err := b.client.ReadEntry(c, &bookiepb.ReadEntryRequest{
			LedgerId: ledgerID,
			EntryId:  entryID,
		})
		if err != nil {
			done(Entry{}, mapRPC(err))
			return
		}
		done(entryFromPB(resp.Entry), nil)
	}()
}

func (b *GRPCBookie) ReadEntriesAsync(ctx context.Context, ledgerID, fromEntryID uint64, max int, done EntriesDone) {
	go func() {
		c, cancel := b.rpcCtx(ctx)
		defer cancel()
		resp, err := b.client.ReadEntries(c, &bookiepb.ReadEntriesRequest{
			LedgerId:    ledgerID,
			FromEntryId: fromEntryID,
			Max:         uint32(max),
		})
		if err != nil {
			done(nil, mapRPC(err))
			return
		}
		out := make([]Entry, 0, len(resp.Entries))
		for _, e := range resp.Entries {
			out = append(out, entryFromPB(e))
		}
		done(out, nil)
	}()
}

func (b *GRPCBookie) ListEntriesAsync(ctx context.Context, ledgerID uint64, done IDsDone) {
	go func() {
		c, cancel := b.rpcCtx(ctx)
		defer cancel()
		resp, err := b.client.ListEntries(c, &bookiepb.ListEntriesRequest{LedgerId: ledgerID})
		if err != nil {
			done(nil, mapRPC(err))
			return
		}
		done(append([]uint64(nil), resp.EntryIds...), nil)
	}()
}

func (b *GRPCBookie) FenceLedgerAsync(ctx context.Context, ledgerID uint64, masterKey []byte, done Done) {
	b.goRPC(ctx, func(c context.Context) error {
		_, err := b.client.FenceLedger(c, &bookiepb.FenceLedgerRequest{
			LedgerId:  ledgerID,
			MasterKey: masterKey,
		})
		return err
	}, done)
}

func (b *GRPCBookie) SetExplicitLACAsync(ctx context.Context, ledgerID uint64, lac int64, masterKey []byte, done Done) {
	b.goRPC(ctx, func(c context.Context) error {
		_, err := b.client.SetExplicitLac(c, &bookiepb.SetExplicitLacRequest{
			LedgerId:  ledgerID,
			Lac:       lac,
			MasterKey: masterKey,
		})
		return err
	}, done)
}

func (b *GRPCBookie) ReadLastConfirmedAsync(ctx context.Context, ledgerID uint64, done Int64Done) {
	go func() {
		c, cancel := b.rpcCtx(ctx)
		defer cancel()
		resp, err := b.client.ReadLastConfirmed(c, &bookiepb.ReadLastConfirmedRequest{LedgerId: ledgerID})
		if err != nil {
			done(0, mapRPC(err))
			return
		}
		done(resp.Lac, nil)
	}()
}

func (b *GRPCBookie) LastEntryIDAsync(ctx context.Context, ledgerID uint64, done Int64Done) {
	go func() {
		c, cancel := b.rpcCtx(ctx)
		defer cancel()
		resp, err := b.client.LastEntryId(c, &bookiepb.LastEntryIdRequest{LedgerId: ledgerID})
		if err != nil {
			done(0, mapRPC(err))
			return
		}
		done(resp.EntryId, nil)
	}()
}

func (b *GRPCBookie) IsFencedAsync(ctx context.Context, ledgerID uint64, done BoolDone) {
	go func() {
		c, cancel := b.rpcCtx(ctx)
		defer cancel()
		resp, err := b.client.IsFenced(c, &bookiepb.IsFencedRequest{LedgerId: ledgerID})
		if err != nil {
			done(false, mapRPC(err))
			return
		}
		done(resp.Fenced, nil)
	}()
}

func (b *GRPCBookie) DeleteLedgerAsync(ctx context.Context, ledgerID uint64, done Done) {
	b.goRPC(ctx, func(c context.Context) error {
		_, err := b.client.DeleteLedger(c, &bookiepb.DeleteLedgerRequest{LedgerId: ledgerID})
		return err
	}, done)
}

// --- Bookie sync adapter (blocks until Callback finishes) ---

func (b *GRPCBookie) sync() *SyncBookie { return NewSyncBookie(b) }

func (b *GRPCBookie) CreateLedger(ledgerID uint64, masterKey []byte) error {
	return b.sync().CreateLedger(ledgerID, masterKey)
}

func (b *GRPCBookie) AddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error {
	return b.sync().AddEntry(ledgerID, entryID, lac, payload, masterKey)
}

func (b *GRPCBookie) RecoveryAddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error {
	return b.sync().RecoveryAddEntry(ledgerID, entryID, lac, payload, masterKey)
}

func (b *GRPCBookie) ReadEntry(ledgerID, entryID uint64) (Entry, error) {
	return b.sync().ReadEntry(ledgerID, entryID)
}

func (b *GRPCBookie) ReadEntries(ledgerID, fromEntryID uint64, max int) ([]Entry, error) {
	return b.sync().ReadEntries(ledgerID, fromEntryID, max)
}

func (b *GRPCBookie) ListEntries(ledgerID uint64) ([]uint64, error) {
	return b.sync().ListEntries(ledgerID)
}

func (b *GRPCBookie) FenceLedger(ledgerID uint64, masterKey []byte) error {
	return b.sync().FenceLedger(ledgerID, masterKey)
}

func (b *GRPCBookie) SetExplicitLAC(ledgerID uint64, lac int64, masterKey []byte) error {
	return b.sync().SetExplicitLAC(ledgerID, lac, masterKey)
}

func (b *GRPCBookie) ReadLastConfirmed(ledgerID uint64) (int64, error) {
	return b.sync().ReadLastConfirmed(ledgerID)
}

func (b *GRPCBookie) LastEntryID(ledgerID uint64) (int64, error) {
	return b.sync().LastEntryID(ledgerID)
}

func (b *GRPCBookie) IsFenced(ledgerID uint64) (bool, error) {
	return b.sync().IsFenced(ledgerID)
}

func (b *GRPCBookie) DeleteLedger(ledgerID uint64) error {
	return b.sync().DeleteLedger(ledgerID)
}
