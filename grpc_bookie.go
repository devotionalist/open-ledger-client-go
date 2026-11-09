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

// GRPCBookie talks to one openledgerd over gRPC.
type GRPCBookie struct {
	client bookiepb.BookieClient
	conn   *grpc.ClientConn
	timeout time.Duration
}

// DialBookie connects to an openledgerd Bookie service.
func DialBookie(ctx context.Context, addr string) (*GRPCBookie, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial bookie %s: %w", addr, err)
	}
	// Touch the connection so dial failures surface before the first RPC when possible.
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

func (b *GRPCBookie) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), b.timeout)
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

func (b *GRPCBookie) CreateLedger(ledgerID uint64, masterKey []byte) error {
	ctx, cancel := b.ctx()
	defer cancel()
	_, err := b.client.CreateLedger(ctx, &bookiepb.CreateLedgerRequest{
		LedgerId:  ledgerID,
		MasterKey: masterKey,
	})
	return mapRPC(err)
}

func (b *GRPCBookie) AddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error {
	ctx, cancel := b.ctx()
	defer cancel()
	_, err := b.client.AddEntry(ctx, &bookiepb.AddEntryRequest{
		LedgerId:  ledgerID,
		EntryId:   entryID,
		Lac:       lac,
		Payload:   payload,
		MasterKey: masterKey,
	})
	return mapRPC(err)
}

func (b *GRPCBookie) RecoveryAddEntry(ledgerID, entryID uint64, lac int64, payload, masterKey []byte) error {
	ctx, cancel := b.ctx()
	defer cancel()
	_, err := b.client.RecoveryAddEntry(ctx, &bookiepb.RecoveryAddEntryRequest{
		LedgerId:  ledgerID,
		EntryId:   entryID,
		Lac:       lac,
		Payload:   payload,
		MasterKey: masterKey,
	})
	return mapRPC(err)
}

func (b *GRPCBookie) ReadEntry(ledgerID, entryID uint64) (Entry, error) {
	ctx, cancel := b.ctx()
	defer cancel()
	resp, err := b.client.ReadEntry(ctx, &bookiepb.ReadEntryRequest{
		LedgerId: ledgerID,
		EntryId:  entryID,
	})
	if err != nil {
		return Entry{}, mapRPC(err)
	}
	return entryFromPB(resp.Entry), nil
}

func (b *GRPCBookie) ReadEntries(ledgerID, fromEntryID uint64, max int) ([]Entry, error) {
	ctx, cancel := b.ctx()
	defer cancel()
	resp, err := b.client.ReadEntries(ctx, &bookiepb.ReadEntriesRequest{
		LedgerId:    ledgerID,
		FromEntryId: fromEntryID,
		Max:         uint32(max),
	})
	if err != nil {
		return nil, mapRPC(err)
	}
	out := make([]Entry, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		out = append(out, entryFromPB(e))
	}
	return out, nil
}

func (b *GRPCBookie) ListEntries(ledgerID uint64) ([]uint64, error) {
	ctx, cancel := b.ctx()
	defer cancel()
	resp, err := b.client.ListEntries(ctx, &bookiepb.ListEntriesRequest{LedgerId: ledgerID})
	if err != nil {
		return nil, mapRPC(err)
	}
	return append([]uint64(nil), resp.EntryIds...), nil
}

func (b *GRPCBookie) FenceLedger(ledgerID uint64, masterKey []byte) error {
	ctx, cancel := b.ctx()
	defer cancel()
	_, err := b.client.FenceLedger(ctx, &bookiepb.FenceLedgerRequest{
		LedgerId:  ledgerID,
		MasterKey: masterKey,
	})
	return mapRPC(err)
}

func (b *GRPCBookie) SetExplicitLAC(ledgerID uint64, lac int64, masterKey []byte) error {
	ctx, cancel := b.ctx()
	defer cancel()
	_, err := b.client.SetExplicitLac(ctx, &bookiepb.SetExplicitLacRequest{
		LedgerId:  ledgerID,
		Lac:       lac,
		MasterKey: masterKey,
	})
	return mapRPC(err)
}

func (b *GRPCBookie) ReadLastConfirmed(ledgerID uint64) (int64, error) {
	ctx, cancel := b.ctx()
	defer cancel()
	resp, err := b.client.ReadLastConfirmed(ctx, &bookiepb.ReadLastConfirmedRequest{LedgerId: ledgerID})
	if err != nil {
		return 0, mapRPC(err)
	}
	return resp.Lac, nil
}

func (b *GRPCBookie) LastEntryID(ledgerID uint64) (int64, error) {
	ctx, cancel := b.ctx()
	defer cancel()
	resp, err := b.client.LastEntryId(ctx, &bookiepb.LastEntryIdRequest{LedgerId: ledgerID})
	if err != nil {
		return 0, mapRPC(err)
	}
	return resp.EntryId, nil
}

func (b *GRPCBookie) IsFenced(ledgerID uint64) (bool, error) {
	ctx, cancel := b.ctx()
	defer cancel()
	resp, err := b.client.IsFenced(ctx, &bookiepb.IsFencedRequest{LedgerId: ledgerID})
	if err != nil {
		return false, mapRPC(err)
	}
	return resp.Fenced, nil
}

func (b *GRPCBookie) DeleteLedger(ledgerID uint64) error {
	ctx, cancel := b.ctx()
	defer cancel()
	_, err := b.client.DeleteLedger(ctx, &bookiepb.DeleteLedgerRequest{LedgerId: ledgerID})
	return mapRPC(err)
}
