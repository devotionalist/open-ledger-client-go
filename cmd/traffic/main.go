// Continuous quorum append/read against openledgerd bookies.
//
// Used with local k8s fault injection (kill one bookie while Qa>=2 still holds).
// Metadata stays in-process (MemoryMetaStore); only the bookie data plane is remote.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	ledgerclient "github.com/devotionalist/open-ledger-client-go"
)

func main() {
	addrsFlag := flag.String("addrs", "", "comma-separated bookie host:port (required)")
	idsFlag := flag.String("ids", "bookie-0,bookie-1,bookie-2", "comma-separated bookie ids matching --addrs order")
	duration := flag.Duration("duration", 60*time.Second, "how long to run")
	reportEvery := flag.Duration("report", 2*time.Second, "stats interval")
	payloadSize := flag.Int("payload", 64, "append payload bytes")
	e := flag.Uint("e", 3, "ensemble size")
	qw := flag.Uint("qw", 3, "write quorum")
	qa := flag.Uint("qa", 2, "ack quorum")
	retry := flag.Duration("retry", 200*time.Millisecond, "backoff after NotEnoughBookies / transient errors")
	ledgerID := flag.Uint64("ledger", 0, "ledger id (0 = unix nanos)")
	flag.Parse()

	addrs := splitCSV(*addrsFlag)
	ids := splitCSV(*idsFlag)
	if len(addrs) == 0 {
		log.Fatal("--addrs is required")
	}
	if len(addrs) != len(ids) {
		log.Fatalf("--addrs (%d) and --ids (%d) length mismatch", len(addrs), len(ids))
	}
	if int(*e) != len(ids) {
		log.Fatalf("--e=%d must equal number of bookies (%d)", *e, len(ids))
	}
	quorum := ledgerclient.Quorum{E: uint32(*e), Qw: uint32(*qw), Qa: uint32(*qa)}
	if err := quorum.Validate(); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if *duration > 0 {
		var cancelDur context.CancelFunc
		ctx, cancelDur = context.WithTimeout(ctx, *duration)
		defer cancelDur()
	}

	bookies := map[ledgerclient.BookieID]ledgerclient.Bookie{}
	var closers []*ledgerclient.GRPCBookie
	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()
	for i, addr := range addrs {
		dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
		b, err := ledgerclient.DialBookie(dialCtx, addr)
		dialCancel()
		if err != nil {
			log.Fatalf("dial %s (%s): %v", ids[i], addr, err)
		}
		closers = append(closers, b)
		bookies[ids[i]] = b
	}

	id := *ledgerID
	if id == 0 {
		id = uint64(time.Now().UnixNano())
	}
	metas := ledgerclient.NewMemoryMetaStore()
	writer, err := ledgerclient.Create(metas, bookies, id, quorum, ids, []byte("traffic-secret"))
	if err != nil {
		log.Fatalf("create ledger %d: %v", id, err)
	}
	log.Printf("ledger=%d ensemble=%v quorum=E%d/Qw%d/Qa%d duration=%s",
		id, ids, quorum.E, quorum.Qw, quorum.Qa, *duration)

	var writesOK, writesErr, readsOK, readsErr atomic.Uint64
	payload := make([]byte, *payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	stopReport := make(chan struct{})
	go func() {
		t := time.NewTicker(*reportEvery)
		defer t.Stop()
		for {
			select {
			case <-stopReport:
				return
			case <-t.C:
				log.Printf("stats writes_ok=%d writes_err=%d reads_ok=%d reads_err=%d lac=%d",
					writesOK.Load(), writesErr.Load(), readsOK.Load(), readsErr.Load(), writer.LAC())
			}
		}
	}()

	deadline := time.Now().Add(*duration)
	for ctx.Err() == nil {
		if *duration > 0 && time.Now().After(deadline) {
			break
		}
		entryID, err := writer.Append(payload)
		if err != nil {
			writesErr.Add(1)
			if ledgerclient.KindOf(err) == ledgerclient.KindFenced {
				log.Printf("fenced at entry attempt: %v", err)
				break
			}
			// Transient: bookie down mid-fault; wait and retry same next id.
			time.Sleep(*retry)
			continue
		}
		writesOK.Add(1)

		// Verify the just-confirmed entry is readable from quorum.
		entry, err := writer.Read(entryID)
		if err != nil {
			readsErr.Add(1)
			time.Sleep(*retry)
			continue
		}
		if len(entry.Payload) != len(payload) {
			readsErr.Add(1)
			log.Printf("payload length mismatch entry=%d", entryID)
			continue
		}
		readsOK.Add(1)
	}
	close(stopReport)

	last, closeErr := writer.Close()
	log.Printf("done writes_ok=%d writes_err=%d reads_ok=%d reads_err=%d lac=%d close_last=%d close_err=%v",
		writesOK.Load(), writesErr.Load(), readsOK.Load(), readsErr.Load(), writer.LAC(), last, closeErr)

	if writesOK.Load() == 0 {
		os.Exit(2)
	}
	// Allow some transient errors during faults; fail if reads never succeeded after writes.
	if readsOK.Load() == 0 {
		os.Exit(3)
	}
	if closeErr != nil {
		os.Exit(4)
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
