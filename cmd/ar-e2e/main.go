// Seed a CLOSED ledger onto bookies and/or verify AutoRecovery replaced a missing member.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	ledgerclient "github.com/devotionalist/open-ledger-client-go"
)

func main() {
	mode := flag.String("mode", "seed", "seed | verify")
	addrsFlag := flag.String("addrs", "", "comma-separated bookie host:port")
	idsFlag := flag.String("ids", "", "comma-separated bookie ids matching --addrs")
	ensembleFlag := flag.String("ensemble", "bookie-0,bookie-1,bookie-2", "ensemble bookie ids")
	ledgerID := flag.Uint64("ledger", 0, "ledger id")
	entries := flag.Int("entries", 5, "number of entries to write (seed)")
	qw := flag.Uint("qw", 3, "write quorum")
	qa := flag.Uint("qa", 2, "ack quorum")
	masterKey := flag.String("master-key", "ar-e2e-key", "ledger master key")
	missing := flag.String("missing", "bookie-0", "bookie that should leave the ensemble (verify)")
	payloadPrefix := flag.String("payload-prefix", "ar-e2e-", "entry payload prefix")
	flag.Parse()

	if *ledgerID == 0 {
		log.Fatal("--ledger is required")
	}
	addrs := splitCSV(*addrsFlag)
	ids := splitCSV(*idsFlag)
	ensemble := splitCSV(*ensembleFlag)
	if len(addrs) == 0 || len(ids) == 0 {
		log.Fatal("--addrs and --ids are required")
	}
	if len(addrs) != len(ids) {
		log.Fatalf("--addrs (%d) and --ids (%d) length mismatch", len(addrs), len(ids))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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

	key := []byte(*masterKey)
	quorum := ledgerclient.Quorum{E: uint32(len(ensemble)), Qw: uint32(*qw), Qa: uint32(*qa)}
	if err := quorum.Validate(); err != nil {
		log.Fatal(err)
	}

	switch *mode {
	case "seed":
		if err := seed(bookies, *ledgerID, ensemble, quorum, key, *entries, *payloadPrefix); err != nil {
			log.Fatal(err)
		}
		meta := map[string]any{
			"ledger_id":     *ledgerID,
			"quorum":        map[string]any{"e": quorum.E, "qw": quorum.Qw, "qa": quorum.Qa},
			"state":         "Closed",
			"last_entry_id": int64(*entries - 1),
			"fragments": []map[string]any{{
				"first_entry_id": 0,
				"ensemble":       ensemble,
			}},
			"master_key":   bytesToJSONArray(key),
			"meta_version": 0,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(meta); err != nil {
			log.Fatal(err)
		}
	case "verify":
		if err := verify(bookies, *ledgerID, ensemble, *missing, key, *entries, *payloadPrefix); err != nil {
			log.Fatal(err)
		}
		fmt.Println("verify_ok")
	default:
		log.Fatalf("unknown --mode %q", *mode)
	}
}

func seed(
	bookies map[ledgerclient.BookieID]ledgerclient.Bookie,
	ledgerID uint64,
	ensemble []ledgerclient.BookieID,
	quorum ledgerclient.Quorum,
	key []byte,
	n int,
	prefix string,
) error {
	for _, id := range ensemble {
		b, ok := bookies[id]
		if !ok {
			return fmt.Errorf("ensemble member %s not in --ids/--addrs", id)
		}
		if err := b.CreateLedger(ledgerID, key); err != nil {
			return fmt.Errorf("create on %s: %w", id, err)
		}
	}
	for entryID := 0; entryID < n; entryID++ {
		targets, err := ledgerclient.WriteSet(ensemble, quorum.Qw, uint64(entryID))
		if err != nil {
			return err
		}
		payload := []byte(fmt.Sprintf("%s%d", prefix, entryID))
		lac := int64(entryID - 1)
		for _, id := range targets {
			if err := bookies[id].AddEntry(ledgerID, uint64(entryID), lac, payload, key); err != nil {
				return fmt.Errorf("add entry=%d on %s: %w", entryID, id, err)
			}
		}
	}
	return nil
}

func verify(
	bookies map[ledgerclient.BookieID]ledgerclient.Bookie,
	ledgerID uint64,
	ensemble []ledgerclient.BookieID,
	missing string,
	_ []byte,
	n int,
	prefix string,
) error {
	checked := 0
	for _, id := range ensemble {
		if id == missing {
			return fmt.Errorf("ensemble still contains missing bookie %s: %v", missing, ensemble)
		}
		b, ok := bookies[id]
		if !ok {
			continue
		}
		for entryID := 0; entryID < n; entryID++ {
			want := []byte(fmt.Sprintf("%s%d", prefix, entryID))
			got, err := b.ReadEntry(ledgerID, uint64(entryID))
			if err != nil {
				return fmt.Errorf("read entry=%d on %s: %w", entryID, id, err)
			}
			if string(got.Payload) != string(want) {
				return fmt.Errorf("entry=%d on %s payload=%q want=%q", entryID, id, got.Payload, want)
			}
		}
		checked++
		fmt.Fprintf(os.Stderr, "verified %d entries on %s\n", n, id)
	}
	if checked == 0 {
		return fmt.Errorf("no dialable ensemble members in %v", ensemble)
	}
	return nil
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

func bytesToJSONArray(b []byte) []int {
	out := make([]int, len(b))
	for i, v := range b {
		out[i] = int(v)
	}
	return out
}
