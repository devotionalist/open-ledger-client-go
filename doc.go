// Package ledgerclient is the quorum client for open-ledger bookies.
//
// A bookie stores entry bytes. This package chooses write quorums, waits for
// Qa journal acknowledgements, fences, and recovers the ledger tail.
package ledgerclient
