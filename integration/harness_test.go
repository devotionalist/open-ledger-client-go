//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ledgerclient "github.com/devotionalist/open-ledger-client-go"
)

type cluster struct {
	ns      string
	ids     []string
	bookies map[ledgerclient.BookieID]ledgerclient.Bookie
	closers []*ledgerclient.GRPCBookie
	ports   []int
	pfs     []*exec.Cmd
	mu      sync.Mutex
}

func (c *cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range c.closers {
		_ = b.Close()
	}
	for _, cmd := range c.pfs {
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	}
}

// live returns a bookie map excluding missing ids (simulates unreachable members).
func (c *cluster) live(exclude ...string) map[ledgerclient.BookieID]ledgerclient.Bookie {
	skip := map[string]struct{}{}
	for _, id := range exclude {
		skip[id] = struct{}{}
	}
	out := make(map[ledgerclient.BookieID]ledgerclient.Bookie, len(c.bookies))
	for id, b := range c.bookies {
		if _, bad := skip[id]; bad {
			continue
		}
		out[id] = b
	}
	return out
}

// pair is the first two bookies (for E=2 scenarios).
func (c *cluster) pair() ([]string, map[ledgerclient.BookieID]ledgerclient.Bookie) {
	ids := c.ids[:2]
	return ids, c.live(c.ids[2:]...)
}

var (
	sharedOnce sync.Once
	shared     *cluster
	sharedErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if shared != nil {
		shared.Close()
	}
	os.Exit(code)
}

func openCluster(t *testing.T) *cluster {
	t.Helper()
	sharedOnce.Do(func() {
		shared, sharedErr = dialCluster()
	})
	if sharedErr != nil {
		t.Fatal(sharedErr)
	}
	return shared
}

func dialCluster() (*cluster, error) {
	ns := envOr("OPEN_LEDGER_NS", "open-ledger")
	want := bookieCount()
	ids := make([]string, want)
	for i := 0; i < want; i++ {
		ids[i] = fmt.Sprintf("bookie-%d", i)
	}

	if addrs := strings.TrimSpace(os.Getenv("OPEN_LEDGER_BOOKIE_ADDRS")); addrs != "" {
		return dialAddrs(strings.Split(addrs, ","), ids)
	}

	for _, id := range ids {
		if err := kubectl(ns, "get", "pod", id); err != nil {
			return nil, err
		}
		if err := waitReady(ns, id); err != nil {
			return nil, err
		}
	}

	c := &cluster{
		ns:      ns,
		ids:     ids,
		bookies: map[ledgerclient.BookieID]ledgerclient.Bookie{},
		ports:   make([]int, len(ids)),
		pfs:     make([]*exec.Cmd, len(ids)),
		closers: make([]*ledgerclient.GRPCBookie, 0, len(ids)),
	}
	for i, id := range ids {
		port, err := freePort()
		if err != nil {
			c.Close()
			return nil, err
		}
		c.ports[i] = port
		if err := c.startPF(i); err != nil {
			c.Close()
			return nil, err
		}
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		bookie, err := ledgerclient.DialBookie(ctx, addr)
		cancel()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("dial %s at %s: %w", id, addr, err)
		}
		c.closers = append(c.closers, bookie)
		c.bookies[id] = bookie
	}
	return c, nil
}

func dialAddrs(addrs []string, ids []string) (*cluster, error) {
	if len(addrs) < len(ids) {
		return nil, fmt.Errorf("OPEN_LEDGER_BOOKIE_ADDRS needs %d host:port values", len(ids))
	}
	c := &cluster{
		ns:      envOr("OPEN_LEDGER_NS", "open-ledger"),
		ids:     ids,
		bookies: map[ledgerclient.BookieID]ledgerclient.Bookie{},
		ports:   make([]int, len(ids)),
		pfs:     make([]*exec.Cmd, len(ids)),
	}
	for i, id := range ids {
		addr := strings.TrimSpace(addrs[i])
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		bookie, err := ledgerclient.DialBookie(ctx, addr)
		cancel()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		c.closers = append(c.closers, bookie)
		c.bookies[id] = bookie
	}
	return c, nil
}

func (c *cluster) startPF(idx int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cmd := c.pfs[idx]; cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
	id := c.ids[idx]
	port := c.ports[idx]
	cmd := exec.Command("kubectl", "-n", c.ns, "port-forward", "pod/"+id, fmt.Sprintf("%d:3181", port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("port-forward %s: %w", id, err)
	}
	c.pfs[idx] = cmd
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitTCP(addr, 30*time.Second); err != nil {
		return err
	}
	return nil
}

// deletePod recreates a bookie pod and rebinds its port-forward on the same local port.
func (c *cluster) deletePod(t *testing.T, id string) {
	t.Helper()
	idx := -1
	for i, name := range c.ids {
		if name == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatalf("unknown bookie %s", id)
	}
	if err := kubectl(c.ns, "delete", "pod", id, "--wait=true"); err != nil {
		t.Fatal(err)
	}
	if err := waitSTS(c.ns); err != nil {
		t.Fatal(err)
	}
	if err := waitReady(c.ns, id); err != nil {
		t.Fatal(err)
	}
	if c.ports[idx] == 0 {
		return // dialAddrs mode: no PF to rebind
	}
	if err := c.startPF(idx); err != nil {
		t.Fatalf("rebind port-forward %s: %v", id, err)
	}
	// Give gRPC a moment to reconnect through the new forward.
	time.Sleep(500 * time.Millisecond)
}

func bookieCount() int {
	if v := strings.TrimSpace(os.Getenv("OPEN_LEDGER_BOOKIE_COUNT")); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n >= 2 {
			return n
		}
	}
	return 8
}

func envOr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func kubectl(ns string, args ...string) error {
	cmd := exec.Command("kubectl", append([]string{"-n", ns}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl %v: %w\n%s", args, err, out)
	}
	return nil
}

func waitSTS(ns string) error {
	cmd := exec.Command("kubectl", "-n", ns, "rollout", "status", "statefulset/bookie", "--timeout=180s")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("rollout status: %w\n%s", err, out)
	}
	return nil
}

func waitReady(ns, pod string) error {
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command("kubectl", "-n", ns, "get", "pod", pod, "-o", "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}")
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "True" {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("pod %s not Ready", pod)
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func waitTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("tcp %s not accepting", addr)
}

var ledgerSeq uint64

func nextLedgerID() uint64 {
	ledgerSeq++
	return uint64(time.Now().UnixNano()) + ledgerSeq
}

func quorumTwo() ledgerclient.Quorum {
	return ledgerclient.Quorum{E: 2, Qw: 2, Qa: 2}
}

// quorumCluster is E=|ensemble|, Qw=3, Qa=2 (striped when E>Qw).
func (c *cluster) quorumCluster() ledgerclient.Quorum {
	e := uint32(len(c.ids))
	qw := uint32(3)
	if qw > e {
		qw = e
	}
	qa := uint32(2)
	if qa > qw {
		qa = qw
	}
	return ledgerclient.Quorum{E: e, Qw: qw, Qa: qa}
}

// quorumDense is E=Qw=min(3,n), Qa=2 — every entry hits the whole small ensemble.
func (c *cluster) quorumDense() ledgerclient.Quorum {
	n := len(c.ids)
	if n > 3 {
		n = 3
	}
	e := uint32(n)
	return ledgerclient.Quorum{E: e, Qw: e, Qa: 2}
}

func (c *cluster) denseIDs() []string {
	n := len(c.ids)
	if n > 3 {
		n = 3
	}
	return c.ids[:n]
}

func (c *cluster) denseBookies() map[ledgerclient.BookieID]ledgerclient.Bookie {
	out := make(map[ledgerclient.BookieID]ledgerclient.Bookie)
	for _, id := range c.denseIDs() {
		out[id] = c.bookies[id]
	}
	return out
}

func requireMinBookies(t *testing.T, c *cluster, n int) {
	t.Helper()
	if len(c.ids) < n {
		t.Skipf("need >=%d bookies, have %d", n, len(c.ids))
	}
}
