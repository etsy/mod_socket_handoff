package backends

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// runMonitor runs Stream against a net.Pipe and returns once it exits.
// The drain callback receives the client end so a test can either read the
// pings or close the connection to simulate a disconnect.
func runMonitor(t *testing.T, ctx context.Context, n *NoopMonitor, h HandoffData, drain func(client net.Conn)) {
	t.Helper()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	drain(client)

	done := make(chan struct{})
	go func() {
		n.Stream(ctx, server, h)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not return within timeout")
	}
}

func TestNoopMonitorClientDisconnect(t *testing.T) {
	before := testutil.ToFloat64(monitorClosed.WithLabelValues("detail", "client_disconnect"))
	activeBefore := testutil.ToFloat64(monitorActive.WithLabelValues("detail"))

	n := &NoopMonitor{pingInterval: 5 * time.Millisecond}
	runMonitor(t, context.Background(), n, HandoffData{Source: "detail"}, func(client net.Conn) {
		client.Close() // client is gone; the first ping write fails
	})

	if got := testutil.ToFloat64(monitorClosed.WithLabelValues("detail", "client_disconnect")); got != before+1 {
		t.Errorf("client_disconnect counter = %v, want %v", got, before+1)
	}
	if got := testutil.ToFloat64(monitorActive.WithLabelValues("detail")); got != activeBefore {
		t.Errorf("active gauge = %v, want %v (should return to baseline)", got, activeBefore)
	}
}

func TestNoopMonitorShutdown(t *testing.T) {
	before := testutil.ToFloat64(monitorClosed.WithLabelValues("detail", "shutdown"))

	ctx, cancel := context.WithCancel(context.Background())
	// Long ping interval so no write happens before the cancel is observed.
	n := &NoopMonitor{pingInterval: 10 * time.Second}
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	runMonitor(t, ctx, n, HandoffData{Source: "detail"}, func(client net.Conn) {
		go io.Copy(io.Discard, client)
	})
	cancel()

	if got := testutil.ToFloat64(monitorClosed.WithLabelValues("detail", "shutdown")); got != before+1 {
		t.Errorf("shutdown counter = %v, want %v", got, before+1)
	}
}

func TestNoopMonitorMaxLifetime(t *testing.T) {
	before := testutil.ToFloat64(monitorClosed.WithLabelValues("detail", "max_lifetime"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	n := &NoopMonitor{pingInterval: 10 * time.Second}
	runMonitor(t, ctx, n, HandoffData{Source: "detail"}, func(client net.Conn) {
		go io.Copy(io.Discard, client)
	})

	if got := testutil.ToFloat64(monitorClosed.WithLabelValues("detail", "max_lifetime")); got != before+1 {
		t.Errorf("max_lifetime counter = %v, want %v", got, before+1)
	}
}

func TestNoopMonitorWritesPing(t *testing.T) {
	n := &NoopMonitor{pingInterval: 5 * time.Millisecond}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		n.Stream(ctx, server, HandoffData{Source: "message_list"})
	}()

	got := make([]byte, len(pingMsg))
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("reading ping: %v", err)
	}
	if !bytes.Equal(got, pingMsg) {
		t.Errorf("first write = %q, want %q", got, pingMsg)
	}
	cancel()
}

func TestNormalizeSource(t *testing.T) {
	tests := map[string]string{
		"detail":       "detail",
		"message_list": "message_list",
		"":             "other",
		"bogus":        "other",
	}
	for in, want := range tests {
		if got := normalizeSource(in); got != want {
			t.Errorf("normalizeSource(%q) = %q, want %q", in, got, want)
		}
	}
}
