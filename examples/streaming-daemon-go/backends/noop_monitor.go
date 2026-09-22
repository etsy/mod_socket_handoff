package backends

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"examples/config"
)

// Backend-owned metrics for the connection monitor. Kept low-cardinality:
// source is clamped to {detail, message_list, other} and reason to a fixed
// set, so these are safe to expose per-label.
var (
	monitorActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "noop_monitor_active",
		Help: "Currently held monitor connections, by originating surface",
	}, []string{"source"})

	monitorClosed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "noop_monitor_closed_total",
		Help: "Monitor connections closed, by surface and reason",
	}, []string{"source", "reason"})
)

// NoopMonitor holds a handed-off client connection open, sending only periodic
// SSE keepalive comments, so connection volume/concurrency/duration can be read
// straight from this daemon's Prometheus metrics. It never streams message data.
type NoopMonitor struct {
	pingInterval time.Duration
}

func init() {
	Register(&NoopMonitor{})
}

func (n *NoopMonitor) Name() string {
	return "noop-monitor"
}

func (n *NoopMonitor) Description() string {
	return "Holds the connection open, sends nothing (convos connection monitor)"
}

func (n *NoopMonitor) Init(cfg *config.BackendConfig) error {
	n.pingInterval = 25 * time.Second
	if cfg != nil && cfg.NoopMonitor.PingIntervalMs > 0 {
		n.pingInterval = time.Duration(cfg.NoopMonitor.PingIntervalMs) * time.Millisecond
	}
	slog.Info("noop-monitor backend initialized", "ping_interval", n.pingInterval)
	return nil
}

// Stream holds the connection open, writing a `: ping` keepalive every
// pingInterval. A write-only stream over SOCK_SEQPACKET only learns the client
// left when a write fails, so the keepalive doubles as disconnect detection.
// A client disconnect is normal, not an error, so it returns (0, nil).
func (n *NoopMonitor) Stream(ctx context.Context, conn net.Conn, handoff HandoffData) (int64, error) {
	source := normalizeSource(handoff.Source)

	RecordBackendRequest("noop-monitor")
	monitorActive.WithLabelValues(source).Inc()
	defer monitorActive.WithLabelValues(source).Dec()

	start := time.Now()
	reason := "shutdown"
	ticker := time.NewTicker(n.pingInterval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
				reason = "max_lifetime"
			}
			break loop
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			if _, err := conn.Write(pingMsg); err != nil {
				reason = "client_disconnect"
				break loop
			}
		}
	}

	RecordBackendDuration("noop-monitor", time.Since(start).Seconds())
	monitorClosed.WithLabelValues(source, reason).Inc()
	return 0, nil
}

// pingMsg is an SSE comment line; clients ignore it, but the write attempt is
// how a write-only stream detects that the client has gone away.
var pingMsg = []byte(": ping\n\n")

// normalizeSource clamps the client-influenced source label to a known set so a
// malformed or unexpected value can't blow up Prometheus label cardinality.
func normalizeSource(source string) string {
	switch source {
	case "detail", "message_list":
		return source
	default:
		return "other"
	}
}
