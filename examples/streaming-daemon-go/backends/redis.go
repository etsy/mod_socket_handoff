package backends

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"examples/config"
)

// RedisPubSub is a backend that subscribes to a Redis pub/sub channel and
// forwards each published message to the client as a raw SSE data event.
// It is used by the conversations streaming endpoint: PHP hands off the
// connection after auth, and the daemon holds the long-lived subscription
// without tying up an Apache worker.
type RedisPubSub struct {
	client *goredis.Client
}

func init() {
	Register(&RedisPubSub{})
}

func (r *RedisPubSub) Name() string {
	return "redis-pubsub"
}

func (r *RedisPubSub) Description() string {
	return "Forwards Redis pub/sub messages to the client as SSE events"
}

func (r *RedisPubSub) Init(cfg *config.BackendConfig) error {
	addr := "127.0.0.1:6379"
	if cfg != nil && cfg.Redis.Addr != "" {
		addr = cfg.Redis.Addr
	}

	r.client = goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: cfg.Redis.Password,
	})

	// Verify connectivity at startup.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping failed: %w", err)
	}

	slog.Info("redis-pubsub backend initialized", "addr", addr)
	return nil
}

// Stream subscribes to the conversation's Redis channel and writes each
// incoming message directly as an SSE data line. The subscription is held
// until the client disconnects (ctx cancelled) or an error occurs.
func (r *RedisPubSub) Stream(ctx context.Context, conn net.Conn, handoff HandoffData) (int64, error) {
	if handoff.SubjectID == 0 {
		return 0, fmt.Errorf("subject_id is required")
	}

	channel := fmt.Sprintf("subject:%d", handoff.SubjectID)
	RecordBackendRequest("redis-pubsub")

	pubsub := r.client.Subscribe(ctx, channel)
	defer pubsub.Close()

	var totalBytes int64

	msgCh := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return totalBytes, ctx.Err()

		case msg, ok := <-msgCh:
			if !ok {
				// Channel closed — client disconnected or subscription dropped.
				return totalBytes, nil
			}

			line := "data: " + msg.Payload + "\n\n"
			if err := conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
				return totalBytes, fmt.Errorf("set write deadline: %w", err)
			}
			n, err := conn.Write([]byte(line))
			totalBytes += int64(n)
			RecordChunkSent()
			if err != nil {
				RecordBackendError("redis-pubsub")
				return totalBytes, fmt.Errorf("write failed: %w", err)
			}
		}
	}
}
