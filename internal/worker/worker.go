package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AluminateOrg/notification-agent/internal/kafka"
	"github.com/AluminateOrg/notification-agent/internal/queue"
	"github.com/AluminateOrg/notification-agent/internal/repo"
	"github.com/AluminateOrg/notification-agent/internal/senders"
	"github.com/AluminateOrg/notification-agent/pkg/store"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type Worker struct {
	repo *repo.Repo
	queue *queue.RedisQueue
	sendPool *senders.SenderPool
	kw *kafka.Writer
	cfg *Config
	orgID string
	store *store.ConfigStore
}

type Config struct {
	RateLimitPerMin int
	BatchSize int
	MaxRetries int
}

func NewWorker(r *repo.Repo, q *queue.RedisQueue, sp *senders.SenderPool, kw *kafka.Writer, cfg *Config, orgID string, st *store.ConfigStore) *Worker {
	return &Worker{repo: r, queue: q, sendPool: sp, kw: kw, cfg: cfg, orgID: orgID, store: st}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(1*time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("worker stopped")
			return
		case <-ticker.C:
			w.pollAndProcess(ctx)
			w.processFallback(ctx)
		}
	}
}

func (w *Worker) pollAndProcess(ctx context.Context) {
	// In this model, backend persists tenant notifications; NA receives API requests and inserts a local record (fallback) and enqueues to Redis/Kafka
	// For simplicity, assume NA has an internal table of queued notifications (fallback) OR receives via /v1/notify (HTTP handler)
	// Here we'll process fallback_notifications if any
}

func (w *Worker) PublishPush(ctx context.Context, eventType string, payload []byte, recipient string) error {
	// Build envelop
	id := uuid.NewString()
	msg := map[string]any{"id": id, "orgId": w.orgID, "type": eventType, "payload": json.RawMessage(payload), "recipient": recipient, "ts": time.Now().UTC()}
	b, _ := json.Marshal(msg)
	// key = orgId
	if err := kafka.WriteWithRetires(ctx, w.kw, []byte(w.orgID), b, w.cfg.MaxRetries); err != nil {
		// persist fallback for durability
		_ = w.store.SaveFallback(id, w.orgID, "push", string(b), 0, time.Now().Add(1*time.Minute).Unix())
		return fmt.Errorf("kafka publish failed: %w", err)
	}
	return nil
}

func (w *Worker) processFallback(ctx context.Context) {
	// Pull fallback rows where next_try <= now and attempt publish
	// For brevity we left implementation outline; in production implement query loop
}