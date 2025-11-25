package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AluminateOrg/notification-agent/internal/kafka"
	"github.com/AluminateOrg/notification-agent/internal/repo"
	"github.com/AluminateOrg/notification-agent/pkg/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)


type Server struct {
	repo *repo.Repo
	org string
	kWriter *kafka.Writer
	store *store.ConfigStore
}

type NotifiReq struct {
	EventType string `json:"event_type"`
	Payload map[string]interface{} `json:"payload"`
	Channel string `json:"channel"`
	Recipient string `json:"recipient"`
}

func NewServer(org string, r *repo.Repo, kw *kafka.Writer, s *store.ConfigStore) *Server {
	return &Server{org: org, repo: r, kWriter: kw, store: s}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Post("/v1/notify", s.handleNotify)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request){ w.Write([]byte("ok")) })
	return r
}

func (s *Server) handleNotify(w http.ResponseWriter, r *http.Request) {
	var nr NotifiReq
	if err := json.NewDecoder(r.Body).Decode(&nr); err != nil {
		http.Error(w,"bad request", http.StatusBadRequest); return
	}
	id := uuid.NewString()
	payloadBytes, _ := json.Marshal(nr.Payload)
	// For durability: save fallback entry immediately
	_ = s.store.SaveFallback(id, s.org, nr.Channel, string(payloadBytes), 0, time.Now().Unix())
	// If channel = push -> publish to Kafka
	if nr.Channel == "push" {
		msg := map[string]any{"id": id, "orgId": s.org, "type": nr.EventType, "payload": json.RawMessage(payloadBytes), "recipient": nr.Recipient}
		b, _ := json.Marshal(msg)
		// write synchronously and retry
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second); defer cancel()
		err := kafka.WriteWithRetires(ctx, s.kWriter, []byte(s.org), b, 3)
		if err != nil {
			log.Error().Err(err).Msg("kafka publish failed, fallback saved")
			http.Error(w,"accepted with fallback", http.StatusAccepted)
			return
		}
		// on success, remove fallback row (implement delete in store.SaveFallback or separate function)
		// TODO: Delete fallback entry where id
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"id": id, "status":"published"})
		return
	}
	// For email/sms: enqueue or send via senders (not implemented here)
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"id": id, "status":"queued"})
}