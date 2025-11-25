package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/AluminateOrg/notification-agent/internal/kafka"
	"github.com/AluminateOrg/notification-agent/internal/queue"
	"github.com/AluminateOrg/notification-agent/internal/senders"
	"github.com/AluminateOrg/notification-agent/internal/templates"
	
)

type OnboardEvent struct {
	Event string `json:"event"`
	OrgId string `json:"orgId"`
	Data struct {
		Email string `json:"email"`
		Name string `json:"name"`
		OrgName string `json:"orgName"`
	}
}

type EmailConfig struct {
	WorkerCount int 
	BatchSize int 
	RateLimitPerMin int 
	MaxRetries int
}

type EmailWorker struct {
	reader *kafka.Reader
	queue *queue.RedisQueue
	sender *senders.SMTPSender
	tpl *templates.TemplateManager
	cfg *EmailConfig
	jobs chan *kafka.Message
}

func NewEmailWorker(reader *kafka.Reader, queue *queue.RedisQueue, sender *senders.SMTPSender,
	tpl *templates.TemplateManager, cfg *EmailConfig) *EmailWorker {
		w := &EmailWorker{reader: reader, queue: queue, sender: sender, tpl: tpl, cfg: cfg, jobs: make(chan *kafka.Message, 1000)}
		return w
}

func (w *EmailWorker) Run(ctx context.Context) {
	for i := 0; i < w.cfg.WorkerCount; i++ {
		go w.workerLoop(ctx, i)
	}

	go w.retryLoop(ctx)

	for {
		m, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("email worker stopped")
				return
			}
			log.Printf("kafka fetch err: %v; retrying...", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		select {
		case w.jobs <- &m:
		case <-ctx.Done():
			return
		}
		if err := w.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}


func (w *EmailWorker) workerLoop(ctx context.Context, id int) {
	l := log.New(log.Writer(), fmt.Sprintf("[worker-%d] ", id), log.LstdFlags)
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-w.jobs:
			var ev OnboardEvent
			if err := json.Unmarshal(m.Value, &ev); err != nil {
				l.Printf("invalid message: %v", err) 
				continue
			}
			//rate limit 
			ok, err := w.queue.Allow(ctx, ev.OrgId, w.cfg.RateLimitPerMin, time.Minute)
			if err != nil {
				l.Printf("rate allow error %v", err) 
			}
			if !ok {
				_ = w.queue.PushDelayed(ctx, string(m.Key)+"|"+string(m.Value), time.Now().Add(10*time.Second))
				continue
			}
			tData := map[string]any{
				"Name": ev.Data.Name,
				"OrgId": ev.OrgId,
				"OrgName": ev.Data.OrgName,
			}
			body, err := w.tpl.Render("onboarding.html", tData)
			if err != nil {
				l.Printf("template render error: %v", err) 
				continue
			}
			subject := fmt.Sprintf("Welcome to %s, %s", ev.Data.OrgName, ev.Data.Name)
			//send async
			ctxSend, cancel := context.WithTimeout(ctx, 15*time.Second)
			err = w.sender.Send(ctxSend, ev.Data.Email, subject, body, "text/html")
			cancel()
			if err != nil {
				l.Printf("send email failed: %v", err)
				// push to retry with exponential backoff
				// store string composed of key|value as id; this is a simple approach; production use proper IDs
				_ = w.queue.PushDelayed(ctx, string(m.Key)+"|"+string(m.Value), time.Now().Add(1*time.Minute))
			} else {
				l.Printf("email sent to %s", ev.Data.Email)
			}
		}
	}
}

func (w *EmailWorker) retryLoop(ctx context.Context) {
	t := time.NewTicker(5*time.Second)
	defer t.Stop()
	for {
		select {
			case <-ctx.Done():
				return
			case <- t.C:
				ids, err := w.queue.PopReady(ctx, 50)
				if err != nil {
					log.Printf("retry pop ready error: %v", err)
					continue
				}
				for _, entry := range ids {
					// entry format: key|value
				// attempt to reconstruct message and push into jobs channel
				// naive parsing:
				sep := "|"
				// find first '|' index
				idx := -1
				for i := 0; i < len(entry); i++ {
					if entry[i] == sep[0] {
						idx = i
						break
					}
				}
				if idx == -1 {
					// malformed; skip
					continue
				}
				key := []byte(entry[:idx])
				val := []byte(entry[idx+1:])
				// create kafka.Message object to reuse worker flow
				m := &kafka.Message{Key: key, Value: val}
				select {
				case w.jobs <- m:
				default:
					// if jobs buffer full, push back as delayed
					_ = w.queue.PushDelayed(ctx, entry, time.Now().Add(10*time.Second))
				}
			}
		}
	}
}