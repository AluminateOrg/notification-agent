package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/AluminateOrg/notification-agent/internal/config"
	"github.com/AluminateOrg/notification-agent/internal/kafka"
	"github.com/AluminateOrg/notification-agent/internal/queue"
	"github.com/AluminateOrg/notification-agent/internal/registration"
	"github.com/AluminateOrg/notification-agent/internal/repo"
	"github.com/AluminateOrg/notification-agent/internal/senders"
	"github.com/AluminateOrg/notification-agent/internal/server"
	"github.com/AluminateOrg/notification-agent/internal/templates"
	"github.com/AluminateOrg/notification-agent/internal/worker"
	"github.com/AluminateOrg/notification-agent/pkg/store"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	cfg := config.Load()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	log.Info().Msg("starting notification agent")

	cs, err := store.NewConfigStore("./na_config.db")
	if err != nil {log.Fatal().Err(err).Msg("failed to init config store")}
	defer cs.Close()

	orgId, _ := cs.Get("orgId")
	if orgId == "" {
		log.Info().Msg("Cant find registered organisation, registering from backend")
		id, name, err := registration.Register(cfg.OrgApiURL, cfg.BootstrapToken)
		if err != nil {log.Fatal().Err(err).Msg("registration failed")}
		orgId = id
		if err := cs.Set("orgId", orgId); err != nil {log.Fatal().Err(err).Msg("failed to save organisation id")}
		log.Info().Str("orgId", orgId).Str("orgName", name).Msg("registration successful")
	}

	//templates 
	tpl, err := templates.NewTemplateManager("./templates")
	if err != nil {log.Fatal().Err(err).Msg("failed to load templates")}

	kc := kafka.NewKafkaClient(cfg.KafkaBrokers)
	kWriter := kc.NewWriter(cfg.KafkaPushTopic)
	kReader := kc.NewReader(cfg.KafkaGlobalTopic, cfg.KafkaGroupID)

	//smtp sender 
	emailSender := senders.NewSMTPSender(cfg.SMTPHost, cfg.SMTPPort, cfg.SMTPUser, cfg.SMTPPass, cfg.SMTPFrom)

	//email reader 
	emailReader := kc.NewEmailReader("onboarding.members", "na-email-group")

	//email worker 
	ewcfg := &worker.EmailConfig{
		WorkerCount: cfg.WorkerCount,
		BatchSize: cfg.BatchSize,
		RateLimitPerMin: cfg.RateLimitPerMin,
		MaxRetries: cfg.MaxRetries,
	}
	
	rq := queue.NewRedisQueue(cfg.RedisAddr)

	ew := worker.NewEmailWorker(emailReader, rq, emailSender, tpl, ewcfg)

	repo, err := repo.NewRepo(cfg.DatabaseURL)
	if err != nil {log.Fatal().Err(err).Msg("failed to init repo")}
	defer repo.Close()

	senderCfg := map[string]string{
		"SMTP_HOST": cfg.SMTPHost, "SMTP_PORT": cfg.SMTPPort, "SMTP_USER": cfg.SMTPUser, "SMTP_PASS": cfg.SMTPPass, "SMTP_FROM": cfg.SMTPFrom,
		"SMS_BASE_URL": cfg.SMSBaseURL, "SMS_ACCOUNT_SID": cfg.SMSAccountSID, "SMS_AUTH_TOKEN": cfg.SMSAuthToken, "SMS_FROM": cfg.SMSFrom,
	}

	sp := senders.NewSenderPool(senderCfg)

	wcfg := &worker.Config{RateLimitPerMin: 200, BatchSize: 20, MaxRetries: 5}
	w := worker.NewWorker(repo, rq, sp, kWriter, wcfg, orgId, cs)
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	//run the email worker 
	go ew.Run(ctx)

	go func() {
		for {
			msg, err := kReader.ReadMessage(ctx)
			if err != nil {log.Error().Err(err).Msg("kafka read error"); time.Sleep(time.Second); continue}
			key := string(msg.Key)
			if key == "" || key == orgId {
				endpoint := fmt.Sprintf("%s/common/notifi-agent/global", cfg.OrgApiURL)
				req, _ := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(msg.Value))
				req.Header.Set("Content-Type","application/json")
				client := &http.Client{Timeout:5*time.Second}
				resp, err := client.Do(req)
				if err != nil {
					log.Error().Err(err).Msg("forward global to backend failed")
					continue
				}
				resp.Body.Close()
			}
		}
	}()

	srv := server.NewServer(orgId, repo, kWriter, cs)
	httpSrv := &http.Server{Addr: ":"+cfg.Port, Handler: srv.Router(), ReadTimeout: 5*time.Second, WriteTimeout: 10*time.Second}
	go func() {
		log.Info().Msgf("server listen on %s", httpSrv.Addr)
		if errr := httpSrv.ListenAndServe(); errr != nil && errr != http.ErrServerClosed {
			log.Fatal().Err(errr).Msg("http server failed")
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Info().Msg("shutdown requested")
	cancel()
	ctxShut, cancelShut := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShut()
	_ = httpSrv.Shutdown(ctxShut)
	_ = kWriter.Close()
	_ = kReader.Close()


}