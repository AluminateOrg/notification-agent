package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port string
	OrgApiURL string
	BootstrapToken  string
	DatabaseURL     string
	KafkaBrokers    []string
	KafkaPushTopic string
	KafkaGlobalTopic string
	KafkaGroupID   string
	RedisAddr       string
	SMTPHost        string
	SMTPPort        string
	SMTPUser        string
	SMTPPass        string
	SMTPFrom        string
	SMSBaseURL      string
	SMSAccountSID   string
	SMSAuthToken    string
	SMSFrom         string
	FCMServerKey    string
	WorkerCount    int
	BatchSize      int
	RateLimitPerMin int
	MaxRetries     int
}

func Load() *Config {
	c := &Config{
		Port: get("NA_PORT", "8090"),
		OrgApiURL: get("ORG_API_URL", "http://localhost:8098"),
		BootstrapToken: get("BOOTSTRAP_TOKEN", ""),
		DatabaseURL: get("DATABASE_URL", "postgres://pulasthi:2002@localhost:5432/aluminatenotifina?sslmode=disable"),
		KafkaBrokers: strings.Split(get("KAFKA_BROKERS", "localhost:19092"), ","),
		KafkaPushTopic: get("KAFKA_PUSH_TOPIC", "push.notifications"),
		KafkaGlobalTopic: get("KAFKA_GLOBAL_TOPIC", "notifications.global"),
		KafkaGroupID: get("KAFKA_GROUP_ID", "na-consumer"),
		RedisAddr:       get("REDIS_ADDR", "localhost:6379"),
		SMTPHost:        get("SMTP_HOST", "smtp.sendgrid.net"),
		SMTPPort:        get("SMTP_PORT", "587"),
		SMTPUser:        get("SMTP_USER", "apikey"),
		SMTPPass:        get("SMTP_PASS", ""),
		SMTPFrom:        get("SMTP_FROM", "no-reply@example.com"),
		SMSBaseURL:      get("SMS_BASE_URL", ""),
		SMSAccountSID:   get("SMS_ACCOUNT_SID", ""),
		SMSAuthToken:    get("SMS_AUTH_TOKEN", ""),
		SMSFrom:         get("SMS_FROM", ""),
		FCMServerKey:    get("FCM_SERVER_KEY", ""),
		WorkerCount: atoi("WORKER_COUNT", 10),
		BatchSize:    atoi("BATCH_SIZE", 20),
		RateLimitPerMin: atoi("RATE_LIMIT_PER_MIN", 300),
		MaxRetries:   atoi("MAX_RETRIES", 5),
	}
	return c
}

func get(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func atoi(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {return def}
	n, err := strconv.Atoi(v)
	if err != nil {return def}
	return n
}