package senders

import (
	"context"
	"fmt"
	"net/http"
	"net/smtp"
	"strings"
	"time"

	"github.com/AluminateOrg/notification-agent/internal/repo"
	"github.com/rs/zerolog/log"
)

type SenderPool struct {
	SMTPHost string
	SMTPPort string
	SMTPUser string
	SMTPPass string
	SMTPFrom string

	SMSBaseURL string
	SMSAccountSID string
	SMSAuthToken string
	SMSFrom string
}

func NewSenderPool(cfg map[string]string) *SenderPool {
	return &SenderPool{
		SMTPHost: cfg["SMTP_HOST"],
		SMTPPort: cfg["SMTP_PORT"],
		SMTPUser: cfg["SMTP_USER"],
		SMTPPass: cfg["SMTP_PASS"],
		SMTPFrom: cfg["SMTP_FROM"],
		SMSBaseURL: cfg["SMS_BASE_URL"],
		SMSAccountSID: cfg["SMS_ACCOUNT_SID"],
		SMSAuthToken: cfg["SMS_AUTH_TOKEN"],
		SMSFrom: cfg["SMS_FROM"],
	}
}

// email sends through SMTP (basic)
func (s *SenderPool) SendEmail(ctx context.Context, n repo.Notification, subject, body string) error {
	addr := fmt.Sprintf("%s:%s", s.SMTPHost, s.SMTPPort)
	auth := smtp.PlainAuth("", s.SMTPUser, s.SMTPPass, s.SMTPHost)
	to := []string{n.Recipient}
	msg := []byte("From: " + s.SMTPFrom + "\r\n" +
		"To: " + strings.Join(to, ",") + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"\r\n" + body + "\r\n")
	if err := smtp.SendMail(addr, auth, s.SMTPFrom, to, msg); err != nil {
		return err
	}
	log.Printf("Email sent to %s for notification %s\n", n.Recipient, n.OrgID)
	return nil
}

// function to send SMS via Twilio API
func (s *SenderPool) SendSMS(ctx context.Context, n repo.Notification, body string) error {
	url := fmt.Sprintf("%s/Accounts/%s/Messages.json", s.SMSBaseURL, s.SMSAccountSID)
	data := map[string]string{"From": s.SMSFrom, "To": n.Recipient, "Body": body}
	form := ""
	for k, v := range data {
		form += fmt.Sprintf("%s=%s&", k, v)
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(s.SMSAccountSID, s.SMSAuthToken)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("sms send failed: %s", resp.Status)
	}
	log.Info().Str("recipient", n.Recipient).Msg("sms sent")
	return nil
}

// func (s *SenderPool) SendPush(ctx context.Context, token string, title, body string) error {
// 	if s.FCMServerKey == "" {
// 		return fmt.Errorf("FCM server key not configured")
// 	}

// }