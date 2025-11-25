package senders

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type SMTPSender struct {
	host string
	port string
	user string
	pass string
	from string
}

func NewSMTPSender(host, port, user, pass, from string) *SMTPSender {
	return &SMTPSender{host: host, port: port, user: user, pass: pass, from: from}
}

func (s *SMTPSender) Send(ctx context.Context, to, subject, body, contentType string) error {
	addr := net.JoinHostPort(s.host, s.port)
	auth := smtp.PlainAuth("", s.user, s.pass, s.host)

	//build headers 
	headers := map[string]string{
		"From":		 s.from,
		"To":		 to,
		"Subject":	 subject,
		"MIME-Version": "1.0",
		"Content-Type": contentType + "; charset=\"utf-8\"",
	}

	var msg strings.Builder
	for k, v := range headers {
		msg.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
	}
    msg.WriteString("\r\n")
	msg.WriteString(body)

	//configure TLS 
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5*time.Second}, "tcp", addr, &tls.Config{
		InsecureSkipVerify: false,
		ServerName: s.host,
	})

	if err == nil {
		//using tls connection
		c, err := smtp.NewClient(conn, s.host)
		if err != nil {
			return err
		}
		defer c.Close()
		if err := c.Auth(auth); err != nil {
			return err
		}
		if err := c.Mail(s.from); err != nil {return err}
		if err := c.Rcpt(to); err != nil {return err}
		w, err := c.Data()
		if err != nil {return err}
		_, err = w.Write([]byte(msg.String()))
		if err != nil {return err}
		err = w.Close()
		if err != nil {return err}
		return c.Quit()
	}

	//fallback to non-tls
	timeOutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- smtp.SendMail(addr, auth, s.from, []string{to}, []byte(msg.String()))
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Error().Err(err).Msg("Failed to send email")
		}
		return err
	case <-timeOutCtx.Done():
		return fmt.Errorf("timeout sending email to %s", to)
	}

}