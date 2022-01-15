package mail

import (
	"bytes"
	"context"
	"crypto/tls"
	"html/template"
	"net/smtp"
	"strings"
	"time"

	"github.com/znbasedb/znbase/pkg/security/audit/task"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/metric"
	"github.com/znbasedb/znbase/pkg/util/stop"
	"github.com/znbasedb/znbase/pkg/util/syncutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// Server is the boxes of the audit mail-sending struct
// TODO support more boxes next version
// Server is the boxes of the audit mail-sending struct
type Server struct {
	ctx    context.Context   // context from server
	cs     *cluster.Settings // cluster settings
	runner *task.Runner      // used to handle mail data
	mu     struct {
		syncutil.RWMutex
		base Base // base info updated by ticker
	}
	skip bool // skip send, used for test

	// TODO use hook instead of skip
	// hook func(data interface{}) error // update hook function whenever needed
}

// Base hold all base info for sending mails
type Base struct {
	tlsEnable  bool     // enable tls or not
	user       string   // username used to send mail
	password   string   // password for username
	smtpServer string   // smtp server with port
	smtpHost   string   // smtp server hostname
	receiver   []string // receiver mail addr
	ready      bool     // all settings has been pass through validation
}

// NewMailServer return a new initialed mail server
func NewMailServer(ctx context.Context, cs *cluster.Settings, mr *metric.Registry) *Server {
	if !util.EnableAQKK {
		return nil
	}
	// not show in report or SHOW ALL
	senderPass.SetConfidential()

	// define mail server
	server := &Server{
		ctx:  ctx,
		cs:   cs,
		skip: false,
	}
	server.runner = task.NewRunner(
		ctx,
		"mail_server",
		DefaultChannelCapacity,
		DefaultParallelism,
		DefaultStopTimeout,
		DefaultWorkTimeout,
		func(data interface{}) error {
			if server.skip {
				return nil
			}
			if server.mu.base.ready {
				if c, err := newClient(ctx, server.mu.base); c != nil {
					if err != nil {
						return err
					}
					log.Infof(ctx, "get writer")
					if cw, err := c.Data(); err == nil {
						log.Infof(ctx, "write mail content")
						if _, err := cw.Write([]byte(data.(string))); err != nil {
							log.Errorf(ctx, "write mail content, err:%s", err)
						}
						log.Infof(ctx, "send mail")
						if err := cw.Close(); err != nil {
							log.Errorf(ctx, "close mail content writer, err:%s", err)
						}
					} else {
						log.Errorf(ctx, "get writer, err:%s", err)
					}
					log.Infof(ctx, "quit client")
					if err := c.Quit(); err != nil {
						log.Errorf(ctx, "quit client, err:%s", err)
						return err
					}
				}
				return nil
			}
			log.Warningf(ctx, "mail server is not ready, base info:%v, alert:%s", server.mu.base, data.(string))
			return pgerror.NewErrorf(pgcode.MailServerNotReady, "mail server is not ready")
		},
		mr,
	)
	return server
}

// Start to send mail in box using new goroutine
func (ms *Server) Start(ctx context.Context, s *stop.Stopper) {
	if ms == nil {
		return
	}
	//	Start a goroutine to refresh settings every 'mail.refresh.interval' interval
	s.RunWorker(
		ctx, func(ct context.Context) {
			refresh := timeutil.NewTimer()
			defer refresh.Stop()
			refresh.Reset(time.Second)
			for {
				select {
				case <-refresh.C:
					log.Infof(ct, "refresh mail settings")
					if !ms.refreshSettings() {
						ms.mu.base.ready = false
					}
					// reset refresh interval
					refresh.Read = true
					refresh.Reset(time.Duration(refreshInterval.Get(&ms.cs.SV)) * time.Second)
				case <-s.ShouldStop():
					log.Infof(ctx, "stop refresh mail settings")
					return
				}
			}
		},
	)

	// Start a goroutine to send mail in box
	s.RunWorker(ctx, func(ct context.Context) {
		ms.runner.Start()
		for {
			select {
			case <-s.ShouldStop():
				log.Infof(ct, "stop mail server")
				ms.runner.Stop()
				return
			default:
				continue
			}
		}
	})
}

// Send a new mail with base template
// TODO support more base template
// Send a new mail with base template
func (ms *Server) Send(ctx context.Context, subject string, body string) error {
	if ms == nil {
		return nil
	}
	// get html template using 'HTMLTemplateBase'
	tpl, err := template.New("mail").Parse(HTMLTemplateBase)

	// get mail body
	if err == nil {
		data := struct {
			Subject string
			Content string
		}{
			Subject: subject,
			Content: body,
		}
		var buf bytes.Buffer
		if err := tpl.Execute(&buf, data); err == nil {
			body = buf.String()
		} else {
			log.Errorf(ctx, "get mail template, error:%s", err)
		}
	} else {
		log.Errorf(ctx, "get mail template, error:%s", err)
	}

	return ms.runner.PutData(mailBody(subject, body))
}

// UpdateSkip update value of skip to ignore send or not
func (ms *Server) UpdateSkip(skip bool) {
	if ms == nil {
		return
	}
	ms.skip = skip
}

// TODO reuse tcp connect to send mail until close connect manually instead of create new connect for every mail
// newClient get new client using new config
func newClient(ctx context.Context, base Base) (*smtp.Client, error) {
	var c *smtp.Client
	var e error
	if base.tlsEnable {
		log.Infof(ctx, "create tls connect")
		conn, _ := tls.Dial("tcp", base.smtpServer, &tls.Config{
			ServerName:         base.smtpServer,
			InsecureSkipVerify: true,
		})

		log.Infof(ctx, "create new tls client")
		if c, e = smtp.NewClient(conn, base.smtpServer); e != nil {
			log.Errorf(ctx, "create new tls client, err:%s", e)
			return nil, e

		}
	} else {
		log.Infof(ctx, "create new client")
		if c, e = smtp.Dial(base.smtpServer); e != nil {
			log.Errorf(ctx, "create new client, err:%s", e)
			return nil, e
		}
	}

	log.Infof(ctx, "create auth info")
	host := base.smtpHost
	if base.tlsEnable {
		host = base.smtpServer
	}
	auth := smtp.PlainAuth(SenderIdentity, base.user, base.password, host)

	log.Infof(ctx, "say hello")
	if err := c.Hello("localhost"); err != nil {
		log.Errorf(ctx, "say hello, err:%s", err)
		return nil, c.Quit()
	}

	if !base.tlsEnable {
		log.Infof(ctx, "send tls command")
		if ok, _ := c.Extension("STARTTLS"); ok {
			config := &tls.Config{ServerName: base.smtpServer, InsecureSkipVerify: true}
			if err := c.StartTLS(config); err != nil {
				log.Errorf(ctx, "send tls command, err:%s", err)
				return nil, c.Quit()
			}
		}
	}

	log.Infof(ctx, "set mail auth")
	if err := c.Auth(auth); err != nil {
		log.Errorf(ctx, "set mail auth, err:%s", err)
		return nil, c.Quit()
	}

	log.Infof(ctx, "update smtp sender")
	if err := c.Mail(base.user); err != nil {
		log.Errorf(ctx, "update smtp sender, err:%s", err)
		return nil, c.Quit()
	}

	log.Infof(ctx, "update smtp receiver")
	for _, r := range base.receiver {
		if err := c.Rcpt(r); err != nil {
			log.Errorf(ctx, "update smtp receiver, err:%s", err)
			return nil, c.Quit()
		}
	}

	return c, nil
}

// TODO more check need to do, check smtp server format -> host:port
// refreshSettings refresh mail settings
func (ms *Server) refreshSettings() bool {
	if ms == nil {
		return true
	}
	base := Base{}
	base.smtpServer = senderSMTP.Get(&ms.cs.SV)
	if strings.EqualFold(base.smtpServer, "") {
		return false
	}
	base.user = senderUser.Get(&ms.cs.SV)
	if strings.EqualFold(base.user, "") {
		return false
	}
	base.tlsEnable = tlsEnable.Get(&ms.cs.SV)
	base.password = senderPass.Get(&ms.cs.SV)
	if strings.EqualFold(base.password, "") {
		return false
	}
	receiver := receiver.Get(&ms.cs.SV)
	if strings.EqualFold(receiver, "") {
		return false
	}
	base.receiver = strings.Split(receiver, ",")
	base.smtpHost = strings.Split(base.smtpServer, ":")[0]
	base.ready = true
	ms.mu.Lock()
	ms.mu.base = base
	ms.mu.Unlock()
	return true
}

// get final mail html content
func mailBody(subject string, body string) string {
	var result strings.Builder
	result.WriteString("Subject:")
	result.WriteString(subject)
	result.WriteString("\nContent-Type: text/html; charset=\"UTF-8\";\r\n")
	result.WriteString(body)
	return result.String()
}

// TestConfig config for test
func (ms *Server) TestConfig() {
	if ms == nil {
		return
	}
	ms.UpdateSkip(true)
	ms.runner.TestConfig()
}
