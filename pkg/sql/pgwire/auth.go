package pgwire

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/hba"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgwirebase"
)

const (
	// authOK is the pgwire auth response code for successful authentication
	// during the connection handshake.
	authOK int32 = 0
	// authCleartextPassword is the pgwire auth response code to request
	// a plaintext password during the connection handshake.
	authCleartextPassword int32 = 3
)

// handleAuthentication checks the connection's user. Errors are sent to the
// client and also returned.
//
// handleAuthentication should discuss with the client to arrange
// authentication and update c.sessionArgs with the authenticated user's
// name, if different from the one given initially. Note: at this
// point the sql.Session does not exist yet! If need exists to access the
// database to look up authentication data, use the internal executor.
func (c *conn) handleAuthentication(
	ctx context.Context,
	insecure bool,
	ie *sql.InternalExecutor,
	auth *hba.Conf,
	execCfg *sql.ExecutorConfig,
) (authErr error) {
	sendError := func(err error) error {
		_ /* err */ = writeErr(ctx, &execCfg.Settings.SV, err, &c.msgBuilder, c.conn)
		return err
	}

	// Check that the requested user exists and retrieve the hashed
	// password in case password authentication is needed.
	exists, hashedPassword, validUntil, encryption, limit, err := sql.GetUserHashedPassword(
		ctx, ie, &c.metrics.SQLMemMetrics, c.sessionArgs.User,
	)
	if err != nil {
		return sendError(err)
	}
	if !exists {
		return sendError(errors.Errorf(security.ErrPasswordUserAuthFailed, c.sessionArgs.User))
	}

	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		tlsState := tlsConn.ConnectionState()
		var methodFn AuthMethod
		var hbaEntry *hba.Entry

		if auth == nil {
			methodFn = authCertPassword
		} else if c.sessionArgs.User == security.RootUser {
			// If a hba.conf file is specified, hard code the root user to always use
			// cert auth or password auth.
			methodFn = authCertPassword
		} else {
			addr, _, err := net.SplitHostPort(c.conn.RemoteAddr().String())
			if err != nil {
				return sendError(err)
			}
			ip := net.ParseIP(addr)
			for _, entry := range auth.Entries {
				switch a := entry.Address.(type) {
				case *net.IPNet:
					if !a.Contains(ip) {
						continue
					}
				case hba.String:
					if !a.IsSpecial("all") {
						return sendError(errors.Errorf("unexpected %s address: %q", serverHBAConfSetting, a.Value))
					}
				default:
					return sendError(errors.Errorf("unexpected address type %T", a))
				}
				match := false
				for _, u := range entry.User {
					if u.IsSpecial("all") {
						match = true
						break
					}
					if u.Value == c.sessionArgs.User {
						match = true
						break
					}
				}
				if !match {
					continue
				}
				methodFn = hbaAuthMethods[entry.Method]
				if methodFn == nil {
					return sendError(errors.Errorf("unknown auth method %s", entry.Method))
				}
				hbaEntry = &entry
				break
			}
			if methodFn == nil {
				return sendError(errors.Errorf("no %s entry for host %q, user %q", serverHBAConfSetting, addr, c.sessionArgs.User))
			}
		}

		permit, remaining, err := sql.ApplyAuthPermit(ctx, ie, &c.metrics.SQLMemMetrics, c.sessionArgs.User)
		if err != nil {
			return sendError(err)
		}
		if !permit {
			return sendError(errors.Errorf("Your account has been locked. You can try again after %d second.", remaining))
		}
		authenticationHook, err := methodFn(c, tlsState, insecure, hashedPassword, validUntil, encryption, execCfg, hbaEntry)
		if err != nil {
			return sendError(err)
		}
		if err := authenticationHook(c.sessionArgs.User, true /* public */); err != nil {
			//handle err; add by ygl
			errMsg := fmt.Sprintf("%s", err)
			if errMsg[:39] == security.ErrPasswordUserAuthFailed[:39] {
				if err := sql.RecordUserAuthFail(ctx, ie, &c.metrics.SQLMemMetrics, c.sessionArgs.User); err != nil {
					return sendError(err)
				}
			}
			return sendError(err)
		}
		if err := sql.ResetUserAuthFail(ctx, ie, &c.metrics.SQLMemMetrics, c.sessionArgs.User); err != nil {
			return sendError(err)
		}
	}

	// log and check user sql connection
	if err := c.checkConnectionLimit(ctx, ie, c.sessionArgs.User, limit, execCfg); err != nil {
		return sendError(err)
	}

	c.msgBuilder.initMsg(pgwirebase.ServerMsgAuth)
	c.msgBuilder.putInt32(authOK)
	return c.msgBuilder.finishMsg(c.conn)
}
