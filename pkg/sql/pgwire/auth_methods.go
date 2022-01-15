package pgwire

import (
	"crypto/tls"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/hba"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// This file contains the methods that are accepted to perform
// authentication of users during the pgwire connection handshake.
//
// Which method are accepted for which user is selected using
// the HBA config loaded into the cluster setting
// server.host_based_authentication.configuration.
//
// Other methods can be added using RegisterAuthMethod(). This is done
// e.g. in the ICL modules to add support for GSS authentication using
// Kerberos.

func init() {
	// The "password" method requires a clear text password.
	//
	// Care should be taken by administrators to only accept this auth
	// method over secure connections, e.g. those encrypted using SSL.
	RegisterAuthMethod("password", authPassword, nil)

	// The "cert" method requires a valid client certificate for the
	// user attempting to connect.
	//
	// This method is only usable over SSL connections.
	RegisterAuthMethod("cert", authCert, nil)

	// The "cert-password" method requires either a valid client
	// certificate for the connecting user, or, if no cert is provided,
	// a cleartext password.
	RegisterAuthMethod("cert-password", authCertPassword, nil)

	// The "reject" method rejects any connection attempt that matches
	// the current rule.
	RegisterAuthMethod("reject", authReject, nil)

	// The "trust" method accepts any connection attempt that matches
	// the current rule.
	RegisterAuthMethod("trust", authTrust, nil)

}

// AuthMethod defines a method for authentication of a connection.
type AuthMethod func(c AuthConn, tlsState tls.ConnectionState, insecure bool, hashedPassword []byte,
	validUntil *tree.DTimestamp, encryption string, execCfg *sql.ExecutorConfig, entry *hba.Entry) (security.UserAuthHook, error)

func authPassword(
	c AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	validUntil *tree.DTimestamp,
	encryption string,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	if err := c.SendAuthRequest(authCleartextPassword, nil); err != nil {
		return nil, err
	}
	password, err := c.ReadPasswordString()
	if err != nil {
		return nil, err
	}

	if validUntil != nil {
		if validUntil.Sub(timeutil.Now()) < 0 {
			return nil, errors.New("the password is invalid")
		}
	}
	return security.UserAuthPasswordHook(
		insecure, encryption, password, hashedPassword,
	), nil
}

func authCert(
	c AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	validUntil *tree.DTimestamp,
	encryption string,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	if len(tlsState.PeerCertificates) == 0 {
		return nil, errors.New("no TLS peer certificates, but required for auth")
	}
	// Normalize the username contained in the certificate.
	tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
		tlsState.PeerCertificates[0].Subject.CommonName,
	).Normalize()
	return security.UserAuthCertHook(insecure, &tlsState)
}

func authCertPassword(
	c AuthConn,
	tlsState tls.ConnectionState,
	insecure bool,
	hashedPassword []byte,
	validUntil *tree.DTimestamp,
	encryption string,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	var fn AuthMethod
	if len(tlsState.PeerCertificates) == 0 {
		fn = authPassword
	} else {
		fn = authCert
	}
	return fn(c, tlsState, insecure, hashedPassword, validUntil, encryption, execCfg, entry)
}

func authTrust(
	_ AuthConn,
	_ tls.ConnectionState,
	_ bool,
	_ []byte,
	_ *tree.DTimestamp,
	_ string,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	return func(_ string, _ bool) error { return nil }, nil
}

func authReject(
	_ AuthConn,
	_ tls.ConnectionState,
	_ bool,
	_ []byte,
	_ *tree.DTimestamp,
	_ string,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	return func(_ string, _ bool) error {
		return errors.New("authentication rejected by configuration")
	}, nil
}
