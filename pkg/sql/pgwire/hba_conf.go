package pgwire

import (
	"context"
	"sort"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/security/hba"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/util/errorutil/unimplemented"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// This file contains the logic for the configuration of HBA rules.
//
// In a nutshell, administrators customize the cluster setting
// `server.host_based_authentication.configuration`; each time they
// do so, all the nodes parse this configuration and re-initialize
// their authentication rules (a list of entries) from the setting.
//
// The HBA configuration is an ordered list of rules. Each time
// a client attempts to connect, the server scans the
// rules from the beginning of the list. The list rule that
// matches the connection decides how to authenticate.
//
// The syntax is inspired/derived from that of PostgreSQL's pg_hba.conf:
// https://www.postgresql.org/docs/12/auth-pg-hba-conf.html
//
// For now, ZNBaseDB only supports the following syntax:
//
//     host  all  <user[,user]...>  <IP-address/mask-length>  <auth-method>
//
// The matching rules are as follows:
// - A rule matches if the connecting username matches either of the
//   usernames listed in the rule, or if the pseudo-user 'all' is
//   present in the user column.
// - A rule matches if the connection client's IP address is included
//   in the network address specified in the CIDR notation.
//

// serverHBAConfSetting is the name of the cluster setting that holds
// the HBA configuration.
const serverHBAConfSetting = "server.host_based_authentication.configuration"

// connAuthConf is the cluster setting that holds the HBA configuration.
var connAuthConf = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		serverHBAConfSetting,
		"host-based authentication configuration to use during connection authentication",
		"",
		checkHBASyntaxBeforeUpdatingSetting,
	)
	return s
}()

// loadLocalAuthConfigUponRemoteSettingChange initializes the local node's cache
// of the auth configuration each time the cluster setting is updated.
func loadLocalAuthConfigUponRemoteSettingChange(
	ctx context.Context, server *Server, st *cluster.Settings,
) {
	val := connAuthConf.Get(&st.SV)
	server.auth.Lock()
	defer server.auth.Unlock()
	if val == "" {
		server.auth.conf = nil
		return
	}
	conf, err := hba.Parse(val)
	if err != nil {
		log.Warningf(ctx, "invalid %s: %v", serverHBAConfSetting, err)
		conf = nil
	}
	// Usernames are normalized during session init. Normalize the HBA usernames
	// in the same way.
	for _, entry := range conf.Entries {
		for iu := range entry.User {
			user := &entry.User[iu]
			user.Value = tree.Name(user.Value).Normalize()
		}
	}
	server.auth.conf = conf
}

// checkHBASyntaxBeforeUpdatingSetting is run by the SQL gateway each time
// a client attempts to update the cluster setting.
func checkHBASyntaxBeforeUpdatingSetting(values *settings.Values, s string) error {
	if s == "" {
		// An empty configuration is always valid.
		return nil
	}
	conf, err := hba.Parse(s)
	if err != nil {
		return err
	}
	for _, entry := range conf.Entries {
		for _, db := range entry.Database {
			if !db.IsSpecial("all") {
				return errors.WithHint(unimplemented.New("hba-per-db", "per-database HBA rules are not supported"),
					"Use the special value 'all' (without quotes) to match all databases.")
			}
		}
		if addr, ok := entry.Address.(hba.String); ok && !addr.IsSpecial("all") {
			return errors.WithHint(unimplemented.New("hba-hostnames", "hostname-based HBA rules are not supported"),
				"List the numeric CIDR notation instead, for example: 123.0.0.1/8.")
		}
		if hbaAuthMethods[entry.Method] == nil {
			return errors.WithHintf(unimplemented.Newf("hba-method-"+entry.Method,
				"unknown auth method %q", entry.Method),
				"Supported methods: %s", listRegisteredMethods())
		}
		if check := hbaCheckHBAEntries[entry.Method]; check != nil {
			if err := check(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// RegisterAuthMethod registers an AuthMethod for pgwire
// authentication and for use in HBA configuration.
//
// The checkEntry method, if provided, is called upon configuration
// the cluster setting in the SQL client which attempts to change the
// configuration. It can block the configuration if e.g. the syntax is
// invalid.
func RegisterAuthMethod(method string, fn AuthMethod, checkEntry CheckHBAEntry) {
	hbaAuthMethods[method] = fn
	if checkEntry != nil {
		hbaCheckHBAEntries[method] = checkEntry
	}
}

// listRegisteredMethods returns a sorted, comma-delimited list
// of registered AuthMethods.
func listRegisteredMethods() string {
	methods := make([]string, 0, len(hbaAuthMethods))
	for method := range hbaAuthMethods {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	return strings.Join(methods, ", ")
}

var (
	hbaAuthMethods     = map[string]AuthMethod{}
	hbaCheckHBAEntries = map[string]CheckHBAEntry{}
)

// CheckHBAEntry defines a method for validating an hba Entry upon
// configuration of the cluster setting by a SQL client.
type CheckHBAEntry func(hba.Entry) error
