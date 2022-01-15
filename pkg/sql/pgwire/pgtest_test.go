// Copyright 2020  The Cockroach Authors.

package pgwire

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/pgproto3"
	"github.com/pkg/errors"
	"github.com/znbasedb/datadriven"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// Walk walks path for datadriven files and calls RunTest on them.
func Walk(t *testing.T, path, addr, user string) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		RunTest(t, path, addr, user)
	})
}

// RunTest executes PGTest commands, connecting to the database specified by
// addr and user. Supported commands:
//
// "send": Sends messages to a server. Takes a newline-delimited list of
// pgproto3.FrontendMessage types. Can fill in values by adding a space then
// a JSON object. No output.
//
// "until": Receives all messages from a server until messages of the given
// types have been seen. Converts them to JSON one per line as output. Takes
// a newline-delimited list of pgproto3.BackendMessage types. An ignore option
// can be used to specify types to ignore. ErrorResponse messages are
// immediately returned as errors unless they are the expected type, in which
// case they will marshal to an empty ErrorResponse message since our error
// detail specifics differ from Postgres.
//
// "receive": Like "until", but only output matching messages instead of all
// messages.
func RunTest(t *testing.T, path, addr, user string) {
	p, err := NewPGTest(context.Background(), addr, user)
	if err != nil {
		t.Fatal(err)
	}
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "send":
			for _, line := range strings.Split(d.Input, "\n") {
				sp := strings.SplitN(line, " ", 2)
				msg := toMessage(sp[0])
				if len(sp) == 2 {
					if err := json.Unmarshal([]byte(sp[1]), msg); err != nil {
						t.Fatal(err)
					}
				}
				if err := p.Send(msg.(pgproto3.FrontendMessage)); err != nil {
					t.Fatalf("%s: send %s: %v", d.Pos, line, err)
				}
			}
			return ""
		case "receive":
			until := parseMessages(d.Input)
			msgs, err := p.Receive(hasKeepErrMsg(d), until...)
			if err != nil {
				t.Fatalf("%s: %+v", d.Pos, err)
			}
			return msgsToJSONWithIgnore(msgs, d)
		case "until":
			until := parseMessages(d.Input)
			msgs, err := p.Until(hasKeepErrMsg(d), until...)
			if err != nil {
				t.Fatalf("%s: %+v", d.Pos, err)
			}
			return msgsToJSONWithIgnore(msgs, d)
		default:
			t.Fatalf("unknown command %s", d.Cmd)
			return ""
		}
	})
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func parseMessages(s string) []pgproto3.BackendMessage {
	var msgs []pgproto3.BackendMessage
	for _, typ := range strings.Split(s, "\n") {
		msgs = append(msgs, toMessage(typ).(pgproto3.BackendMessage))
	}
	return msgs
}

func hasKeepErrMsg(d *datadriven.TestData) bool {
	for _, arg := range d.CmdArgs {
		if arg.Key == "keepErrMessage" {
			return true
		}
	}
	return false
}

func msgsToJSONWithIgnore(msgs []pgproto3.BackendMessage, args *datadriven.TestData) string {
	ignore := map[string]bool{}
	errs := map[string]string{}
	for _, arg := range args.CmdArgs {
		switch arg.Key {
		case "keepErrMessage":
		case "ignore":
			for _, typ := range arg.Vals {
				ignore[fmt.Sprintf("*pgproto3.%s", typ)] = true
			}
		case "mapError":
			errs[arg.Vals[0]] = arg.Vals[1]
		default:
			panic(fmt.Errorf("unknown argument: %v", arg))
		}
	}
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, msg := range msgs {
		if ignore[fmt.Sprintf("%T", msg)] {
			continue
		}
		if errmsg, ok := msg.(*pgproto3.ErrorResponse); ok {
			code := errmsg.Code
			if v, ok := errs[code]; ok {
				code = v
			}
			if err := enc.Encode(struct {
				Type    string
				Code    string
				Message string `json:",omitempty"`
			}{
				Type:    "ErrorResponse",
				Code:    code,
				Message: errmsg.Message,
			}); err != nil {
				panic(err)
			}
		} else if err := enc.Encode(msg); err != nil {
			panic(err)
		}
	}
	return sb.String()
}

func toMessage(typ string) interface{} {
	switch typ {
	case "Bind":
		return &pgproto3.Bind{}
	case "Close":
		return &pgproto3.Close{}
	case "CommandComplete":
		return &pgproto3.CommandComplete{}
	case "DataRow":
		return &pgproto3.DataRow{}
	case "ErrorResponse":
		return &pgproto3.ErrorResponse{}
	case "Execute":
		return &pgproto3.Execute{}
	case "Parse":
		return &pgproto3.Parse{}
	case "Query":
		return &pgproto3.Query{}
	case "ReadyForQuery":
		return &pgproto3.ReadyForQuery{}
	case "Sync":
		return &pgproto3.Sync{}
	default:
		panic(fmt.Errorf("unknown type %q", typ))
	}
}

// PGTest can be used to send and receive arbitrary pgwire messages on
// Postgres-compatible servers.
type PGTest struct {
	fe   *pgproto3.Frontend
	conn net.Conn
}

// NewPGTest connects to a Postgres server at addr with username user.
func NewPGTest(ctx context.Context, addr, user string) (*PGTest, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()
	fe, err := pgproto3.NewFrontend(conn, conn)
	if err != nil {
		return nil, errors.Wrap(err, "new frontend")
	}
	if err := fe.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608, // Version 3.0
		Parameters: map[string]string{
			"user": user,
		},
	}); err != nil {
		return nil, errors.Wrap(err, "startup")
	}
	if msg, err := fe.Receive(); err != nil {
		return nil, errors.Wrap(err, "receive")
	} else if auth, ok := msg.(*pgproto3.Authentication); !ok || auth.Type != 0 {
		return nil, errors.Errorf("unexpected: %#v", msg)
	}
	p := &PGTest{
		fe:   fe,
		conn: conn,
	}
	_, err = p.Until(false /* keepErrMsg */, &pgproto3.ReadyForQuery{})
	success = err == nil
	return p, err
}

// Close sends a Terminate message and closes the connection.
func (p *PGTest) Close() error {
	defer p.conn.Close()
	return p.fe.Send(&pgproto3.Terminate{})
}

// Send sends msg to the serrver.
func (p *PGTest) Send(msg pgproto3.FrontendMessage) error {
	if testing.Verbose() {
		fmt.Printf("SEND %T: %+[1]v\n", msg)
	}
	return p.fe.Send(msg)
}

// Receive reads messages until messages of the given types have been found
// in the specified order (with any number of messages in between). It returns
// matched messages.
func (p *PGTest) Receive(
	keepErrMsg bool, typs ...pgproto3.BackendMessage,
) ([]pgproto3.BackendMessage, error) {
	var matched []pgproto3.BackendMessage
	for len(typs) > 0 {
		msgs, err := p.Until(keepErrMsg, typs[0])
		if err != nil {
			return nil, err
		}
		matched = append(matched, msgs[len(msgs)-1])
		typs = typs[1:]
	}
	return matched, nil
}

// Until is like Receive except all messages are returned instead of only
// matched messages.
func (p *PGTest) Until(
	keepErrMsg bool, typs ...pgproto3.BackendMessage,
) ([]pgproto3.BackendMessage, error) {
	var msgs []pgproto3.BackendMessage
	for len(typs) > 0 {
		typ := reflect.TypeOf(typs[0])

		// Receive messages and make copies of them.
		recv, err := p.fe.Receive()
		if err != nil {
			return nil, errors.Wrap(err, "receive")
		}
		if testing.Verbose() {
			fmt.Printf("RECV %T: %+[1]v\n", recv)
		}
		if errmsg, ok := recv.(*pgproto3.ErrorResponse); ok {
			if typ != typErrorResponse {
				return nil, errors.Errorf("waiting for %T, got %#v", typs[0], errmsg)
			}
			var message string
			if keepErrMsg {
				message = errmsg.Message
			}
			// ErrorResponse doesn't encode/decode correctly, so
			// manually append it here.
			msgs = append(msgs, &pgproto3.ErrorResponse{
				Code:    errmsg.Code,
				Message: message,
			})
			typs = typs[1:]
			continue
		}
		// If we saw a ready message but weren't waiting for one, we
		// might wait forever so bail.
		if msg, ok := recv.(*pgproto3.ReadyForQuery); ok && typ != typReadyForQuery {
			return nil, errors.Errorf("waiting for %T, got %#v", typs[0], msg)
		}
		data := recv.Encode(nil)
		// Trim off message type and length.
		data = data[5:]

		x := reflect.New(reflect.ValueOf(recv).Elem().Type())
		msg := x.Interface().(pgproto3.BackendMessage)
		if err := msg.Decode(data); err != nil {
			return nil, errors.Wrap(err, "decode")
		}
		msgs = append(msgs, msg)
		if typ == reflect.TypeOf(msg) {
			typs = typs[1:]
		}
	}
	return msgs, nil
}

var (
	typErrorResponse = reflect.TypeOf(&pgproto3.ErrorResponse{})
	typReadyForQuery = reflect.TypeOf(&pgproto3.ReadyForQuery{})
	flagAddr         = flag.String("addr", "", "pass a custom postgres address to TestWalk instead of starting an in-memory node")
	flagUser         = flag.String("user", "postgres", "username used if -addr is specified")
)

func TestPGTest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	addr := *flagAddr
	user := *flagUser
	if addr == "" {
		ctx := context.Background()
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Insecure: true,
		})
		defer s.Stopper().Stop(ctx)

		addr = s.ServingAddr()
		user = security.RootUser
	}

	Walk(t, "testdata/pgtest", addr, user)
}
