// Copyright 2018  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package changefeedicl

import (
	"context"
	"encoding/base64"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/jobs/jobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/storage/dumpsink"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/humanizeutil"
)

// Sink is an interface for anything that a cdc may emit into.
type Sink interface {
	// EmitRow enqueues a row message for asynchronous delivery on the sink. An
	// error may be returned if a previously enqueued message has failed.
	EmitRow(
		ctx context.Context,
		table *sqlbase.TableDescriptor,
		key, value []byte,
		updated hlc.Timestamp,
		ddl bool,
		desc sqlbase.DescriptorProto,
		databaseName string,
		schemaName string,
		operate TableOperate,
		DB *client.DB,
		Executor sqlutil.InternalExecutor,
		poller *poller,
	) (bool, error)
	// EmitResolvedTimestamp enqueues a resolved timestamp message for
	// asynchronous delivery on every topic that has been seen by EmitRow. An
	// error may be returned if a previously enqueued message has failed.
	EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error
	// Flush blocks until every message enqueued by EmitRow and
	// EmitResolvedTimestamp has been acknowledged by the sink. If an error is
	// returned, no guarantees are given about which messages have been
	// delivered or not delivered.
	Flush(ctx context.Context) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
	GetTopics() (map[string]string, error)
}

// return sink instance depends on configuration
func getSink(
	sinkURI string,
	nodeID roachpb.NodeID,
	opts map[string]string,
	targets jobspb.ChangefeedTargets,
	settings *cluster.Settings,
	makeDumpSinkFromURI dumpsink.FromURIFactory,
	jobID int64,
) (Sink, error) {
	u, err := url.Parse(sinkURI)
	if err != nil {
		return nil, err
	}
	q := u.Query()

	// Use a function here to delay creation of the sink until after we've done
	// all the parameter verification.
	var makeSink func() (Sink, error)
	switch u.Scheme {
	case sinkSchemeBuffer:
		makeSink = func() (Sink, error) { return &bufferSink{}, nil }
	case sinkSchemeKafka:
		var cfg kafkaSinkConfig
		cfg.kafkaTopicPrefix = q.Get(sinkParamTopicPrefix)
		q.Del(sinkParamTopicPrefix)
		if schemaTopic := q.Get(sinkParamSchemaTopic); schemaTopic != `` {
			return nil, errors.Errorf(`%s is not yet supported`, sinkParamSchemaTopic)
		}
		q.Del(sinkParamSchemaTopic)
		if tlsBool := q.Get(sinkParamTLSEnabled); tlsBool != `` {
			var err error
			if cfg.tlsEnabled, err = strconv.ParseBool(tlsBool); err != nil {
				return nil, errors.Errorf(`param %s must be a bool: %s`, sinkParamTLSEnabled, err)
			}
		}
		q.Del(sinkParamTLSEnabled)
		if caCertHex := q.Get(sinkParamCACert); caCertHex != `` {
			// TODO(dan): There's a straightforward and unambiguous transformation
			// between the base 64 encoding defined in RFC 4648 and the URL variant
			// defined in the same RFC: simply replace all `+` with `-` and `/` with
			// `_`. Consider always doing this for the user and accepting either
			// variant.
			if cfg.caCert, err = base64.StdEncoding.DecodeString(caCertHex); err != nil {
				return nil, errors.Errorf(`param %s must be base 64 encoded: %s`, sinkParamCACert, err)
			}
		}
		q.Del(sinkParamCACert)

		saslParam := q.Get(sinkParamSASLEnabled)
		q.Del(sinkParamSASLEnabled)
		if saslParam != `` {
			b, err := strconv.ParseBool(saslParam)
			if err != nil {
				return nil, errors.Wrapf(err, `param %s must be a bool:`, sinkParamSASLEnabled)
			}
			cfg.saslEnabled = b
		}
		handshakeParam := q.Get(sinkParamSASLHandshake)
		q.Del(sinkParamSASLHandshake)
		if handshakeParam == `` {
			cfg.saslHandshake = true
		} else {
			if !cfg.saslEnabled {
				return nil, errors.Errorf(`%s must be enabled to configure SASL handshake behavior`, sinkParamSASLEnabled)
			}
			b, err := strconv.ParseBool(handshakeParam)
			if err != nil {
				return nil, errors.Wrapf(err, `param %s must be a bool:`, sinkParamSASLHandshake)
			}
			cfg.saslHandshake = b
		}
		cfg.saslUser = q.Get(sinkParamSASLUser)
		q.Del(sinkParamSASLUser)
		cfg.saslPassword = q.Get(sinkParamSASLPassword)
		q.Del(sinkParamSASLPassword)
		if cfg.saslEnabled {
			if cfg.saslUser == `` {
				return nil, errors.Errorf(`%s must be provided when SASL is enabled`, sinkParamSASLUser)
			}
			if cfg.saslPassword == `` {
				return nil, errors.Errorf(`%s must be provided when SASL is enabled`, sinkParamSASLPassword)
			}
		} else {
			if cfg.saslUser != `` {
				return nil, errors.Errorf(`%s must be enabled if a SASL user is provided`, sinkParamSASLEnabled)
			}
			if cfg.saslPassword != `` {
				return nil, errors.Errorf(`%s must be enabled if a SASL password is provided`, sinkParamSASLEnabled)
			}
		}

		makeSink = func() (Sink, error) {
			return makeKafkaSink(cfg, u.Host, targets)
		}
	case `experimental-s3`, `experimental-gs`, `experimental-nodelocal`, `experimental-http`,
		`experimental-https`, `experimental-azure`:
		fileSizeParam := q.Get(sinkParamFileSize)
		q.Del(sinkParamFileSize)
		var fileSize int64 = 16 << 20 // 16MB
		if fileSizeParam != `` {
			if fileSize, err = humanizeutil.ParseBytes(fileSizeParam); err != nil {
				return nil, errors.Wrapf(err, `parsing %s`, fileSizeParam)
			}
		}
		u.Scheme = strings.TrimPrefix(u.Scheme, `experimental-`)
		// Transfer "ownership" of validating all remaining query parameters to
		// ExportStorage.
		u.RawQuery = q.Encode()
		q = url.Values{}
		makeSink = func() (Sink, error) {
			return makeCloudStorageSink(u.String(), nodeID, fileSize, settings, opts, makeDumpSinkFromURI)
		}
	case sinkSchemeExperimentalSQL:
		// Swap the cdc prefix for the sql connection one that sqlSink
		// expects.
		u.Scheme = `postgres`
		// TODO(dan): Make tableName configurable or based on the job ID or
		// something.
		tableName := `sqlsink`
		makeSink = func() (Sink, error) {
			return makeSQLSink(u.String(), tableName, targets)
		}
		// Remove parameters we know about for the unknown parameter check.
		q.Del(`sslcert`)
		q.Del(`sslkey`)
		q.Del(`sslmode`)
		q.Del(`sslrootcert`)
	default:
		return nil, errors.Errorf(`unsupported sink: %s`, u.Scheme)
	}

	for k := range q {
		return nil, errors.Errorf(`unknown sink query parameter: %s`, k)
	}

	s, err := makeSink()
	if err != nil {
		return nil, err
	}
	return s, nil
}
