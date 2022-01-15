// Copyright 2016  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package dumpsink

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/blobs"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/util/contextutil"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/retry"
	"github.com/znbasedb/znbase/pkg/util/sysutil"
	"github.com/znbasedb/znbase/pkg/workload"
	"golang.org/x/net/html"
)

const (
	// S3AccessKeyParam is the query parameter for access_key in an S3 URI.
	S3AccessKeyParam = "AWS_ACCESS_KEY_ID"
	// S3SecretParam is the query parameter for the 'secret' in an S3 URI.
	S3SecretParam = "AWS_SECRET_ACCESS_KEY"
	// S3TempTokenParam is the query parameter for session_token in an S3 URI.
	S3TempTokenParam = "AWS_SESSION_TOKEN"
	// S3EndpointParam is the query parameter for the 'endpoint' in an S3 URI.
	S3EndpointParam = "AWS_ENDPOINT"
	// S3RegionParam is the query parameter for the 'endpoint' in an S3 URI.
	S3RegionParam = "AWS_REGION"

	// AuthParam is the query parameter for the cluster settings named
	// key in a URI.
	// AuthParam          = "AUTH"
	// authParamImplicit  = "implicit"
	// authParamDefault   = "default"
	// authParamSpecified = "specified"

	// CredentialsParam is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	// CredentialsParam = "CREDENTIALS"

	cloudsinkPrefix = "cloudsink"
	cloudsinkHTTP   = cloudsinkPrefix + ".http"

	cloudsinkHTTPCASetting = cloudsinkHTTP + ".custom_ca"

	cloudsinkTimeout = cloudsinkPrefix + ".timeout"

	hdfsUserParm     = "user"
	hdfsKrb5confParm = "krb5Conf"
	hdfsRealmParm    = "realm"
	hdfsPasswordParm = "password"
	hdfsNamenodeParm = "nnPrincipal"
	// hdfsConfPref     = "conf"

	hdfsDefaultUser = "hdfs"

	kafkaTopicParm = "topic"
)

// ErrFileDoesNotExist is a sentinel error for indicating that a specified
// bucket/object/key/file (depending on storage terminology) does not exist.
// This error is raised by the ReadFile method.
var ErrFileDoesNotExist = errors.New("external_storage: file doesn't exist")

// HTTPRetryOptions defines the tunable settings which control the retry of HTTP
// operations.
var HTTPRetryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	MaxRetries:     32,
	Multiplier:     4,
}

// ConfFromURI generates an DumpSink config from a URI string.
func ConfFromURI(ctx context.Context, path string) (roachpb.DumpSink, error) {
	conf := roachpb.DumpSink{}
	uri, err := url.Parse(path)
	if err != nil {
		return conf, err
	}
	switch uri.Scheme {
	case "s3":
		conf.Provider = roachpb.ExportStorageProvider_S3
		conf.S3Config = &roachpb.DumpSink_S3{
			Bucket:    uri.Host,
			Prefix:    uri.Path,
			AccessKey: uri.Query().Get(S3AccessKeyParam),
			Secret:    uri.Query().Get(S3SecretParam),
			TempToken: uri.Query().Get(S3TempTokenParam),
			Endpoint:  uri.Query().Get(S3EndpointParam),
			Region:    uri.Query().Get(S3RegionParam),
		}
		if conf.S3Config.AccessKey == "" {
			return conf, errors.Errorf("s3 uri missing %q parameter", S3AccessKeyParam)
		}
		if conf.S3Config.Secret == "" {
			return conf, errors.Errorf("s3 uri missing %q parameter", S3SecretParam)
		}
		conf.S3Config.Prefix = strings.TrimLeft(conf.S3Config.Prefix, "/")
		// AWS secrets often contain + characters, which must be escaped when
		// included in a query string; otherwise, they represent a space character.
		// More than a few users have been bitten by this.
		//
		// Luckily, AWS secrets are base64-encoded data and thus will never actually
		// contain spaces. We can convert any space characters we see to +
		// characters to recover the original secret.
		conf.S3Config.Secret = strings.Replace(conf.S3Config.Secret, " ", "+", -1)
	case "http", "https":
		conf.Provider = roachpb.ExportStorageProvider_Http
		conf.HttpPath.BaseUri = path
	case "nodelocal":
		nodeID, err := strconv.Atoi(uri.Host)
		if err != nil && uri.Host != "" {
			return conf, errors.Errorf("host component of nodelocal URI must be a node ID: %s", path)
		}
		conf.Provider = roachpb.ExportStorageProvider_LocalFile
		conf.LocalFile.Path = uri.Path
		conf.LocalFile.NodeID = roachpb.NodeID(nodeID)
	case "hdfs":
		conf.Provider = roachpb.ExportStorageProvider_HDFS
		values := uri.Query()
		conf.HdfsConfig = &roachpb.DumpSink_HDFS{
			Host:              uri.Host,
			Path:              uri.Path,
			Username:          values.Get(hdfsUserParm),
			Krb5Conf:          values.Get(hdfsKrb5confParm),
			Realm:             values.Get(hdfsRealmParm),
			Password:          values.Get(hdfsPasswordParm),
			NamenodePrincipal: values.Get(hdfsNamenodeParm),
		}
		if conf.HdfsConfig.Username == "" {
			conf.HdfsConfig.Username = hdfsDefaultUser
		}
		if conf.HdfsConfig.Path == "" {
			//默认在用户目录下
			conf.HdfsConfig.Path = "/user/" + conf.HdfsConfig.Username
		}
		var buf bytes.Buffer
		for k := range values {
			if strings.HasPrefix(k, "conf_") {
				buf.WriteString(strings.TrimPrefix(k, "conf_"))
				buf.WriteString("=")
				buf.WriteString(values.Get(k))
				buf.WriteString(",")
			}
		}
		conf.HdfsConfig.HdfsConfigs = buf.String()
		log.Warningf(ctx, "HDFS Kerberos config: %s", conf.HdfsConfig.Krb5Conf)

	case "kafka":
		conf.Provider = roachpb.ExportStorageProvider_KAFKA
		conf.KafkaConfig.KafkaServer = path
		conf.KafkaConfig.Topic = uri.Query().Get(kafkaTopicParm)
	case "experimental-workload":
		conf.Provider = roachpb.ExportStorageProvider_Workload
		if conf.WorkloadConfig, err = ParseWorkloadConfig(uri); err != nil {
			return conf, err
		}
	default:
		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
	}
	return conf, nil
}

// FromURI returns an DumpSink for the given URI.
func FromURI(
	ctx context.Context,
	uri string,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (DumpSink, error) {
	conf, err := ConfFromURI(ctx, uri)
	if err != nil {
		return nil, err
	}
	return MakeDumpSink(ctx, conf, settings, blobClientFactory)
}

// SanitizeDumpSinkURI returns the export storage URI with sensitive
// credentials stripped.
func SanitizeDumpSinkURI(path string) (string, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if uri.Scheme == "experimental-workload" {
		return path, nil
	}
	// All current export storage providers store credentials in the query string,
	// if they store it in the URI at all.
	uri.RawQuery = ""
	return uri.String(), nil
}

// MakeDumpSink creates an DumpSink from the given config.
func MakeDumpSink(
	ctx context.Context,
	dest roachpb.DumpSink,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (DumpSink, error) {
	switch dest.Provider {
	case roachpb.ExportStorageProvider_LocalFile:
		telemetry.Count("external-io.nodelocal")
		return makeLocalSink(ctx, dest.LocalFile, settings, blobClientFactory)
	case roachpb.ExportStorageProvider_Http:
		telemetry.Count("external-io.http")
		return makeHTTPSink(dest.HttpPath.BaseUri, settings, dest.HttpPath.Header)
	case roachpb.ExportStorageProvider_S3:
		telemetry.Count("external-io.s3")
		return makeS3Sink(ctx, dest.S3Config, settings)
	case roachpb.ExportStorageProvider_Workload:
		if err := settings.Version.CheckVersion(
			cluster.VersionExportStorageWorkload, "experimental-workload",
		); err != nil {
			return nil, err
		}
		telemetry.Count("external-io.workload")
		return makeWorkloadSink(dest.WorkloadConfig)
	case roachpb.ExportStorageProvider_HDFS:
		telemetry.Count("external-io.hdfs")
		return MakeHdfsSink(ctx, dest.HdfsConfig, settings)
	case roachpb.ExportStorageProvider_KAFKA:
		telemetry.Count("external-io.kafka")
		return makeKafkaSink(ctx, dest.S3Config, settings)
	}
	return nil, errors.Errorf("unsupported export destination type: %s", dest.Provider.String())
}

// Factory describes a factory function for ExternalStorage.
type Factory func(ctx context.Context, dest roachpb.DumpSink) (DumpSink, error)

// FromURIFactory describes a factory function for ExternalStorage given a URI.
type FromURIFactory func(ctx context.Context, uri string) (DumpSink, error)

// DumpSink provides functions to read and write files in some storage,
// namely various cloud storage providers, for example to store backups.
// Generally an implementation is instantiated pointing to some base path or
// prefix and then gets and puts files using the various methods to interact
// with individual files contained within that path or prefix.
// However, implementations must also allow callers to provide the full path to
// a given file as the "base" path, and then read or write it with the methods
// below by simply passing an empty filename. Implementations that use stdlib's
// `path.Join` to concatenate their base path with the provided filename will
// find its semantics well suited to this -- it elides empty components and does
// not append surplus slashes.
type DumpSink interface {
	io.Closer

	// Conf should return the serializable configuration required to reconstruct
	// this DumpSink implementation.
	Conf() roachpb.DumpSink

	// ReadFile should return a Reader for requested name.
	ReadFile(ctx context.Context, basename string) (io.ReadCloser, error)
	// Write sends the named payload to the requested node.
	// This method will read entire content of file and send
	// it over to another node, based on the nodeID.
	Write(ctx context.Context, file string, content io.ReadSeeker) error
	// WriteFile should write the content to requested name.
	WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error

	// Delete removes the named file from the store.
	Delete(ctx context.Context, basename string) error

	// Size returns the length of the named file in bytes.
	Size(ctx context.Context, basename string) (int64, error)

	// ListFiles returns files that match a globs-style pattern. The returned
	// results are usually relative to the base path, meaning an ExternalStorage
	// instance can be initialized with some base path, used to query for files,
	// then pass those results to its other methods.
	//
	// As a special-case, if the passed patternSuffix is empty, the base path used
	// to initialize the storage connection is treated as a pattern. In this case,
	// as the connection is not really reusable for interacting with other files
	// and there is no clear definition of what it would mean to be relative to
	// that, the results are fully-qualified absolute URIs. The base URI is *only*
	// allowed to contain globs-patterns when the explicit patternSuffix is "".
	ListFiles(ctx context.Context, patternSuffix string) ([]string, error)

	//WriteFileWithReader write file with io.Reader
	WriteFileWithReader(ctx context.Context, file string, content io.Reader) error

	Seek(ctx context.Context, filename string, offset, endOffset int64, whence int) (io.ReadCloser, error)

	SetConfig(header string) error
}

var (
	// HTTPCustomCA *settings.StringSetting
	//*
	HTTPCustomCA = settings.RegisterStringSetting(
		cloudsinkHTTPCASetting,
		"custom root CA (appended to system's default CAs) for verifying certificates when interacting with HTTPS storage",
		"",
	) //*/
	// TimeoutSetting *settings.DurationSetting
	TimeoutSetting = settings.RegisterDurationSetting(
		cloudsinkTimeout,
		"the timeout for load/dump storage operations",
		10*time.Minute)
)

// LocalFileSink provides functions to read and write files in some storage,
// namely various cloud storage providers, for example to store backups.
// Generally an implementation is instantiated pointing to some base path or
// prefix and then gets and puts files using the various methods to interact
// with individual files contained within that path or prefix.
// However, implementations must also allow callers to provide the full path to
// a given file as the "base" path, and then read or write it with the methods
// below by simply passing an empty filename. Implementations that use stdlib's
// `path.Join` to concatenate their base path with the provided filename will
// find its semantics well suited to this -- it elides empty components and does
// not append surplus slashes.
type LocalFileSink struct {
	cfg        roachpb.DumpSink_LocalFilePath // contains un-prefixed filepath -- DO NOT use for I/O ops.
	base       string                         // relative filepath prefixed with externalIODir, for I/O ops on this node.
	blobClient blobs.BlobClient               // inter-node file sharing service
}

func (l *LocalFileSink) Write(ctx context.Context, file string, content io.ReadSeeker) error {
	return l.blobClient.Write(ctx, file, content)
}

//Seek Specify the offset to read, get the corresponding io
func (l *LocalFileSink) Seek(
	ctx context.Context, filename string, offset, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return l.blobClient.Seek(ctx, joinRelativePath(l.base, filename), offset, whence)
}

//WriteFileWithReader write file with id.Reader
func (l *LocalFileSink) WriteFileWithReader(
	ctx context.Context, basename string, content io.Reader,
) error {
	return l.blobClient.WriteFileWithReader(ctx, joinRelativePath(l.base, basename), content)
}

//SetConfig set config
func (l *LocalFileSink) SetConfig(header string) error {
	return errors.New("LocalFileSink does not support setting the head")
}

func joinRelativePath(filePath string, file string) string {
	// Joining "." to make this a relative path.
	// This ensures path.Clean does not simplify in unexpected ways.
	return filepath.Join(".", filePath, file)
}

//IsNodeLocal return Whether the current node is a local node
func (l *LocalFileSink) IsNodeLocal() bool {
	return l.blobClient.IsNodeLocal()
}

//AbsPath returns a absolute filepath
func (l *LocalFileSink) AbsPath() string {
	return filepath.Join(l.blobClient.(*blobs.LocalClient).LocalStorage.ExternalIODir, l.base)
}

//RecursionLogList get log files recursively
func (l *LocalFileSink) RecursionLogList(ctx context.Context) ([]string, error) {
	pattern := l.base
	return l.blobClient.RecursionLogList(ctx, pattern)
}

// ListFiles returns files that match a globs-style pattern. The returned
// results are usually relative to the base path, meaning an ExternalStorage
// instance can be initialized with some base path, used to query for files,
// then pass those results to its other methods.
//
// As a special-case, if the passed patternSuffix is empty, the base path used
// to initialize the storage connection is treated as a pattern. In this case,
// as the connection is not really reusable for interacting with other files
// and there is no clear definition of what it would mean to be relative to
// that, the results are fully-qualified absolute URIs. The base URI is *only*
// allowed to contain globs-patterns when the explicit patternSuffix is "".
func (l *LocalFileSink) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	pattern := l.base
	var fileList []string
	matches, err := l.blobClient.List(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}

	for _, fileName := range matches {
		if strings.HasSuffix(fileName, patternSuffix) {
			fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(fileName, l.base), string(os.PathSeparator)))
		}
	}

	return fileList, nil
}

var _ DumpSink = &LocalFileSink{}

// MakeLocalSinkURI converts a local path (should always be relative) to a
// valid nodelocal URI.
func MakeLocalSinkURI(path string) string {
	return fmt.Sprintf("nodelocal:///%s", path)
}

// MakeSpecifyLocalSinkURI converts a local path (should always be relative) to a
// valid nodelocal URI.
func MakeSpecifyLocalSinkURI(path string, id roachpb.NodeID) string {
	return fmt.Sprintf("nodelocal://%d/%s", id, path)
}
func makeLocalSink(
	ctx context.Context,
	cfg roachpb.DumpSink_LocalFilePath,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (DumpSink, error) {
	if cfg.Path == "" {
		return nil, errors.Errorf("Local storage requested but path not provided")
	}
	client, err := blobClientFactory(ctx, cfg.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create blob client")
	}

	// In non-server execution we have no settings and no restriction on local IO.
	if settings != nil {
		if settings.ExternalIODir == "" {
			return nil, errors.Errorf("local file access is disabled")
		}
	}
	return &LocalFileSink{base: cfg.Path, cfg: cfg, blobClient: client}, nil
}

// Conf should return the serializable configuration required to reconstruct
// this DumpSink implementation.
func (l *LocalFileSink) Conf() roachpb.DumpSink {
	return roachpb.DumpSink{
		Provider:  roachpb.ExportStorageProvider_LocalFile,
		LocalFile: l.cfg,
	}
}

// WriteFile should write the content to requested name.
func (l *LocalFileSink) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	return l.blobClient.WriteFile(ctx, joinRelativePath(l.base, basename), content)
}

// ReadFile should return a Reader for requested name.
func (l *LocalFileSink) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	return l.blobClient.ReadFile(ctx, joinRelativePath(l.base, basename))
}

// Delete removes the named file from the store.
func (l *LocalFileSink) Delete(ctx context.Context, basename string) error {
	return l.blobClient.Delete(ctx, joinRelativePath(l.base, basename))
}

// Size returns the length of the named file in bytes.
func (l *LocalFileSink) Size(ctx context.Context, basename string) (int64, error) {
	stat, err := l.blobClient.Stat(ctx, joinRelativePath(l.base, basename))
	if err != nil {
		return 0, nil
	}
	return stat.Filesize, nil
}

// Close after the first call is undefined.
// Specific implementations may document their own behavior.
func (*LocalFileSink) Close() error {
	return nil
}

type httpSink struct {
	base     *url.URL
	client   *http.Client
	hosts    []string
	settings *cluster.Settings
	header   string
}

func (h *httpSink) Write(ctx context.Context, file string, content io.ReadSeeker) error {
	return errors.New("http does not support additional writing")
}
func (h *httpSink) Seek(
	ctx context.Context, filename string, offset, endOffset int64, whence int,
) (io.ReadCloser, error) {
	body, err := newResumingHTTPReaderWithSeek(ctx, h, filename, offset, endOffset)
	if err != nil {
		return nil, err
	}
	nopCloser := ioutil.NopCloser(body)
	return nopCloser, nil
}

func (h *httpSink) WriteFileWithReader(
	ctx context.Context, basename string, content io.Reader,
) error {
	return errors.Errorf(`httpSink storage does not support WriteFileWithReader `)
}

func (h *httpSink) SetConfig(header string) error {
	h.header = header
	return nil
}

func visit(links []string, n *html.Node) []string {
	if n.Type == html.ElementNode && n.Data == "a" {
		for _, a := range n.Attr {
			if a.Key == "href" {
				links = append(links, a.Val)
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		links = visit(links, c)
	}
	return links
}

// ListFiles returns files that match a globs-style pattern. The returned
// results are usually relative to the base path, meaning an ExternalStorage
// instance can be initialized with some base path, used to query for files,
// then pass those results to its other methods.
//
// As a special-case, if the passed patternSuffix is empty, the base path used
// to initialize the storage connection is treated as a pattern. In this case,
// as the connection is not really reusable for interacting with other files
// and there is no clear definition of what it would mean to be relative to
// that, the results are fully-qualified absolute URIs. The base URI is *only*
// allowed to contain globs-patterns when the explicit patternSuffix is "".
func (h *httpSink) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	httpMap := make(map[string]string)
	if h.header != "" {
		buf := bytes.NewBufferString(h.header)
		if len(buf.Bytes()) != 0 {
			err := json.Unmarshal(buf.Bytes(), &httpMap)
			if err != nil {
				return nil, err
			}
		}
	}
	//尝试实现获取文件名列表
	resp, err := h.req(ctx, "GET", "", nil, httpMap)
	if err != nil {
		return nil, err
	}
	doc, err := html.Parse(resp.Body)
	var links []string
	for _, link := range visit(nil, doc) {
		if strings.HasSuffix(link, patternSuffix) {
			links = append(links, link)
		}
	}
	return links, nil
}

var _ DumpSink = &httpSink{}

func makeHTTPClient(settings *cluster.Settings) (*http.Client, error) {
	var tlsConf *tls.Config
	if pem := HTTPCustomCA.Get(&settings.SV); pem != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "could not load system root CA pool")
		}
		if !roots.AppendCertsFromPEM([]byte(pem)) {
			return nil, errors.Errorf("failed to parse root CA certificate from %q", pem)
		}
		tlsConf = &tls.Config{RootCAs: roots}
	}
	// Copy the defaults from http.DefaultTransport. We cannot just copy the
	// entire struct because it has a sync Mutex. This has the unfortunate problem
	// that if Go adds fields to DefaultTransport they won't be copied here,
	// but this is ok for now.
	t := http.DefaultTransport.(*http.Transport)
	httpClient := &http.Client{Transport: &http.Transport{
		Proxy:                 t.Proxy,
		DialContext:           t.DialContext,
		MaxIdleConns:          t.MaxIdleConns,
		IdleConnTimeout:       t.IdleConnTimeout,
		TLSHandshakeTimeout:   t.TLSHandshakeTimeout,
		ExpectContinueTimeout: t.ExpectContinueTimeout,

		// Add our custom CA.
		TLSClientConfig: tlsConf,
	}}
	return httpClient, nil
}

func makeHTTPSink(base string, settings *cluster.Settings, header string) (DumpSink, error) {
	if base == "" {
		return nil, errors.Errorf("HTTP sink requested but base path not provided")
	}

	client, err := makeHTTPClient(settings)
	if err != nil {
		return nil, err
	}
	uri, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	return &httpSink{
		base:     uri,
		client:   client,
		hosts:    strings.Split(uri.Host, ","),
		settings: settings,
		header:   header,
	}, nil
}

// Conf should return the serializable configuration required to reconstruct
// this DumpSink implementation.
func (h *httpSink) Conf() roachpb.DumpSink {
	return roachpb.DumpSink{
		Provider: roachpb.ExportStorageProvider_Http,
		HttpPath: roachpb.DumpSink_Http{
			BaseUri: h.base.String(),
		},
	}
}

type resumingHTTPReader struct {
	body      io.ReadCloser
	canResume bool  // Can we resume if download aborts prematurely?
	pos       int64 // How much data was received so far.
	ctx       context.Context
	url       string
	client    *httpSink
	offset    int64
}

var _ io.ReadCloser = &resumingHTTPReader{}

func newResumingHTTPReader(
	ctx context.Context, client *httpSink, url string,
) (*resumingHTTPReader, error) {
	r := &resumingHTTPReader{
		ctx:    ctx,
		client: client,
		url:    url,
	}
	httpMap := make(map[string]string)

	if client.header != "" {
		buf := bytes.NewBufferString(client.header)
		if len(buf.Bytes()) != 0 {
			err := json.Unmarshal(buf.Bytes(), &httpMap)
			if err != nil {
				return nil, err
			}
		}
	}

	resp, err := r.sendRequest(httpMap)
	if err != nil {
		return nil, err
	}

	r.canResume = resp.Header.Get("Accept-Ranges") == "bytes"
	r.body = resp.Body
	return r, nil
}

func newResumingHTTPReaderWithSeek(
	ctx context.Context, client *httpSink, url string, offset, endOffset int64,
) (*resumingHTTPReader, error) {
	r := &resumingHTTPReader{
		ctx:    ctx,
		client: client,
		url:    url,
		offset: offset,
	}
	header := fmt.Sprintf("bytes=%v-%v", offset, endOffset)
	headers := make(map[string]string)

	if client.header != "" {
		buf := bytes.NewBufferString(client.header)
		if len(buf.Bytes()) != 0 {
			err := json.Unmarshal(buf.Bytes(), &headers)
			if err != nil {
				return nil, err
			}
		}
	}
	headers["Range"] = header
	resp, err := r.sendRequest(headers)
	if err != nil {
		return nil, err
	}
	r.canResume = resp.Header.Get("Accept-Ranges") == "bytes"
	r.body = resp.Body
	return r, nil
}

func (r *resumingHTTPReader) Close() error {
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}

// checkHTTPContentRangeHeader parses Content-Range header and
// ensures that range start offset is the same as 'pos'
// See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range
func checkHTTPContentRangeHeader(h string, pos int64) error {
	if len(h) == 0 {
		return errors.New("http server does not honor download resume")
	}

	h = strings.TrimPrefix(h, "bytes ")
	dash := strings.IndexByte(h, '-')
	if dash <= 0 {
		return errors.Errorf("malformed Content-Range header: %s", h)
	}

	resume, err := strconv.ParseInt(h[:dash], 10, 64)
	if err != nil {
		return errors.Errorf("malformed start offset in Content-Range header: %s", h)
	}

	if resume != pos {
		log.Errorf(context.TODO(),
			"expected resume position %d, found %d instead in Content-Range header: %s",
			pos, resume, h)
	}
	return nil
}

func (r *resumingHTTPReader) sendRequest(
	reqHeaders map[string]string,
) (resp *http.Response, err error) {
	for attempt, retries := 0, retry.StartWithCtx(r.ctx, HTTPRetryOptions); retries.Next(); attempt++ {
		resp, err := r.client.req(r.ctx, "GET", r.url, nil, reqHeaders)
		if err == nil {
			return resp, nil
		}

		log.Errorf(r.ctx, "HTTP:Req error: err=%s (attempt %d)", err, attempt)

		if !errors.HasType(err, (*retryableHTTPError)(nil)) {
			return nil, err
		}
	}
	if r.ctx.Err() == nil {
		return nil, errors.New("too many retries; giving up")
	}

	return nil, r.ctx.Err()
}

// requestNextRanges issues additional http request
// to continue downloading next range of bytes.
func (r *resumingHTTPReader) requestNextRange() (err error) {
	if err := r.body.Close(); err != nil {
		return err
	}

	r.body = nil
	var resp *http.Response

	httpMap := make(map[string]string)
	buf := bytes.NewBufferString(r.client.header)
	if len(buf.Bytes()) != 0 {
		err = json.Unmarshal(buf.Bytes(), &httpMap)
		if err != nil {
			return err
		}
	}
	httpMap["Range"] = fmt.Sprintf("bytes=%d-", r.offset+r.pos)
	resp, err = r.sendRequest(httpMap)

	if err == nil {
		err = checkHTTPContentRangeHeader(resp.Header.Get("Content-Range"), r.pos)
	}

	if err == nil {
		r.body = resp.Body
	}
	return
}

// Read implements io.Reader interface to read the data from the underlying
// http stream, issuing additional requests in case download is interrupted.
func (r *resumingHTTPReader) Read(p []byte) (n int, err error) {
	for retries := 0; n == 0 && err == nil; retries++ {
		//n, err = r.client.client.Read(p)
		n, err = r.body.Read(p)
		r.pos += int64(n)

		if err != nil && !errors.IsAny(err, io.EOF, io.ErrUnexpectedEOF) {
			log.Errorf(r.ctx, "HTTP:Read err: %s", err)
		}

		// Resume download if the http server supports this.
		if r.canResume && isResumableHTTPError(err) {
			log.Errorf(r.ctx, "HTTP:Retry: error %s", err)
			if retries >= maxNoProgressReads {
				err = errors.Wrap(err, "multiple Read calls return no data")
				return
			}
			err = r.requestNextRange()
		}
	}

	return
}

// isResumableHTTPError returns true if we can
// resume download after receiving an error 'err'.
// We can attempt to resume download if the error is ErrUnexpectedEOF.
// In particular, we should not worry about a case when error is io.EOF.
// The reason for this is two-fold:
//   1. The underlying http library converts io.EOF to io.ErrUnexpectedEOF
//   if the number of bytes transferred is less than the number of
//   bytes advertised in the Content-Length header.  So if we see
//   io.ErrUnexpectedEOF we can simply request the next range.
//   2. If the server did *not* advertise Content-Length, then
//   there is really nothing we can do: http standard says that
//   the stream ends when the server terminates connection.
// In addition, we treat connection reset by peer errors (which can
// happen if we didn't read from the connection too long due to e.g. load),
// the same as unexpected eof errors.
func isResumableHTTPError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		sysutil.IsErrConnectionReset(err) ||
		sysutil.IsErrConnectionRefused(err)
}

// Maximum number of times we can attempt to retry reading from external storage,
// without making any progress.
const maxNoProgressReads = 3

func (h *httpSink) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://github.com/cockroachdb/cockroach/issues/23859
	return newResumingHTTPReader(ctx, h, basename)
}

// WriteFile should write the content to requested name.
func (h *httpSink) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {

	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("PUT %s", basename),
		TimeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			_, err := h.reqNoBody(ctx, "PUT", basename, content)
			return err
		})
}

// Delete removes the named file from the store.
func (h *httpSink) Delete(ctx context.Context, basename string) error {
	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("DELETE %s", basename),
		TimeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			_, err := h.reqNoBody(ctx, "DELETE", basename, nil)
			return err
		})
}

// Size returns the length of the named file in bytes.
func (h *httpSink) Size(ctx context.Context, basename string) (int64, error) {
	var resp *http.Response
	if err := contextutil.RunWithTimeout(ctx, fmt.Sprintf("HEAD %s", basename),
		TimeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			var err error
			resp, err = h.reqNoBody(ctx, "HEAD", basename, nil)
			return err
		}); err != nil {
		return 0, err
	}
	if resp.ContentLength < 0 {
		return 0, errors.Errorf("bad ContentLength: %d", resp.ContentLength)
	}
	return resp.ContentLength, nil
}

func (h *httpSink) Close() error {
	return nil
}

type retryableHTTPError struct {
	cause error
}

func (e *retryableHTTPError) Error() string {
	return fmt.Sprintf("retryable http error: %s", e.cause)
}

// reqNoBody is like req but it closes the response body.
func (h *httpSink) reqNoBody(
	ctx context.Context, method, file string, body io.Reader,
) (*http.Response, error) {

	httpMap := make(map[string]string)

	if h.header != "" {
		buf := bytes.NewBufferString(h.header)
		if len(buf.Bytes()) != 0 {
			err := json.Unmarshal(buf.Bytes(), &httpMap)
			if err != nil {
				return nil, err
			}
		}
	}

	resp, err := h.req(ctx, method, file, body, httpMap)
	if resp != nil {
		resp.Body.Close()
	}
	return resp, err
}

func (h *httpSink) req(
	ctx context.Context, method, file string, body io.Reader, headers map[string]string,
) (*http.Response, error) {
	dest := *h.base
	if hosts := len(h.hosts); hosts > 1 {
		if file == "" {
			return nil, errors.New("cannot use a multi-host HTTP basepath for single file")
		}
		hash := fnv.New32a()
		if _, err := hash.Write([]byte(file)); err != nil {
			panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
		}
		dest.Host = h.hosts[int(hash.Sum32())%hosts]
	}
	dest.Path = filepath.Join(dest.Path, file)
	url := dest.String()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, errors.Wrapf(err, "error constructing request %s %q", method, url)
	}
	req = req.WithContext(ctx)

	for key, val := range headers {
		req.Header.Add(key, val)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		// We failed to establish connection to the server (we don't even have
		// a response object/server response code). Those errors (e.g. due to
		// network blip, or DNS resolution blip, etc) are usually transient. The
		// client may choose to retry the request few times before giving up.
		return nil, &retryableHTTPError{err}
	}

	switch resp.StatusCode {
	case 200, 201, 204, 206:
	// Pass.
	default:
		body, _ := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		err := errors.Errorf("error response from server: %s %q", resp.Status, body)
		if err != nil && resp.StatusCode == 404 {
			err = errors.Wrapf(ErrFileDoesNotExist, "http storage file does not exist: %s", err.Error())
		}
		return nil, err
	}
	return resp, nil
}

type s3Sink struct {
	bucket   *string
	conf     *roachpb.DumpSink_S3
	prefix   string
	s3       *s3.S3
	settings *cluster.Settings
}

func (s *s3Sink) Write(ctx context.Context, file string, content io.ReadSeeker) error {
	return errors.New("s3 does not support additional writing")
}
func (s *s3Sink) Seek(
	ctx context.Context, filename string, offset, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return nil, errors.Errorf("不支持")
}

func (s *s3Sink) WriteFileWithReader(
	ctx context.Context, basename string, content io.Reader,
) error {
	return errors.Errorf(`s3Sink storage does not support WriteFileWithReader `)
}

func (s *s3Sink) SetConfig(header string) error {
	return errors.New("s3Sink does not support setting the head")
}

// delayedRetry runs fn and re-runs it a limited number of times if it
// fails. It knows about specific kinds of errors that need longer retry
// delays than normal.
func delayedRetry(ctx context.Context, fn func() error) error {
	const maxAttempts = 3
	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		err := fn()
		if err == nil {
			return nil
		}
		if s3err, ok := err.(s3.RequestFailure); ok {
			// A 503 error could mean we need to reduce our request rate. Impose an
			// arbitrary slowdown in that case.
			// See http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
			if s3err.StatusCode() == 503 {
				select {
				case <-time.After(time.Second * 5):
				case <-ctx.Done():
				}
			}
		}
		// See https:github.com/GoogleCloudPlatform/google-cloud-go/issues/1012#issuecomment-393606797
		// which suggests this GCE error message could be due to auth quota limits
		// being reached.
		if strings.Contains(err.Error(), "net/http: timeout awaiting response headers") {
			select {
			case <-time.After(time.Second * 5):
			case <-ctx.Done():
			}
		}
		return err
	})
}

func getPrefixBeforeWildcard(p string) string {
	globIndex := strings.IndexAny(p, "*?[")
	if globIndex < 0 {
		return p
	}
	return filepath.Dir(p[:globIndex])
}

func s3QueryParams(conf *roachpb.DumpSink_S3) string {
	q := make(url.Values)
	setIf := func(key, value string) {
		if value != "" {
			q.Set(key, value)
		}
	}
	setIf(S3AccessKeyParam, conf.AccessKey)
	setIf(S3SecretParam, conf.Secret)
	setIf(S3TempTokenParam, conf.TempToken)
	setIf(S3EndpointParam, conf.Endpoint)
	setIf(S3RegionParam, conf.Region)

	return q.Encode()
}

func (s *s3Sink) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	var fileList []string

	pattern := s.prefix
	var matchErr error
	err := s.s3.ListObjectsPagesWithContext(
		ctx,
		&s3.ListObjectsInput{
			Bucket: s.bucket,
			Prefix: aws.String(getPrefixBeforeWildcard(s.prefix)),
		},
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, fileObject := range page.Contents {
				matches, err := filepath.Match(pattern, *fileObject.Key)
				if err != nil {
					matchErr = err
					return false
				}
				if matches {
					s3URL := url.URL{
						Scheme:   "s3",
						Host:     *s.bucket,
						Path:     *fileObject.Key,
						RawQuery: s3QueryParams(s.conf),
					}
					fileList = append(fileList, s3URL.String())
				}

			}
			return !lastPage
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, `failed to list s3 bucket`)
	}
	if matchErr != nil {
		return nil, errors.Wrap(matchErr, `failed to list s3 bucket`)
	}

	return fileList, nil
}

var _ DumpSink = &s3Sink{}

func makeS3Sink(
	ctx context.Context, conf *roachpb.DumpSink_S3, settings *cluster.Settings,
) (DumpSink, error) {
	if conf == nil {
		return nil, errors.Errorf("s3 upload requested but info missing")
	}
	region := conf.Region
	config := conf.Keys()
	if conf.Endpoint != "" {
		config.Endpoint = &conf.Endpoint
		if conf.Region == "" {
			region = "default-region"
		}
		client, err := makeHTTPClient(settings)
		if err != nil {
			return nil, err
		}
		config.HTTPClient = client
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, errors.Wrap(err, "new aws session")
	}
	if region == "" {
		err = delayedRetry(ctx, func() error {
			var err error
			region, err = s3manager.GetBucketRegion(ctx, sess, conf.Bucket, "us-east-1")
			return err
		})
		if err != nil {
			return nil, errors.Wrap(err, "could not find s3 bucket's region")
		}
	}
	sess.Config.Region = aws.String(region)
	if conf.Endpoint != "" {
		sess.Config.S3ForcePathStyle = aws.Bool(true)
	}
	return &s3Sink{
		bucket:   aws.String(conf.Bucket),
		conf:     conf,
		prefix:   conf.Prefix,
		s3:       s3.New(sess),
		settings: settings,
	}, nil
}

// Conf should return the serializable configuration required to reconstruct
// this DumpSink implementation.
func (s *s3Sink) Conf() roachpb.DumpSink {
	return roachpb.DumpSink{
		Provider: roachpb.ExportStorageProvider_S3,
		S3Config: s.conf,
	}
}

// WriteFile should write the content to requested name.
func (s *s3Sink) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
	err := contextutil.RunWithTimeout(ctx, "put s3 object",
		TimeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			_, err := s.s3.PutObjectWithContext(ctx, &s3.PutObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(filepath.Join(s.prefix, basename)),
				Body:   content,
			})
			return err
		})
	return errors.Wrap(err, "failed to put s3 object")
}

// ReadFile should return a Reader for requested name.
func (s *s3Sink) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://github.com/znbasedb/znbase/issues/23859
	out, err := s.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: s.bucket,
		Key:    aws.String(filepath.Join(s.prefix, basename)),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get s3 object")
	}
	return out.Body, nil
}

// Delete removes the named file from the store.
func (s *s3Sink) Delete(ctx context.Context, basename string) error {
	return contextutil.RunWithTimeout(ctx, "delete s3 object",
		TimeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			_, err := s.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(filepath.Join(s.prefix, basename)),
			})
			return err
		})
}

// Size returns the length of the named file in bytes.
func (s *s3Sink) Size(ctx context.Context, basename string) (int64, error) {
	var out *s3.HeadObjectOutput
	err := contextutil.RunWithTimeout(ctx, "get s3 object header",
		TimeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			var err error
			out, err = s.s3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(filepath.Join(s.prefix, basename)),
			})
			return err
		})
	if err != nil {
		return 0, errors.Wrap(err, "failed to get s3 object headers")
	}
	return *out.ContentLength, nil
}

func (s *s3Sink) Close() error {
	return nil
}

// ParseWorkloadConfig parses a workload config URI to a proto config.
func ParseWorkloadConfig(uri *url.URL) (*roachpb.DumpSink_Workload, error) {
	c := &roachpb.DumpSink_Workload{}
	pathParts := strings.Split(strings.Trim(uri.Path, `/`), `/`)
	if len(pathParts) != 3 {
		return nil, errors.Errorf(
			`path must be of the form /<format>/<generator>/<table>: %s`, uri.Path)
	}
	c.Format, c.Generator, c.Table = pathParts[0], pathParts[1], pathParts[2]
	q := uri.Query()
	if _, ok := q[`version`]; !ok {
		return nil, errors.New(`parameter version is required`)
	}
	c.Version = q.Get(`version`)
	q.Del(`version`)
	if s := q.Get(`row-start`); len(s) > 0 {
		q.Del(`row-start`)
		var err error
		if c.BatchBegin, err = strconv.ParseInt(s, 10, 64); err != nil {
			return nil, err
		}
	}
	if e := q.Get(`row-end`); len(e) > 0 {
		q.Del(`row-end`)
		var err error
		if c.BatchEnd, err = strconv.ParseInt(e, 10, 64); err != nil {
			return nil, err
		}
	}
	for k, vs := range q {
		for _, v := range vs {
			c.Flags = append(c.Flags, `--`+k+`=`+v)
		}
	}
	return c, nil
}

type workloadSink struct {
	conf  *roachpb.DumpSink_Workload
	gen   workload.Generator
	table workload.Table
}

func (s *workloadSink) Write(ctx context.Context, file string, content io.ReadSeeker) error {
	return errors.New("workload does not support additional writing")
}
func (s *workloadSink) Seek(
	ctx context.Context, filename string, offset, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return nil, errors.Errorf("不支持")
}

func (s *workloadSink) WriteFileWithReader(
	ctx context.Context, basename string, content io.Reader,
) error {
	return errors.Errorf(`workloadSink storage does not support WriteFileWithReader `)
}
func (s *workloadSink) SetConfig(header string) error {
	return errors.New("workloadSink does not support setting the head")
}

func (s *workloadSink) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	return nil, errors.Errorf(`workload storage does not support listing files`)
}

var _ DumpSink = &workloadSink{}

func makeWorkloadSink(conf *roachpb.DumpSink_Workload) (DumpSink, error) {
	if conf == nil {
		return nil, errors.Errorf("workload upload requested but info missing")
	}
	if strings.ToLower(conf.Format) != `csv` {
		return nil, errors.Errorf(`unsupported format: %s`, conf.Format)
	}
	meta, err := workload.Get(conf.Generator)
	if err != nil {
		return nil, err
	}
	// Different versions of the workload could generate different data, so
	// disallow this.
	if meta.Version != conf.Version {
		return nil, errors.Errorf(
			`expected %s version "%s" but got "%s"`, meta.Name, conf.Version, meta.Version)
	}
	gen := meta.New()
	if f, ok := gen.(workload.Flagser); ok {
		if err := f.Flags().Parse(conf.Flags); err != nil {
			return nil, errors.Wrapf(err, `parsing parameters %s`, strings.Join(conf.Flags, ` `))
		}
	}
	s := &workloadSink{
		conf: conf,
		gen:  gen,
	}
	for _, t := range gen.Tables() {
		if t.Name == conf.Table {
			s.table = t
			break
		}
	}
	if s.table.Name == `` {
		return nil, errors.Wrapf(err, `unknown table %s for generator %s`, conf.Table, meta.Name)
	}
	return s, nil
}

// Conf should return the serializable configuration required to reconstruct
// this DumpSink implementation.
func (s *workloadSink) Conf() roachpb.DumpSink {
	return roachpb.DumpSink{
		Provider:       roachpb.ExportStorageProvider_Workload,
		WorkloadConfig: s.conf,
	}
}

// ReadFile should return a Reader for requested name.
func (s *workloadSink) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	if basename != `` {
		return nil, errors.New(`basenames are not supported by workload sink`)
	}
	r := workload.NewCSVRowsReader(s.table, int(s.conf.BatchBegin), int(s.conf.BatchEnd))
	return ioutil.NopCloser(r), nil
}

// WriteFile should write the content to requested name.
func (s *workloadSink) WriteFile(_ context.Context, _ string, _ io.ReadSeeker) error {
	return errors.Errorf(`workload sink does not support writes`)
}

// Delete removes the named file from the store.
func (s *workloadSink) Delete(_ context.Context, _ string) error {
	return errors.Errorf(`workload sink does not support writes`)
}

// Size returns the length of the named file in bytes.
func (s *workloadSink) Size(_ context.Context, _ string) (int64, error) {
	return 0, errors.Errorf(`workload sink does not support sizing`)
}

// Close after the first call is undefined.
// Specific implementations may document their own behavior.
func (s *workloadSink) Close() error {
	return nil
}

func makeKafkaSink(
	ctx context.Context, sinkS3 *roachpb.DumpSink_S3, settings2 *cluster.Settings,
) (DumpSink, error) {
	return nil, errors.Errorf(`not support kafka sink`)
}
