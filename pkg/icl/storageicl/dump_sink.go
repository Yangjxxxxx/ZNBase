// Copyright 2016  The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package storageicl

// Now these are Unused, they were moved to pkg/storage/dumpsink/dump_sink.go
//const (
//	// S3AccessKeyParam is the query parameter for access_key in an S3 URI.
//	S3AccessKeyParam = "AWS_ACCESS_KEY_ID"
//	// S3SecretParam is the query parameter for the 'secret' in an S3 URI.
//	S3SecretParam = "AWS_SECRET_ACCESS_KEY"
//	// S3TempTokenParam is the query parameter for session_token in an S3 URI.
//	S3TempTokenParam = "AWS_SESSION_TOKEN"
//	// S3EndpointParam is the query parameter for the 'endpoint' in an S3 URI.
//	S3EndpointParam = "AWS_ENDPOINT"
//	// S3RegionParam is the query parameter for the 'endpoint' in an S3 URI.
//	S3RegionParam = "AWS_REGION"
//
//	// AuthParam is the query parameter for the cluster settings named
//	// key in a URI.
//	// AuthParam          = "AUTH"
//	// authParamImplicit  = "implicit"
//	// authParamDefault   = "default"
//	// authParamSpecified = "specified"
//
//	// CredentialsParam is the query parameter for the base64-encoded contents of
//	// the Google Application Credentials JSON file.
//	// CredentialsParam = "CREDENTIALS"
//
//	// cloudsinkPrefix = "cloudsink"
//	// cloudsinkHTTP   = cloudsinkPrefix + ".http"
//
//	// cloudsinkHTTPCASetting = cloudsinkHTTP + ".custom_ca"
//
//	// cloudsinkTimeout = cloudsinkPrefix + ".timeout"
//
//	hdfsUserParm     = "user"
//	hdfsKrb5confParm = "krb5Conf"
//	hdfsRealmParm    = "realm"
//	hdfsPasswordParm = "password"
//	hdfsNamenodeParm = "nnPrincipal"
//	// hdfsConfPref     = "conf"
//
//	hdfsDefaultUser = "hdfs"
//
//	kafkaTopicParm = "topic"
//)

// Now it is Unused, it was moved to pkg/storage/dumpsink/dump_sink.go
// DumpSinkConfFromURI generates an DumpSink config from a URI string.
//func DumpSinkConfFromURI(ctx context.Context, path string) (roachpb.DumpSink, error) {
//	conf := roachpb.DumpSink{}
//	uri, err := url.Parse(path)
//	if err != nil {
//		return conf, err
//	}
//	switch uri.Scheme {
//	case "s3":
//		conf.Provider = roachpb.ExportStorageProvider_S3
//		conf.S3Config = &roachpb.DumpSink_S3{
//			Bucket:    uri.Host,
//			Prefix:    uri.Path,
//			AccessKey: uri.Query().Get(S3AccessKeyParam),
//			Secret:    uri.Query().Get(S3SecretParam),
//			TempToken: uri.Query().Get(S3TempTokenParam),
//			Endpoint:  uri.Query().Get(S3EndpointParam),
//			Region:    uri.Query().Get(S3RegionParam),
//		}
//		if conf.S3Config.AccessKey == "" {
//			return conf, errors.Errorf("s3 uri missing %q parameter", S3AccessKeyParam)
//		}
//		if conf.S3Config.Secret == "" {
//			return conf, errors.Errorf("s3 uri missing %q parameter", S3SecretParam)
//		}
//		conf.S3Config.Prefix = strings.TrimLeft(conf.S3Config.Prefix, "/")
//		// AWS secrets often contain + characters, which must be escaped when
//		// included in a query string; otherwise, they represent a space character.
//		// More than a few users have been bitten by this.
//		//
//		// Luckily, AWS secrets are base64-encoded data and thus will never actually
//		// contain spaces. We can convert any space characters we see to +
//		// characters to recover the original secret.
//		conf.S3Config.Secret = strings.Replace(conf.S3Config.Secret, " ", "+", -1)
//	case "http", "https":
//		conf.Provider = roachpb.ExportStorageProvider_Http
//		conf.HttpPath.BaseUri = path
//	case "nodelocal":
//		nodeID, err := strconv.Atoi(uri.Host)
//		if err != nil && uri.Host != "" {
//			return conf, errors.Errorf("host component of nodelocal URI must be a node ID: %s", path)
//		}
//		conf.Provider = roachpb.ExportStorageProvider_LocalFile
//		conf.LocalFile.Path = uri.Path
//		conf.LocalFile.NodeID = roachpb.NodeID(nodeID)
//	case "hdfs":
//		conf.Provider = roachpb.ExportStorageProvider_HDFS
//		values := uri.Query()
//		conf.HdfsConfig = &roachpb.DumpSink_HDFS{
//			Host:              uri.Host,
//			Path:              uri.Path,
//			Username:          values.Get(hdfsUserParm),
//			Krb5Conf:          values.Get(hdfsKrb5confParm),
//			Realm:             values.Get(hdfsRealmParm),
//			Password:          values.Get(hdfsPasswordParm),
//			NamenodePrincipal: values.Get(hdfsNamenodeParm),
//		}
//		if conf.HdfsConfig.Username == "" {
//			conf.HdfsConfig.Username = hdfsDefaultUser
//		}
//		if conf.HdfsConfig.Path == "" {
//			//默认在用户目录下
//			conf.HdfsConfig.Path = "/user/" + conf.HdfsConfig.Username
//		}
//		var buf bytes.Buffer
//		for k := range values {
//			if strings.HasPrefix(k, "conf_") {
//				buf.WriteString(strings.TrimPrefix(k, "conf_"))
//				buf.WriteString("=")
//				buf.WriteString(values.Get(k))
//				buf.WriteString(",")
//			}
//		}
//		conf.HdfsConfig.HdfsConfigs = buf.String()
//		log.Warningf(ctx, "HDFS Kerberos config: %s", conf.HdfsConfig.Krb5Conf)
//
//	case "kafka":
//		conf.Provider = roachpb.ExportStorageProvider_KAFKA
//		conf.KafkaConfig.KafkaServer = path
//		conf.KafkaConfig.Topic = uri.Query().Get(kafkaTopicParm)
//	case "experimental-workload":
//		conf.Provider = roachpb.ExportStorageProvider_Workload
//		if conf.WorkloadConfig, err = ParseWorkloadConfig(uri); err != nil {
//			return conf, err
//		}
//	default:
//		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
//	}
//	return conf, nil
//}

// Now it is Unused, it was moved to pkg/storage/dumpsink/dump_sink.go
// FromURI returns an DumpSink for the given URI.
//func FromURI(
//	ctx context.Context, uri string, settings *cluster.Settings,
//) (DumpSink, error) {
//	conf, err := ConfFromURI(ctx, uri)
//	if err != nil {
//		return nil, err
//	}
//	return MakeDumpSink(ctx, conf, settings)
//}

// Now it is Unused, it was moved to pkg/storage/dumpsink/dump_sink.go
// SanitizeDumpSinkURI returns the export storage URI with sensitive
// credentials stripped.
//func SanitizeDumpSinkURI(path string) (string, error) {
//	uri, err := url.Parse(path)
//	if err != nil {
//		return "", err
//	}
//	if uri.Scheme == "experimental-workload" {
//		return path, nil
//	}
//	// All current export storage providers store credentials in the query string,
//	// if they store it in the URI at all.
//	uri.RawQuery = ""
//	return uri.String(), nil
//}

// Now it is Unused, it was moved to pkg/storage/dumpsink/dump_sink.go
// MakeDumpSink creates an DumpSink from the given config.
//func MakeDumpSink(
//	ctx context.Context, dest roachpb.DumpSink, settings *cluster.Settings,
//) (DumpSink, error) {
//	switch dest.Provider {
//	case roachpb.ExportStorageProvider_LocalFile:
//		telemetry.Count("external-io.nodelocal")
//		return makeLocalSink(dest.LocalFile, settings)
//	case roachpb.ExportStorageProvider_Http:
//		telemetry.Count("external-io.http")
//		return makeHTTPSink(dest.HttpPath.BaseUri, settings)
//	case roachpb.ExportStorageProvider_S3:
//		telemetry.Count("external-io.s3")
//		return makeS3Sink(ctx, dest.S3Config, settings)
//	case roachpb.ExportStorageProvider_Workload:
//		if err := settings.Version.CheckVersion(
//			cluster.VersionExportStorageWorkload, "experimental-workload",
//		); err != nil {
//			return nil, err
//		}
//		telemetry.Count("external-io.workload")
//		return makeWorkloadSink(dest.WorkloadConfig)
//	case roachpb.ExportStorageProvider_HDFS:
//		telemetry.Count("external-io.hdfs")
//		return makeHdfsSink(ctx, dest.HdfsConfig, settings)
//	case roachpb.ExportStorageProvider_KAFKA:
//		telemetry.Count("external-io.kafka")
//		return makeKafkaSink(ctx, dest.S3Config, settings)
//	}
//	return nil, errors.Errorf("unsupported export destination type: %s", dest.Provider.String())
//}

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
//type DumpSink interface {
//	io.Closer
//
//	// Conf should return the serializable configuration required to reconstruct
//	// this DumpSink implementation.
//	Conf() roachpb.DumpSink
//
//	// ReadFile should return a Reader for requested name.
//	ReadFile(ctx context.Context, basename string) (io.ReadCloser, error)
//
//	// WriteFile should write the content to requested name.
//	WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error
//
//	// Delete removes the named file from the store.
//	Delete(ctx context.Context, basename string) error
//
//	// Size returns the length of the named file in bytes.
//	Size(ctx context.Context, basename string) (int64, error)
//}

// Now these are Unused, they were moved to pkg/storage/dumpsink/dump_sink.go
//var (
//	//httpCustomCA *settings.StringSetting
//	//timeoutSetting *settings.DurationSetting
//	//*
//	httpCustomCA   = dumpsink.HTTPCustomCA
//	timeoutSetting = dumpsink.TimeoutSetting
//)

//type localFileSink struct {
//	cfg  roachpb.DumpSink_LocalFilePath // constains un-prefixed base -- DO NOT use for I/O ops.
//	base string                         // the prefixed base, for I/O ops on this node.
//}
//
//var _ DumpSink = &localFileSink{}

// Now it is Unused, it was moved to pkg/storage/dumpsink/dump_sink.go
// MakeLocalSinkURI converts a local path (absolute or relative) to a
// valid nodelocal URI.
//func MakeLocalSinkURI(path string) (string, error) {
//	path, err := filepath.Abs(path)
//	if err != nil {
//		return "", err
//	}
//	return fmt.Sprintf("nodelocal://%s", path), nil
//}

// Now it is Unused, it was moved to pkg/storage/dumpsink/dump_sink.go
//func makeLocalSink(
//	cfg roachpb.DumpSink_LocalFilePath, settings *cluster.Settings,
//) (DumpSink, error) {
//	if cfg.Path == "" {
//		return nil, errors.Errorf("Local storage requested but path not provided")
//	}
//	// TODO(dt): check that this node is cfg.NodeID if non-zero.
//
//	localBase := cfg.Path
//	// In non-server execution we have no settings and no restriction on local IO.
//	if settings != nil {
//		if settings.ExternalIODir == "" {
//			return nil, errors.Errorf("local file access is disabled")
//		}
//		// we prefix with the IO dir
//		localBase = filepath.Clean(filepath.Join(settings.ExternalIODir, localBase))
//		// ... and make sure we didn't ../ our way back out.
//		if !strings.HasPrefix(localBase, settings.ExternalIODir) {
//			return nil, errors.Errorf("local file access to paths outside of external-io-dir is not allowed")
//		}
//	}
//	return &localFileSink{base: localBase, cfg: cfg}, nil
//}
//
//func (l *localFileSink) Conf() roachpb.DumpSink {
//	return roachpb.DumpSink{
//		Provider:  roachpb.ExportStorageProvider_LocalFile,
//		LocalFile: l.cfg,
//	}
//}
//
//func (l *localFileSink) WriteFile(_ context.Context, basename string, content io.ReadSeeker) error {
//	p := filepath.Join(l.base, basename)
//	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
//		return errors.Wrap(err, "creating local export storage path")
//	}
//	tmpP := p + `.tmp`
//	f, err := os.Create(tmpP)
//	if err != nil {
//		return errors.Wrapf(err, "creating local export tmp file %q", tmpP)
//	}
//	defer f.Close()
//	_, err = io.Copy(f, content)
//	if err != nil {
//		return errors.Wrapf(err, "writing to local export tmp file %q", tmpP)
//	}
//	if err := f.Sync(); err != nil {
//		return errors.Wrapf(err, "syncing to local export tmp file %q", tmpP)
//	}
//	return errors.Wrapf(os.Rename(tmpP, p), "renaming to local export file %q", p)
//}
//
//func (l *localFileSink) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
//	return os.Open(filepath.Join(l.base, basename))
//}
//
//func (l *localFileSink) Delete(_ context.Context, basename string) error {
//	return os.Remove(filepath.Join(l.base, basename))
//}
//
//func (l *localFileSink) Size(_ context.Context, basename string) (int64, error) {
//	fi, err := os.Stat(filepath.Join(l.base, basename))
//	if err != nil {
//		return 0, err
//	}
//	return fi.Size(), nil
//}
//
//func (*localFileSink) Close() error {
//	return nil
//}
//
//type httpSink struct {
//	base     *url.URL
//	client   *http.Client
//	hosts    []string
//	settings *cluster.Settings
//}
//
//var _ DumpSink = &httpSink{}
//
//func makeHTTPClient(settings *cluster.Settings) (*http.Client, error) {
//	var tlsConf *tls.Config
//	if pem := httpCustomCA.Get(&settings.SV); pem != "" {
//		roots, err := x509.SystemCertPool()
//		if err != nil {
//			return nil, errors.Wrap(err, "could not load system root CA pool")
//		}
//		if !roots.AppendCertsFromPEM([]byte(pem)) {
//			return nil, errors.Errorf("failed to parse root CA certificate from %q", pem)
//		}
//		tlsConf = &tls.Config{RootCAs: roots}
//	}
//	// Copy the defaults from http.DefaultTransport. We cannot just copy the
//	// entire struct because it has a sync Mutex. This has the unfortunate problem
//	// that if Go adds fields to DefaultTransport they won't be copied here,
//	// but this is ok for now.
//	t := http.DefaultTransport.(*http.Transport)
//	return &http.Client{Transport: &http.Transport{
//		Proxy:                 t.Proxy,
//		DialContext:           t.DialContext,
//		MaxIdleConns:          t.MaxIdleConns,
//		IdleConnTimeout:       t.IdleConnTimeout,
//		TLSHandshakeTimeout:   t.TLSHandshakeTimeout,
//		ExpectContinueTimeout: t.ExpectContinueTimeout,
//
//		// Add our custom CA.
//		TLSClientConfig: tlsConf,
//	}}, nil
//}
//
//func makeHTTPSink(base string, settings *cluster.Settings) (DumpSink, error) {
//	if base == "" {
//		return nil, errors.Errorf("HTTP sink requested but base path not provided")
//	}
//
//	client, err := makeHTTPClient(settings)
//	if err != nil {
//		return nil, err
//	}
//	uri, err := url.Parse(base)
//	if err != nil {
//		return nil, err
//	}
//	return &httpSink{
//		base:     uri,
//		client:   client,
//		hosts:    strings.Split(uri.Host, ","),
//		settings: settings,
//	}, nil
//}
//
//func (h *httpSink) Conf() roachpb.DumpSink {
//	return roachpb.DumpSink{
//		Provider: roachpb.ExportStorageProvider_Http,
//		HttpPath: roachpb.DumpSink_Http{
//			BaseUri: h.base.String(),
//		},
//	}
//}
//
//func (h *httpSink) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
//	// https://github.com/znbasedb/znbase/issues/23859
//	resp, err := h.req(ctx, "GET", basename, nil)
//	if err != nil {
//		return nil, err
//	}
//	return resp.Body, nil
//}
//
//func (h *httpSink) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
//	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("PUT %s", basename),
//		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
//			_, err := h.reqNoBody(ctx, "PUT", basename, content)
//			return err
//		})
//}
//
//func (h *httpSink) Delete(ctx context.Context, basename string) error {
//	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("DELETE %s", basename),
//		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
//			_, err := h.reqNoBody(ctx, "DELETE", basename, nil)
//			return err
//		})
//}
//
//func (h *httpSink) Size(ctx context.Context, basename string) (int64, error) {
//	var resp *http.Response
//	if err := contextutil.RunWithTimeout(ctx, fmt.Sprintf("HEAD %s", basename),
//		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
//			var err error
//			resp, err = h.reqNoBody(ctx, "HEAD", basename, nil)
//			return err
//		}); err != nil {
//		return 0, err
//	}
//	if resp.ContentLength < 0 {
//		return 0, errors.Errorf("bad ContentLength: %d", resp.ContentLength)
//	}
//	return resp.ContentLength, nil
//}
//
//func (h *httpSink) Close() error {
//	return nil
//}
//
//// reqNoBody is like req but it closes the response body.
//func (h *httpSink) reqNoBody(
//	ctx context.Context, method, file string, body io.Reader,
//) (*http.Response, error) {
//	resp, err := h.req(ctx, method, file, body)
//	if resp != nil {
//		resp.Body.Close()
//	}
//	return resp, err
//}
//
//func (h *httpSink) req(
//	ctx context.Context, method, file string, body io.Reader,
//) (*http.Response, error) {
//	dest := *h.base
//	if hosts := len(h.hosts); hosts > 1 {
//		if file == "" {
//			return nil, errors.New("cannot use a multi-host HTTP basepath for single file")
//		}
//		hash := fnv.New32a()
//		if _, err := hash.Write([]byte(file)); err != nil {
//			panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
//		}
//		dest.Host = h.hosts[int(hash.Sum32())%hosts]
//	}
//	dest.Path = path.Join(dest.Path, file)
//	url := dest.String()
//	req, err := http.NewRequest(method, url, body)
//	req = req.WithContext(ctx)
//	if err != nil {
//		return nil, errors.Wrapf(err, "error constructing request %s %q", method, url)
//	}
//	resp, err := h.client.Do(req)
//	if err != nil {
//		return nil, errors.Wrapf(err, "error executing request %s %q", method, url)
//	}
//	switch resp.StatusCode {
//	case 200, 201, 204:
//		// ignore
//	default:
//		body, _ := ioutil.ReadAll(resp.Body)
//		_ = resp.Body.Close()
//		return nil, errors.Errorf("error response from server: %s %q", resp.Status, body)
//	}
//	return resp, nil
//}
//
//type s3Sink struct {
//	bucket   *string
//	conf     *roachpb.DumpSink_S3
//	prefix   string
//	s3       *s3.S3
//	settings *cluster.Settings
//}
//
//// delayedRetry runs fn and re-runs it a limited number of times if it
//// fails. It knows about specific kinds of errors that need longer retry
//// delays than normal.
//func delayedRetry(ctx context.Context, fn func() error) error {
//	const maxAttempts = 3
//	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
//		err := fn()
//		if err == nil {
//			return nil
//		}
//		if s3err, ok := err.(s3.RequestFailure); ok {
//			// A 503 error could mean we need to reduce our request rate. Impose an
//			// arbitrary slowdown in that case.
//			// See http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
//			if s3err.StatusCode() == 503 {
//				select {
//				case <-time.After(time.Second * 5):
//				case <-ctx.Done():
//				}
//			}
//		}
//		// See https:github.com/GoogleCloudPlatform/google-cloud-go/issues/1012#issuecomment-393606797
//		// which suggests this GCE error message could be due to auth quota limits
//		// being reached.
//		if strings.Contains(err.Error(), "net/http: timeout awaiting response headers") {
//			select {
//			case <-time.After(time.Second * 5):
//			case <-ctx.Done():
//			}
//		}
//		return err
//	})
//}
//
//var _ DumpSink = &s3Sink{}
//
//func makeS3Sink(
//	ctx context.Context, conf *roachpb.DumpSink_S3, settings *cluster.Settings,
//) (DumpSink, error) {
//	if conf == nil {
//		return nil, errors.Errorf("s3 upload requested but info missing")
//	}
//	region := conf.Region
//	config := conf.Keys()
//	if conf.Endpoint != "" {
//		config.Endpoint = &conf.Endpoint
//		if conf.Region == "" {
//			region = "default-region"
//		}
//		client, err := makeHTTPClient(settings)
//		if err != nil {
//			return nil, err
//		}
//		config.HTTPClient = client
//	}
//	sess, err := session.NewSession(config)
//	if err != nil {
//		return nil, errors.Wrap(err, "new aws session")
//	}
//	if region == "" {
//		err = delayedRetry(ctx, func() error {
//			var err error
//			region, err = s3manager.GetBucketRegion(ctx, sess, conf.Bucket, "us-east-1")
//			return err
//		})
//		if err != nil {
//			return nil, errors.Wrap(err, "could not find s3 bucket's region")
//		}
//	}
//	sess.Config.Region = aws.String(region)
//	if conf.Endpoint != "" {
//		sess.Config.S3ForcePathStyle = aws.Bool(true)
//	}
//	return &s3Sink{
//		bucket:   aws.String(conf.Bucket),
//		conf:     conf,
//		prefix:   conf.Prefix,
//		s3:       s3.New(sess),
//		settings: settings,
//	}, nil
//}
//
//func (s *s3Sink) Conf() roachpb.DumpSink {
//	return roachpb.DumpSink{
//		Provider: roachpb.ExportStorageProvider_S3,
//		S3Config: s.conf,
//	}
//}
//
//func (s *s3Sink) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
//	err := contextutil.RunWithTimeout(ctx, "put s3 object",
//		timeoutSetting.Get(&s.settings.SV),
//		func(ctx context.Context) error {
//			_, err := s.s3.PutObjectWithContext(ctx, &s3.PutObjectInput{
//				Bucket: s.bucket,
//				Key:    aws.String(path.Join(s.prefix, basename)),
//				Body:   content,
//			})
//			return err
//		})
//	return errors.Wrap(err, "failed to put s3 object")
//}
//
//func (s *s3Sink) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
//	// https://github.com/znbasedb/znbase/issues/23859
//	out, err := s.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
//		Bucket: s.bucket,
//		Key:    aws.String(path.Join(s.prefix, basename)),
//	})
//	if err != nil {
//		return nil, errors.Wrap(err, "failed to get s3 object")
//	}
//	return out.Body, nil
//}
//
//func (s *s3Sink) Delete(ctx context.Context, basename string) error {
//	return contextutil.RunWithTimeout(ctx, "delete s3 object",
//		timeoutSetting.Get(&s.settings.SV),
//		func(ctx context.Context) error {
//			_, err := s.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
//				Bucket: s.bucket,
//				Key:    aws.String(path.Join(s.prefix, basename)),
//			})
//			return err
//		})
//}
//
//func (s *s3Sink) Size(ctx context.Context, basename string) (int64, error) {
//	var out *s3.HeadObjectOutput
//	err := contextutil.RunWithTimeout(ctx, "get s3 object header",
//		timeoutSetting.Get(&s.settings.SV),
//		func(ctx context.Context) error {
//			var err error
//			out, err = s.s3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
//				Bucket: s.bucket,
//				Key:    aws.String(path.Join(s.prefix, basename)),
//			})
//			return err
//		})
//	if err != nil {
//		return 0, errors.Wrap(err, "failed to get s3 object headers")
//	}
//	return *out.ContentLength, nil
//}
//
//func (s *s3Sink) Close() error {
//	return nil
//}
//
//// ParseWorkloadConfig parses a workload config URI to a proto config.
//func ParseWorkloadConfig(uri *url.URL) (*roachpb.DumpSink_Workload, error) {
//	c := &roachpb.DumpSink_Workload{}
//	pathParts := strings.Split(strings.Trim(uri.Path, `/`), `/`)
//	if len(pathParts) != 3 {
//		return nil, errors.Errorf(
//			`path must be of the form /<format>/<generator>/<table>: %s`, uri.Path)
//	}
//	c.Format, c.Generator, c.Table = pathParts[0], pathParts[1], pathParts[2]
//	q := uri.Query()
//	if _, ok := q[`version`]; !ok {
//		return nil, errors.New(`parameter version is required`)
//	}
//	c.Version = q.Get(`version`)
//	q.Del(`version`)
//	if s := q.Get(`row-start`); len(s) > 0 {
//		q.Del(`row-start`)
//		var err error
//		if c.BatchBegin, err = strconv.ParseInt(s, 10, 64); err != nil {
//			return nil, err
//		}
//	}
//	if e := q.Get(`row-end`); len(e) > 0 {
//		q.Del(`row-end`)
//		var err error
//		if c.BatchEnd, err = strconv.ParseInt(e, 10, 64); err != nil {
//			return nil, err
//		}
//	}
//	for k, vs := range q {
//		for _, v := range vs {
//			c.Flags = append(c.Flags, `--`+k+`=`+v)
//		}
//	}
//	return c, nil
//}
//
//type workloadSink struct {
//	conf  *roachpb.DumpSink_Workload
//	gen   workload.Generator
//	table workload.Table
//}
//
//var _ DumpSink = &workloadSink{}
//
//func makeWorkloadSink(conf *roachpb.DumpSink_Workload) (DumpSink, error) {
//	if conf == nil {
//		return nil, errors.Errorf("workload upload requested but info missing")
//	}
//	if strings.ToLower(conf.Format) != `csv` {
//		return nil, errors.Errorf(`unsupported format: %s`, conf.Format)
//	}
//	meta, err := workload.Get(conf.Generator)
//	if err != nil {
//		return nil, err
//	}
//	// Different versions of the workload could generate different data, so
//	// disallow this.
//	if meta.Version != conf.Version {
//		return nil, errors.Errorf(
//			`expected %s version "%s" but got "%s"`, meta.Name, conf.Version, meta.Version)
//	}
//	gen := meta.New()
//	if f, ok := gen.(workload.Flagser); ok {
//		if err := f.Flags().Parse(conf.Flags); err != nil {
//			return nil, errors.Wrapf(err, `parsing parameters %s`, strings.Join(conf.Flags, ` `))
//		}
//	}
//	s := &workloadSink{
//		conf: conf,
//		gen:  gen,
//	}
//	for _, t := range gen.Tables() {
//		if t.Name == conf.Table {
//			s.table = t
//			break
//		}
//	}
//	if s.table.Name == `` {
//		return nil, errors.Wrapf(err, `unknown table %s for generator %s`, conf.Table, meta.Name)
//	}
//	return s, nil
//}
//
//func (s *workloadSink) Conf() roachpb.DumpSink {
//	return roachpb.DumpSink{
//		Provider:       roachpb.ExportStorageProvider_Workload,
//		WorkloadConfig: s.conf,
//	}
//}
//
//func (s *workloadSink) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
//	if basename != `` {
//		return nil, errors.New(`basenames are not supported by workload sink`)
//	}
//	r := workload.NewCSVRowsReader(s.table, int(s.conf.BatchBegin), int(s.conf.BatchEnd))
//	return ioutil.NopCloser(r), nil
//}
//
//func (s *workloadSink) WriteFile(_ context.Context, _ string, _ io.ReadSeeker) error {
//	return errors.Errorf(`workload sink does not support writes`)
//}
//func (s *workloadSink) Delete(_ context.Context, _ string) error {
//	return errors.Errorf(`workload sink does not support writes`)
//}
//func (s *workloadSink) Size(_ context.Context, _ string) (int64, error) {
//	return 0, errors.Errorf(`workload sink does not support sizing`)
//}
//func (s *workloadSink) Close() error {
//	return nil
//}
//
//func makeKafkaSink(
//	ctx context.Context, sinkS3 *roachpb.DumpSink_S3, settings2 *cluster.Settings,
//) (DumpSink, error) {
//	return nil, errors.Errorf(`not support kafka sink`)
//}
