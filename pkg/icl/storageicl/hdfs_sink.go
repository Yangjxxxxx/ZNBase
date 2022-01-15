package storageicl

/*
  使用hdfs做为备份数据的导出/导入
  样例：
    hdfs://node1:8020/data/customer?user=hdfs&krb5Conf=..&keytab=..&conf_dfs.client.retry.policy.enabled=true&conf_dfs.replication=3
*/
// Now these are Unused, they were moved to pkg/storage/dumpsink/dump_sink.go
//type hdfsSink struct {
//	path     *string
//	client   *hdfs.Client
//	conf     *roachpb.DumpSink_HDFS
//	settings *cluster.Settings
//}
//
//func (h *hdfsSink) filepath(file string) string {
//	return *h.path + "/" + file
//}
//
//var _ DumpSink = &hdfsSink{}
//
//func makeHdfsSink(
//	ctx context.Context, hdfsConf *roachpb.DumpSink_HDFS, settings *cluster.Settings,
//) (DumpSink, error) {
//	if hdfsConf.Path == "" {
//		return nil, errors.Errorf("HDFS sink requested but base path not provided")
//	}
//
//	client, err := newHdfsClient(hdfsConf)
//	if err != nil {
//		return nil, err
//	}
//
//	return &hdfsSink{
//		path:     &hdfsConf.Path,
//		client:   client,
//		conf:     hdfsConf,
//		settings: settings,
//	}, nil
//}
//
//func newHdfsClient(hdfsParm *roachpb.DumpSink_HDFS) (*hdfs.Client, error) {
//	conf, err := hadoopconf.LoadFromEnvironment()
//	if err != nil {
//		return nil, err
//	}
//	if hdfsParm.HdfsConfigs != "" {
//		configs := strings.Split(hdfsParm.HdfsConfigs, ",")
//		for _, ele := range configs {
//			kv := strings.Split(ele, "=")
//			if len(kv) < 2 {
//				continue
//			}
//			conf[kv[0]] = kv[1]
//		}
//	}
//
//	options := hdfs.ClientOptionsFromConf(conf)
//	address := hdfsParm.Host
//	if address != "" {
//		options.Addresses = strings.Split(address, ",")
//	}
//
//	if hdfsParm.Krb5Conf != "" {
//		options.KerberosClient, err = getKerberosClient(hdfsParm)
//		if err != nil {
//			return nil, err
//		}
//		options.KerberosServicePrincipleName = hdfsParm.NamenodePrincipal //"nn/_HOST"
//		options.UseDatanodeHostname = true
//	} else {
//		options.User = hdfsParm.Username
//	}
//	return hdfs.NewClient(options)
//}
//
//func getKerberosClient(hdfsParm *roachpb.DumpSink_HDFS) (*krb.Client, error) {
//	strs := strings.Split(hdfsParm.Krb5Conf, "\\n") //从URI参数值无法传递换行符，只能解析到“\n”字符串
//	var buf bytes.Buffer
//	for _, k := range strs {
//		buf.WriteString(k)
//		buf.WriteString("\n")
//	}
//
//	cfg, err := config.NewConfigFromString(buf.String())
//	if err != nil {
//		return nil, errors.Wrapf(err, "Couldn't load krb config.")
//	}
//	client := krb.NewClientWithPassword(hdfsParm.Username, hdfsParm.Realm, hdfsParm.Password)
//	err = client.WithConfig(cfg).Login()
//	if err != nil {
//		return nil, errors.Wrapf(err, "Couldn't initialize krb client.")
//	}
//
//	return &client, nil
//}
//
//func (h *hdfsSink) Conf() roachpb.DumpSink {
//	return roachpb.DumpSink{
//		Provider:   roachpb.ExportStorageProvider_HDFS,
//		HdfsConfig: h.conf,
//	}
//}
//
//type hdfsReader struct {
//	file *hdfs.FileReader
//}
//
//func (r hdfsReader) Read(p []byte) (n int, err error) {
//	return r.file.Read(p)
//}
//func (r hdfsReader) Close() error {
//	return r.file.Close()
//}
//
//func (h *hdfsSink) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
//	file, err := h.client.Open(h.filepath(*h.path))
//	if err != nil {
//		return nil, err
//	}
//	return hdfsReader{file}, nil
//}
//
//func (h *hdfsSink) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
//	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("PUT %s", basename),
//		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
//			_, err := h.writeContent(ctx, h.filepath(basename), content)
//			return err
//		})
//}
//
//func (h *hdfsSink) Delete(ctx context.Context, basename string) error {
//	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("DELETE %s", basename),
//		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
//			err := h.client.Remove(h.filepath(basename))
//			return err
//		})
//}
//
//func (h *hdfsSink) Size(ctx context.Context, basename string) (int64, error) {
//	var len *int64
//	if err := contextutil.RunWithTimeout(ctx, fmt.Sprintf("HEAD %s", basename),
//		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
//			var err error
//			file, err := h.client.Open(h.filepath(basename))
//			if err != nil {
//				return nil
//			}
//			defer func() {
//				_ = file.Close()
//			}()
//
//			*len = file.Stat().Size()
//			return nil
//		}); err != nil {
//		return 0, err
//	}
//	if *len < 0 {
//		return 0, errors.Errorf("bad ContentLength: %d", len)
//	}
//	return *len, nil
//}
//
//func (h *hdfsSink) Close() error {
//	return h.client.Close()
//}
//
//// reqNoBody is like req but it closes the response body.
//func (h *hdfsSink) writeContent(
//	ctx context.Context, filepath string, content io.ReadSeeker,
//) (int, error) {
//	wc := 0
//	writer, err := h.client.Create(filepath)
//	if err != nil {
//		return wc, err
//	}
//	defer func() {
//		_ = writer.Close()
//	}()
//
//	buf := make([]byte, 4096)
//	for n, _ := content.Read(buf); n > 0; {
//		nw, err := writer.Write(buf[0:n])
//		if err != nil {
//			return wc, err
//		}
//		wc += nw
//		n, _ = content.Read(buf)
//	}
//	return wc, nil
//}
