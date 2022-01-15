package dumpsink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/colinmarc/hdfs"
	"github.com/colinmarc/hdfs/hadoopconf"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/util/contextutil"
	krb "gopkg.in/jcmturner/gokrb5.v5/client"
	"gopkg.in/jcmturner/gokrb5.v5/config"
)

/*
  使用hdfs做为备份数据的导出/导入
  样例：
    hdfs://node1:8020/data/customer?user=hdfs&krb5Conf=..&keytab=..&conf_dfs.client.retry.policy.enabled=true&conf_dfs.replication=3
*/
type hdfsSink struct {
	path     *string
	client   *hdfs.Client
	conf     *roachpb.DumpSink_HDFS
	settings *cluster.Settings
}

func (h *hdfsSink) Write(ctx context.Context, file string, content io.ReadSeeker) error {
	return errors.New("hdfs does not support additional writing")
}
func (h *hdfsSink) Seek(
	ctx context.Context, filename string, offset, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return nil, errors.Errorf("不支持")
}

func (h *hdfsSink) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	return nil, errors.Errorf(`hdfs storage does not support listing files`)
}

func (h *hdfsSink) WriteFileWithReader(
	ctx context.Context, basename string, content io.Reader,
) error {
	return errors.Errorf(`hdfs storage does not support WriteFileWithReader `)
}

func (h *hdfsSink) SetConfig(header string) error {
	return errors.New("hdfsSink does not support setting the head")
}

func (h *hdfsSink) filepath(file string) string {
	return *h.path + "/" + file
}

var _ DumpSink = &hdfsSink{}

// MakeHdfsSink 使用hdfs做为备份数据的导出/导入
func MakeHdfsSink(
	ctx context.Context, hdfsConf *roachpb.DumpSink_HDFS, settings *cluster.Settings,
) (DumpSink, error) {
	if hdfsConf.Path == "" {
		return nil, errors.Errorf("HDFS sink requested but base path not provided")
	}

	client, err := newHdfsClient(hdfsConf)
	if err != nil {
		return nil, err
	}

	return &hdfsSink{
		path:     &hdfsConf.Path,
		client:   client,
		conf:     hdfsConf,
		settings: settings,
	}, nil
}

func newHdfsClient(hdfsParm *roachpb.DumpSink_HDFS) (*hdfs.Client, error) {
	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return nil, err
	}
	if hdfsParm.HdfsConfigs != "" {
		configs := strings.Split(hdfsParm.HdfsConfigs, ",")
		for _, ele := range configs {
			kv := strings.Split(ele, "=")
			if len(kv) < 2 {
				continue
			}
			conf[kv[0]] = kv[1]
		}
	}

	options := hdfs.ClientOptionsFromConf(conf)
	address := hdfsParm.Host
	if address != "" {
		options.Addresses = strings.Split(address, ",")
	}

	if hdfsParm.Krb5Conf != "" {
		options.KerberosClient, err = getKerberosClient(hdfsParm)
		if err != nil {
			return nil, err
		}
		options.KerberosServicePrincipleName = hdfsParm.NamenodePrincipal //"nn/_HOST"
		options.UseDatanodeHostname = true
	} else {
		options.User = hdfsParm.Username
	}
	return hdfs.NewClient(options)
}

func getKerberosClient(hdfsParm *roachpb.DumpSink_HDFS) (*krb.Client, error) {
	strs := strings.Split(hdfsParm.Krb5Conf, "\\n") //从URI参数值无法传递换行符，只能解析到“\n”字符串
	var buf bytes.Buffer
	for _, k := range strs {
		buf.WriteString(k)
		buf.WriteString("\n")
	}

	cfg, err := config.NewConfigFromString(buf.String())
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't load krb config.")
	}
	client := krb.NewClientWithPassword(hdfsParm.Username, hdfsParm.Realm, hdfsParm.Password)
	err = client.WithConfig(cfg).Login()
	if err != nil {
		return nil, errors.Wrapf(err, "Couldn't initialize krb client.")
	}

	return &client, nil
}

// Conf should return the serializable configuration required to reconstruct
// this DumpSink implementation.
func (h *hdfsSink) Conf() roachpb.DumpSink {
	return roachpb.DumpSink{
		Provider:   roachpb.ExportStorageProvider_HDFS,
		HdfsConfig: h.conf,
	}
}

type hdfsReader struct {
	file *hdfs.FileReader
}

func (r hdfsReader) Read(p []byte) (n int, err error) {
	return r.file.Read(p)
}
func (r hdfsReader) Close() error {
	return r.file.Close()
}

// ReadFile should return a Reader for requested name.
func (h *hdfsSink) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	file, err := h.client.Open(h.filepath(*h.path))
	if err != nil {
		return nil, err
	}
	return hdfsReader{file}, nil
}

// WriteFile should write the content to requested name.
func (h *hdfsSink) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("PUT %s", basename),
		TimeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			_, err := h.writeContent(ctx, h.filepath(basename), content)
			return err
		})
}

// Delete removes the named file from the store.
func (h *hdfsSink) Delete(ctx context.Context, basename string) error {
	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("DELETE %s", basename),
		TimeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			err := h.client.Remove(h.filepath(basename))
			return err
		})
}

// Size returns the length of the named file in bytes.
func (h *hdfsSink) Size(ctx context.Context, basename string) (int64, error) {
	var len *int64
	if err := contextutil.RunWithTimeout(ctx, fmt.Sprintf("HEAD %s", basename),
		TimeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			var err error
			file, err := h.client.Open(h.filepath(basename))
			if err != nil {
				return nil
			}
			defer func() {
				_ = file.Close()
			}()

			*len = file.Stat().Size()
			return nil
		}); err != nil {
		return 0, err
	}
	if *len < 0 {
		return 0, errors.Errorf("bad ContentLength: %d", len)
	}
	return *len, nil
}

func (h *hdfsSink) Close() error {
	return h.client.Close()
}

// reqNoBody is like req but it closes the response body.
func (h *hdfsSink) writeContent(
	ctx context.Context, filepath string, content io.ReadSeeker,
) (int, error) {
	wc := 0
	writer, err := h.client.Create(filepath)
	if err != nil {
		return wc, err
	}
	defer func() {
		_ = writer.Close()
	}()

	buf := make([]byte, 4096)
	for n, _ := content.Read(buf); n > 0; {
		nw, err := writer.Write(buf[0:n])
		if err != nil {
			return wc, err
		}
		wc += nw
		n, _ = content.Read(buf)
	}
	return wc, nil
}
