package baseicl

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/icl/cliicl/cliflagsicl"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// DefaultAlternatePeriod 默认轮寻周期
const DefaultAlternatePeriod = time.Hour * 24 * 7 // 1 week, give or take time changes.

// DefaultTextFieldValue 默认的普通文本，没有加密
const DefaultTextFieldValue = "plain"

// StoreEncryptedInfo 包含了 --enterprise-encryption 设定的参数值.
type StoreEncryptedInfo struct {
	StorePath       string
	KeyPath         string
	OldKeyPath      string
	AlternatePeriod time.Duration
}

// ConvertEncryptionOpBuf 加密信息转换为protobuf
func (es StoreEncryptedInfo) ConvertEncryptionOpBuf() ([]byte, error) {
	opts := EncryptionOptions{
		KeySource: EncryptionKeySource_KeyFiles,
		KeyFiles: &EncryptionKeyFiles{
			CurrentKey: es.KeyPath,
			OldKey:     es.OldKeyPath,
		},
		DataKeyRotationPeriod: int64(es.AlternatePeriod / time.Second),
	}

	return protoutil.Marshal(&opts)
}

// 加密信息的完整他字符串
func (es StoreEncryptedInfo) String() string {
	// 打印所有字段
	return fmt.Sprintf("path=%s,key=%s,old-key=%s,rotation-period=%s",
		es.StorePath, es.KeyPath, es.OldKeyPath, es.AlternatePeriod)
}

// NewStoreEncryptionInfo 输入的字符信息构造加密信息.
func NewStoreEncryptionInfo(value string) (StoreEncryptedInfo, error) {
	const pathField = "path"
	var es StoreEncryptedInfo
	es.AlternatePeriod = DefaultAlternatePeriod

	used := make(map[string]struct{})
	for _, split := range strings.Split(value, ",") {
		if len(split) == 0 {
			continue
		}
		subSplits := strings.SplitN(split, "=", 2)
		if len(subSplits) == 1 {
			return StoreEncryptedInfo{}, fmt.Errorf("field not in the form <key>=<value>: %s", split)
		}
		field := strings.ToLower(subSplits[0])
		value := subSplits[1]
		if _, ok := used[field]; ok {
			return StoreEncryptedInfo{}, fmt.Errorf("%s field was used twice in encryption definition", field)
		}
		used[field] = struct{}{}

		if len(field) == 0 {
			return StoreEncryptedInfo{}, fmt.Errorf("empty field")
		}
		if len(value) == 0 {
			return StoreEncryptedInfo{}, fmt.Errorf("no value specified for %s", field)
		}

		switch field {
		case pathField:
			var err error
			es.StorePath, err = base.GetAbsoluteStorePath(pathField, value)
			if err != nil {
				return StoreEncryptedInfo{}, err
			}
		case "key":
			if value == DefaultTextFieldValue {
				es.KeyPath = DefaultTextFieldValue
			} else {
				var err error
				es.KeyPath, err = base.GetAbsoluteStorePath("key", value)
				if err != nil {
					return StoreEncryptedInfo{}, err
				}
			}
		case "old-key":
			if value == DefaultTextFieldValue {
				es.OldKeyPath = DefaultTextFieldValue
			} else {
				var err error
				es.OldKeyPath, err = base.GetAbsoluteStorePath("old-key", value)
				if err != nil {
					return StoreEncryptedInfo{}, err
				}
			}
		case "rotation-period":
			var err error
			es.AlternatePeriod, err = time.ParseDuration(value)
			if err != nil {
				return StoreEncryptedInfo{}, errors.Wrapf(err, "could not parse rotation-duration value: %s", value)
			}
		default:
			return StoreEncryptedInfo{}, fmt.Errorf("%s is not a valid enterprise-encryption field", field)
		}
	}

	// 检查所有字段是否赋值
	if es.StorePath == "" {
		return StoreEncryptedInfo{}, fmt.Errorf("no path specified")
	}
	if es.KeyPath == "" {
		return StoreEncryptedInfo{}, fmt.Errorf("no key specified")
	}
	if es.OldKeyPath == "" {
		return StoreEncryptedInfo{}, fmt.Errorf("no old-key specified")
	}

	return es, nil
}

// StoreEncryptedInfoVector 包含了一组StoreEncryptedInfo
type StoreEncryptedInfoVector struct {
	Specs []StoreEncryptedInfo
}

var _ pflag.Value = &StoreEncryptedInfoVector{}

// String 返回数组中所有加密信息
func (encl StoreEncryptedInfoVector) String() string {
	var buffer bytes.Buffer
	for _, ss := range encl.Specs {
		fmt.Fprintf(&buffer, "--%s=%s ", cliflagsicl.EnterpriseEncryption.Name, ss)
	}
	// Trim the extra space from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// Type 返回默认的string格式类型.
func (encl *StoreEncryptedInfoVector) Type() string {
	return "StoreEncryptedInfo"
}

// Set 添加一个新值.
func (encl *StoreEncryptedInfoVector) Set(value string) error {
	spec, err := NewStoreEncryptionInfo(value)
	if err != nil {
		return err
	}
	if encl.Specs == nil {
		encl.Specs = []StoreEncryptedInfo{spec}
	} else {
		encl.Specs = append(encl.Specs, spec)
	}
	return nil
}

// PopulateStoreSpecWithEncryption 遍历 StoreEncryptedInfoVector 查找匹配路径.
// 不匹配报错
// 匹配存储区设置了一些与加密相关的字段
func PopulateStoreSpecWithEncryption(
	storeSpecs base.StoreSpecList, encryptedInfos StoreEncryptedInfoVector,
) error {
	for _, es := range encryptedInfos.Specs {
		found := false
		for i := range storeSpecs.Specs {
			if storeSpecs.Specs[i].Path != es.StorePath {
				continue
			}

			// 找到匹配路径
			if storeSpecs.Specs[i].UseFileRegistry {
				return fmt.Errorf("store with path %s already has an encryption setting",
					storeSpecs.Specs[i].Path)
			}

			// 绝对路径需要
			storeSpecs.Specs[i].UseFileRegistry = true
			opts, err := es.ConvertEncryptionOpBuf()
			if err != nil {
				return err
			}
			storeSpecs.Specs[i].ExtraOptions = opts
			found = true
			break
		}
		if !found {
			return fmt.Errorf("no store with path %s found for encryption setting: %v", es.StorePath, es)
		}
	}
	return nil
}

// GetEncryptedOptionsForStore 根据输入路径查找store对应的加密信息
func GetEncryptedOptionsForStore(
	dir string, encryptedInfos StoreEncryptedInfoVector,
) ([]byte, error) {
	// 需要绝对路径
	path, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "could not find absolute path for %s ", dir)
	}

	for _, es := range encryptedInfos.Specs {
		if es.StorePath == path {
			return es.ConvertEncryptionOpBuf()
		}
	}

	return nil, nil
}
