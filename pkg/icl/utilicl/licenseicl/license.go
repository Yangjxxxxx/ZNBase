package licenseicl

import (
	"bytes"
	"encoding/base64"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

// LicensePrefix 是许可证字符串上的前缀，以使它们易于识别。
const LicensePrefix = "crl-0-"

// Encode 将许可证序列化为base64字符串。
func (l License) Encode() (string, error) {
	bytes, err := protoutil.Marshal(&l)
	if err != nil {
		return "", err
	}
	return LicensePrefix + base64.RawStdEncoding.EncodeToString(bytes), nil
}

// Decode 尝试读取base64编码的许可证。
func Decode(s string) (*License, error) {
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, LicensePrefix) {
		return nil, errors.New("invalid license string")
	}
	s = strings.TrimPrefix(s, LicensePrefix)
	data, err := base64.RawStdEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.Wrap(err, "invalid license string")
	}
	var lic License
	if err := protoutil.Unmarshal(data, &lic); err != nil {
		return nil, errors.Wrap(err, "invalid license string")
	}
	return &lic, nil
}

// Check returns an error if the license is empty or not currently valid.
func (l *License) Check(at time.Time, cluster uuid.UUID, org, feature string) error {
	if l == nil {
		// TODO(dt): link to some stable URL that then redirects to a helpful page
		// that explains what to do here.
		link := "https://znbaselabs.com/pricing?cluster="
		return errors.Errorf(
			"use of %s requires an enterprise license. "+
				"see %s%s for details on how to enable enterprise features",
			feature,
			link,
			cluster.String(),
		)
	}

	// We extend some grace period to enterprise license holders rather than
	// suddenly throwing errors at them.
	if l.ValidUntilUnixSec > 0 && l.Type != License_Enterprise {
		if expiration := timeutil.Unix(l.ValidUntilUnixSec, 0); at.After(expiration) {
			licensePrefix := ""
			switch l.Type {
			case License_NonCommercial:
				licensePrefix = "non-commercial "
			case License_Evaluation:
				licensePrefix = "evaluation "
			}
			return errors.Errorf(
				"Use of %s requires an enterprise license. Your %slicense expired on %s. If you're "+
					"interested in getting a new license, please contact ZNBaseDB technical support "+
					"and we can help you out.",
				feature,
				licensePrefix,
				expiration.Format("January 2, 2006"),
			)
		}
	}

	if l.ClusterID == nil {
		if strings.EqualFold(l.OrganizationName, org) {
			return nil
		}
		return errors.Errorf("license valid only for %q", l.OrganizationName)
	}

	for _, c := range l.ClusterID {
		if cluster == c {
			return nil
		}
	}

	// no match, so compose an error message.
	var matches bytes.Buffer
	for i, c := range l.ClusterID {
		if i > 0 {
			matches.WriteString(", ")
		}
		matches.WriteString(c.String())
	}
	return errors.Errorf(
		"license for cluster(s) %s is not valid for cluster %s", matches.String(), cluster.String(),
	)
}
