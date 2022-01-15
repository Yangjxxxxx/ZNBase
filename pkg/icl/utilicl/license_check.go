// Copyright 2017 The Cockroach Authors.
//
// Licensed as a Cockroach Enterprise file under the ZNBase Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/znbasedb/znbase/blob/master/licenses/ICL.txt

package utilicl

import (
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/icl/utilicl/licenseicl"
	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

var enterpriseLicense = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		"enterprise.license",
		"the encoded cluster license",
		"",
		func(sv *settings.Values, s string) error {
			_, err := licenseicl.Decode(s)
			return err
		},
	)
	s.SetConfidential()
	return s
}()

var testingEnterpriseEnabled = true

// TestingEnableEnterprise allows overriding the license check in tests.
func TestingEnableEnterprise() func() {
	before := testingEnterpriseEnabled
	testingEnterpriseEnabled = true
	return func() {
		testingEnterpriseEnabled = before
	}
}

// TestingDisableEnterprise allows re-enabling the license check in tests.
//func TestingDisableEnterprise() func() {
//	before := testingEnterpriseEnabled
//	testingEnterpriseEnabled = false
//	return func() {
//		testingEnterpriseEnabled = before
//	}
//}

// CheckCommercialFeatureEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
func CheckCommercialFeatureEnabled(
	st *cluster.Settings, cluster uuid.UUID, org, feature string,
) error {
	//TODO comment here to ensure you can use all enterprise features everywhere (Edit by Jerry 2019-08-07-14:34)
	return nil
	//if testingEnterpriseEnabled {
	//	return nil
	//}
	//return checkEnterpriseEnabledAt(st, timeutil.Now(), cluster, org, feature)
}

func init() {
	base.CheckCommercialFeatureEnabled = CheckCommercialFeatureEnabled
	base.LicenseType = getLicenseType
}

//func checkEnterpriseEnabledAt(
//	st *cluster.Settings, at time.Time, cluster uuid.UUID, org, feature string,
//) error {
//	var lic *licenseicl.License
//	// FIXME(tschottdorf): see whether it makes sense to cache the decoded
//	// license.
//	if str := enterpriseLicense.Get(&st.SV); str != "" {
//		var err error
//		if lic, err = licenseicl.Decode(str); err != nil {
//			return err
//		}
//	}
//	return lic.Check(at, cluster, org, feature)
//}

func getLicenseType(st *cluster.Settings) (string, error) {
	str := enterpriseLicense.Get(&st.SV)
	if str == "" {
		return "None", nil
	}
	lic, err := licenseicl.Decode(str)
	if err != nil {
		return "", err
	}
	return lic.Type.String(), nil
}
