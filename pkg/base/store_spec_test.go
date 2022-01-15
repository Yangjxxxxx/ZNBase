// Copyright 2016  The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package base

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// TestNewStoreSpec verifies that the --store arguments are correctly parsed
// into StoreSpecs.
func TestNewStoreSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		value       string
		expectedErr string
		expected    StoreSpec
	}{
		// path
		{"path=/mnt/hda1", "", StoreSpec{
			Path:  "/mnt/hda1",
			State: roachpb.ENABLE,
		}},
		{",path=/mnt/hda1", "", StoreSpec{
			Path:  "/mnt/hda1",
			State: roachpb.ENABLE,
		}},
		{"path=/mnt/hda1,", "", StoreSpec{Path: "/mnt/hda1", State: roachpb.ENABLE}},
		{",,,path=/mnt/hda1,,,", "", StoreSpec{Path: "/mnt/hda1", State: roachpb.ENABLE}},
		{"/mnt/hda1", "", StoreSpec{Path: "/mnt/hda1", State: roachpb.ENABLE}},
		{"path=", "no value specified for path", StoreSpec{}},
		{"path=/mnt/hda1,path=/mnt/hda2", "path field was used twice in store definition", StoreSpec{}},
		{"/mnt/hda1,path=/mnt/hda2", "path field was used twice in store definition", StoreSpec{}},

		// attributes
		{"path=/mnt/hda1,attrs=ssd", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"ssd"}},
			State:      roachpb.ENABLE,
		}},
		{"path=/mnt/hda1,attrs=ssd:hdd", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
			State:      roachpb.ENABLE,
		}},
		{"path=/mnt/hda1,attrs=hdd:ssd", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
			State:      roachpb.ENABLE,
		}},
		{"attrs=ssd:hdd,path=/mnt/hda1", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
			State:      roachpb.ENABLE,
		}},
		{"attrs=hdd:ssd,path=/mnt/hda1,", "", StoreSpec{
			Path:       "/mnt/hda1",
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
			State:      roachpb.ENABLE,
		}},
		{"attrs=hdd:ssd", "no path specified", StoreSpec{}},
		{"path=/mnt/hda1,attrs=", "no value specified for attrs", StoreSpec{}},
		{"path=/mnt/hda1,attrs=hdd:hdd", "duplicate attribute given for store: hdd", StoreSpec{}},
		{"path=/mnt/hda1,attrs=hdd,attrs=ssd", "attrs field was used twice in store definition", StoreSpec{}},

		// size
		{"path=/mnt/hda1,size=671088640", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 671088640}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=20GB", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 20000000000}, State: roachpb.ENABLE}},
		{"size=20GiB,path=/mnt/hda1", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 21474836480}, State: roachpb.ENABLE}},
		{"size=0.1TiB,path=/mnt/hda1", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 109951162777}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=.1TiB", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 109951162777}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=123TB", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 123000000000000}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=123TiB", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{InBytes: 135239930216448}, State: roachpb.ENABLE}},
		// %
		{"path=/mnt/hda1,size=50.5%", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 50.5}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=100%", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 100}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=1%", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 1}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=0.999999%", "store size (0.999999%) must be between 1.000000% and 100.000000%", StoreSpec{}},
		{"path=/mnt/hda1,size=100.0001%", "store size (100.0001%) must be between 1.000000% and 100.000000%", StoreSpec{}},
		// 0.xxx
		{"path=/mnt/hda1,size=0.99", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 99}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=0.5000000", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 50}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=0.01", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 1}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=0.009999", "store size (0.009999) must be between 1.000000% and 100.000000%", StoreSpec{}},
		// .xxx
		{"path=/mnt/hda1,size=.999", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 99.9}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=.5000000", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 50}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=.01", "", StoreSpec{Path: "/mnt/hda1", Size: SizeSpec{Percent: 1}, State: roachpb.ENABLE}},
		{"path=/mnt/hda1,size=.009999", "store size (.009999) must be between 1.000000% and 100.000000%", StoreSpec{}},
		// errors
		{"path=/mnt/hda1,size=0", "store size (0) must be larger than 640 MiB", StoreSpec{}},
		{"path=/mnt/hda1,size=abc", "could not parse store size (abc) strconv.ParseFloat: parsing \"\": invalid syntax", StoreSpec{}},
		{"path=/mnt/hda1,size=", "no value specified for size", StoreSpec{}},
		{"size=20GiB,path=/mnt/hda1,size=20GiB", "size field was used twice in store definition", StoreSpec{}},
		{"size=123TB", "no path specified", StoreSpec{}},

		// type
		{"type=mem,size=20GiB", "", StoreSpec{Size: SizeSpec{InBytes: 21474836480}, InMemory: true, State: roachpb.ENABLE}},
		{"size=20GiB,type=mem", "", StoreSpec{Size: SizeSpec{InBytes: 21474836480}, InMemory: true, State: roachpb.ENABLE}},
		{"size=20.5GiB,type=mem", "", StoreSpec{Size: SizeSpec{InBytes: 22011707392}, InMemory: true, State: roachpb.ENABLE}},
		{"size=20GiB,type=mem,attrs=mem", "", StoreSpec{
			Size:       SizeSpec{InBytes: 21474836480},
			InMemory:   true,
			Attributes: roachpb.Attributes{Attrs: []string{"mem"}},
			State:      roachpb.ENABLE,
		}},
		{"type=mem,size=20", "store size (20) must be larger than 640 MiB", StoreSpec{}},
		{"type=mem,size=", "no value specified for size", StoreSpec{}},
		{"type=mem,attrs=ssd", "size must be specified for an in memory store", StoreSpec{}},
		{"path=/mnt/hda1,type=mem", "path specified for in memory store", StoreSpec{}},
		{"path=/mnt/hda1,type=other", "other is not a valid store type", StoreSpec{}},
		{"path=/mnt/hda1,type=mem,size=20GiB", "path specified for in memory store", StoreSpec{}},

		//State
		{"path=/", "", StoreSpec{Path: "/", State: roachpb.ENABLE}},
		{"path=/,state=ENABLE", "", StoreSpec{Path: "/", State: roachpb.ENABLE}},
		{"path=/,state=DISABLE", "", StoreSpec{Path: "/", State: roachpb.DISABLE}},
		{"path=/,state=", "no value specified for state", StoreSpec{}},
		{"path=/,state=ASD", "field state is not a valid store state", StoreSpec{}},
		{"path=/,state=0", "field state is not a valid store state", StoreSpec{}},
		{"path=/,state=--", "field state is not a valid store state", StoreSpec{}},
		{"path=/,states=ENABLE", "states is not a valid store field", StoreSpec{}},
		{"path=/,states=DISABLE", "states is not a valid store field", StoreSpec{}},

		// RocksDB
		{"path=/,rocksdb=key1=val1;key2=val2", "", StoreSpec{Path: "/", RocksDBOptions: "key1=val1;key2=val2", State: roachpb.ENABLE}},

		// all together
		{"path=/mnt/hda1,attrs=hdd:ssd,size=20GiB", "", StoreSpec{
			Path:       "/mnt/hda1",
			Size:       SizeSpec{InBytes: 21474836480},
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
			State:      roachpb.ENABLE,
		}},
		{"type=mem,attrs=hdd:ssd,size=20GiB", "", StoreSpec{
			Size:       SizeSpec{InBytes: 21474836480},
			InMemory:   true,
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
			State:      roachpb.ENABLE,
		}},
		{"type=mem,attrs=hdd:ssd,size=20GiB,state=DISABLE", "", StoreSpec{
			Size:       SizeSpec{InBytes: 21474836480},
			InMemory:   true,
			Attributes: roachpb.Attributes{Attrs: []string{"hdd", "ssd"}},
			State:      roachpb.DISABLE,
		}},

		// other error cases
		{"", "no value specified", StoreSpec{}},
		{",", "no path specified", StoreSpec{}},
		{",,,", "no path specified", StoreSpec{}},
		{"path=/mnt/hda1,something=abc", "something is not a valid store field", StoreSpec{}},
		{"something=abc", "something is not a valid store field", StoreSpec{}},
		{"type=mem,other=abc", "other is not a valid store field", StoreSpec{}},
	}

	for i, testCase := range testCases {
		storeSpec, err := NewStoreSpec(testCase.value)
		if err != nil {
			if len(testCase.expectedErr) == 0 {
				t.Errorf("%d(%s): no expected error, got %s", i, testCase.value, err)
			}
			if testCase.expectedErr != fmt.Sprint(err) {
				t.Errorf("%d(%s): expected error \"%s\" does not match actual \"%s\"", i, testCase.value,
					testCase.expectedErr, err)
			}
			continue
		}
		if len(testCase.expectedErr) > 0 {
			t.Errorf("%d(%s): expected error %s but there was none", i, testCase.value, testCase.expectedErr)
			continue
		}
		if !reflect.DeepEqual(testCase.expected, storeSpec) {
			t.Errorf("%d(%s): actual doesn't match expected\nactual:   %+v\nexpected: %+v", i,
				testCase.value, storeSpec, testCase.expected)
		}

		// Now test String() to make sure the result can be parsed.
		storeSpecString := storeSpec.String()
		storeSpec2, err := NewStoreSpec(storeSpecString)
		if err != nil {
			t.Errorf("%d(%s): error parsing String() result: %s", i, testCase.value, err)
			continue
		}
		// Compare strings to deal with floats not matching exactly.
		if !reflect.DeepEqual(storeSpecString, storeSpec2.String()) {
			t.Errorf("%d(%s): actual doesn't match expected\nactual:   %#+v\nexpected: %#+v", i, testCase.value,
				storeSpec, storeSpec2)
		}
	}
}
