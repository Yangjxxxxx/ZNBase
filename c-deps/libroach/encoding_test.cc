// Copyright 2017 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

#include <cinttypes>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>
#include <vector>
#include "encoding.h"

using namespace znbase;

TEST(Libroach, Encoding) {
    // clang-format off
    std::vector<uint32_t> cases32{
            0, 1, 2, 3,
            1 << 16, (1 << 16) - 1, (1 << 16) + 1,
            1 << 24, (1 << 24) - 1, (1 << 24) + 1,
            UINT32_MAX - 1, UINT32_MAX,
    };

    std::vector<uint64_t> cases64{
            0, 1, 2, 3,
            1 << 16, (1 << 16) - 1, (1 << 16) + 1,
            1 << 24, (1 << 24) - 1, (1 << 24) + 1,
            UINT32_MAX - 1, UINT32_MAX,
            (1ULL << 32) + 1,
            1ULL << 40, (1ULL << 40) - 1, (1ULL << 40) + 1,
            1ULL << 48, (1ULL << 48) - 1, (1ULL << 48) + 1,
            1ULL << 56, (1ULL << 56) - 1, (1ULL << 56) + 1,
            UINT64_MAX - 1, UINT64_MAX
    };
    // clang-format on

    std::mt19937 rng;
    std::uniform_int_distribution<uint32_t> uniform32;
    std::uniform_int_distribution<uint64_t> uniform64;
    for (int i = 0; i < 32; i++) {
        cases32.push_back(uniform32(rng));
        cases64.push_back(uniform64(rng));
    }

    cases64.insert(cases64.end(), cases32.begin(), cases32.end());

    for (auto it = cases32.begin(); it != cases32.end(); it++) {
        std::string buf;
        EncodeUint32(&buf, *it);
        uint32_t out;
        rocksdb::Slice slice(buf);
        DecodeUint32(&slice, &out);
        EXPECT_EQ(*it, out);
    }

    for (auto it = cases64.begin(); it != cases64.end(); it++) {
        std::string buf;
        EncodeUint64(&buf, *it);
        uint64_t out;
        rocksdb::Slice slice(buf);
        DecodeUint64(&slice, &out);
        EXPECT_EQ(*it, out);
    }

    for (auto it = cases64.begin(); it != cases64.end(); it++) {
        std::string buf;
        EncodeUvarint64(&buf, *it);
        uint64_t out;
        rocksdb::Slice slice(buf);
        DecodeUvarint64(&slice, &out);
        EXPECT_EQ(*it, out);
    }
}
//类型的编解码测试
TEST(Libroach, Decimal) {
    unsigned char data1[] = {21, 2, 40, 137};
    rocksdb::Slice input((char *) data1, sizeof(data1));
    struct Decimal value_dec;
    DecodeDecimalValue(input,&value_dec);
    std::cout<<"value_dec.Negative="<< value_dec.Negative<<"  value_dec.Exponent="<<value_dec.Exponent<<std::endl;
}

TEST(Libroach, PharseDecimal) {
    struct Decimal value_dec;
    std::string ss = "-10.00";
    PharseDecimal(ss,value_dec);
    std::cout << value_dec.Coeff.abs[0] << std::endl;
}

TEST(Libroach, CompareDecimal) {
    struct Decimal value_dec1;
    std::string ss1 = "-10.00";
    PharseDecimal(ss1,value_dec1);

    struct Decimal value_dec2;
    std::string ss2 = "-10.00";
    PharseDecimal(ss2,value_dec2);
    int res = CompareDecimals(value_dec1,value_dec2);
    std::cout << res << std::endl;
}

TEST(Libroach, Float) {//0
    unsigned char data1[] = {20, 0, 0 ,0 ,0 ,0 ,0 ,0, 0};
    rocksdb::Slice input((char *) data1, sizeof(data1));
    double val;
    DecodeFloatValue(input,&val);
    std::cout << val << std::endl;
    unsigned char data2[] = {4};
    rocksdb::Slice input2((char *) data2, sizeof(data2));
    double val1;
    DecodeFloatAscending(input2,&val1);
    std::cout << val1<< std::endl;
}