// Copyright 2015  The Cockroach Authors.
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

#pragma once

#include <libroach.h>
#include <rocksdb/slice.h>
#include <stdint.h>
#include <cstdarg>
#include <cmath>
#include "defines.h"
#include "protos/storage/engine/enginepb/mvcc.pb.h"
#include <roachpb/pushdown.pb.h>

namespace znbase {
    enum VType : unsigned char {
        Unknown = 0,
        Null = 1,
        NotNull = 2,
        Int = 3,
        Float = 4,
        Decimal = 5,
        Bytes = 6,
        BytesDesc = 7, // Bytes encoded descendingly
        Time = 8,
        Duration = 9,
        True = 10,
        False = 11,
        UUID = 12,
        Array = 13,
        IPAddr = 14,
        // SentinelType is used for bit manipulation to check if the encoded type
        // value requires more than 4 bits, and thus will be encoded in two bytes. It
        // is not used as a type value, and thus intentionally overlaps with the
        // subsequent type value. The 'Type' annotation is intentionally omitted here.
        SentinelType = 15,
        JSON = 15,
//    Tuple = 16,
        BitArray = 17,
        BitArrayDesc = 18,
    };
    std::string ToString(VType);

    enum SType : unsigned char{
        ColumnType_BOOL           = 0,
        ColumnType_INT            = 1,
        ColumnType_FLOAT          = 2,
        ColumnType_DECIMAL        = 3,
        ColumnType_DATE           = 4,
        ColumnType_TIMESTAMP      = 5,
        ColumnType_INTERVAL       = 6,
        ColumnType_STRING         = 7,
        ColumnType_BYTES          = 8,
        ColumnType_TIMESTAMPTZ    = 9,
        ColumnType_COLLATEDSTRING = 10,
        ColumnType_NAME           = 11,
        ColumnType_OID            = 12,
        // NULL is not supported as a table column type, however it can be
        // transferred through distsql streams.
        ColumnType_NULL           = 13,
        ColumnType_UUID           = 14,
        ColumnType_ARRAY          = 15,
        ColumnType_INET           = 16,
        ColumnType_TIME           = 17,
        ColumnType_JSONB          = 18,
        ColumnType_TUPLE          = 20,
        ColumnType_BIT            = 21,
        ColumnType_INT2VECTOR     = 200,
        ColumnType_OIDVECTOR      = 201,
    };

    // 初始的最大列ID
    const uint32_t DEFAULT_COL_ID = 10;
    // 最大解析列ID
    const uint32_t MAX_COL_ID = 256;
    // Value编解码的常量
    const int checksumUninitialized = 0;
    const int checksumSize          = 4;
    const int tagPos                = checksumSize;
    const int headerSize            = tagPos + 1;

    const int kIntZero = 136;
    const int kIntSmall = 109;
    const int kIntMax = 253;
    const int kIntMin = 128;

    const int IntMin = 128;
    const int intMaxWidth = 8;
    const int intZero     = IntMin + intMaxWidth;           // 136
    const int IntMax = 0xfd;                                // 253
    const int intSmall    = IntMax - intZero - intMaxWidth; // 109

    const uint64_t uint64AscendingEncodedLength = 8;
    const uint64_t  floatValueEncodedLength = uint64AscendingEncodedLength;

    typedef uint32_t ColumnID;
    const uint32_t NoColumnID = 0;
    const int MaxVarintLen16=3;
    const int MaxVarintLen32=5;
    const int MaxVarintLen64=10;

    const uint8_t bytesMarker           = 0x12;
    const uint8_t bytesDescMarker       = bytesMarker + 1;
    const uint8_t timeMarker            = bytesDescMarker + 1;
    const uint8_t durationBigNegMarker  = timeMarker + 1; // Only used for durations < MinInt64 nanos.
    const uint8_t durationMarker        = durationBigNegMarker + 1;
    const uint8_t durationBigPosMarker  = durationMarker + 1; // Only used for durations > MaxInt64 nanos.



    const int encodedNotNull = 0x01;
    const int floatNaN     = encodedNotNull + 1;
    const int floatNeg     = floatNaN + 1;
    const int floatZero    = floatNeg + 1;
    const int floatPos     = floatZero + 1;
    const int floatNaNDesc = floatPos + 1; // NaN encoded descendingly

    const uint64_t uvnan    = 0x7FF8000000000001;

    const int  decimalNaN               = durationBigPosMarker + 1; // 24
    const int  decimalNegativeInfinity  = decimalNaN + 1;
    const int  decimalNegLarge          = decimalNegativeInfinity + 1;
    const int  decimalNegMedium         = decimalNegLarge + 11;
    const int  decimalNegSmall          = decimalNegMedium + 1;
    const int  decimalZero              = decimalNegSmall + 1;
    const int  decimalPosSmall          = decimalZero + 1;
    const int  decimalPosMedium         = decimalPosSmall + 1;
    const int  decimalPosLarge          = decimalPosMedium + 11;
    const int  decimalInfinity          = decimalPosLarge + 1;
    const int  decimalNaNDesc           = decimalInfinity + 1;// NaN encoded descendingly
    const int  decimalTerminator        = 0x00;

    const uint8_t escape = 0x00;
    const uint8_t escapedTerm  = 0x01;
    const uint8_t escapedJSONObjectKeyTerm  = 0x02;
    const uint8_t escapedJSONArray = 0x03;
    const uint8_t escaped00 = 0xff;
    const uint8_t escapedFF = 0x00;

    struct escapes  {
        uint8_t escape;
        uint8_t escapedTerm;
        uint8_t escaped00;
        uint8_t escapedFF;
        uint8_t  marker;
    };

    //timestamp时间戳
    struct Time{
        timespec t_timespec; // t_sec:自1970年1月1日UTC以来经过的秒数,
        // t_Nsec:纳秒级返回t指定的秒内的纳秒级偏移 ,
        int  t_abbv = 0;         // t_abby在UTC以东的秒数内的偏移量。
    };

    ////decimal
    typedef int Form;   // Form specifies the form of a Decimal.
    struct big_Int {
        bool neg;// sign 标志位,表示正负，在decimal中永远为false,该标志在decimal中无意义
        std::vector<uint64_t> abs; // absolute value of the integer 该整数的绝对值
        int Sign() {
            if (abs.size() == 0) {
                return 0;
            }
            if (neg) {
                return -1;
            }
            return 1;
        }
    };

    struct Decimal {
        Form _Form;
        bool Negative=false;   //该结构初始的默认值必须为false,false:正数
        int32_t Exponent;
        struct big_Int Coeff;

        double DecimalToDouble() {
            double de = 0;
            double power = pow(10, double(Exponent));
            if(Coeff.abs.size()==0){
                de=0;
                return de;
            }

            if (Negative) {
                de = -double(Coeff.abs[0]) * power;
            } else {
                de = double(Coeff.abs[0]) * power;
            }
            return de;
        }

        int Sign() {
            if (_Form == 0&& (Coeff.Sign() == 0) ) {
                return 0;
            }
            if (Negative) {
                return -1;
            }
            return 1;
        }

        bool IsZero() {
            return Sign() == 0;
        }
    };

    const  Form  Finite  = 0;
    // Infinite is the infinite form.
    const  Form Infinite=Finite+1;
    // NaNSignaling is the signaling NaN form. It will always raise the
    // InvalidOperation condition during an operation.
    const  Form NaNSignaling=Infinite+1;
    // NaN is the NaN form.
    const  Form NaN =NaNSignaling+1;
    const int _W = 64;      // word size in bits
    // 32 or 64
    const int _S = _W / 8 ;// word size in bytes

    //UUID stuct
    const int Uuid_Size = 16;
    const int UuidValueEncodeLength = 16;
//    struct Uuid {
//        unsigned char value[Uuid_Size];
//    }; //go中该类型为全大写，c会与UUID=12重复

    typedef unsigned char Uuid;

    double HexToDouble(std::string S_Bin);
    void DecodeUint64Ascending(rocksdb::Slice &buf, uint64_t *value);

    void DecodeTableKey(rocksdb::Slice &buf, roachpb::PushDownExpr &push_down, std::vector<rocksdb::Slice> *keys);
    //======================key中Int主键解码==================
    void DecodeVarintAscending(rocksdb::Slice &buf,int64_t* value);
    //======================key中String主键解码==================
    std::string DecodeUnsafeStringAscending(rocksdb::Slice &buf, std::string *str);
    //======================key中Float主键解码==================
    void DecodeFloatAscending(rocksdb::Slice &buf, double *data);
    //======================key中Decimal主键解码==================
    std::string DecodeDecimalAscending(rocksdb::Slice &buf, struct Decimal *value);
    //======================key中Time主键解码==================
    void DecodeTimeAscending(rocksdb::Slice &buf, struct Time *value);

    bool DecodeTableIDKey(rocksdb::Slice &key, uint64_t *table_id);

    bool DecodeIndexIDKey(rocksdb::Slice &key, uint64_t *index_id);

    //======================key中Int主键解码 Desc==============
    void DecodeVarintDescending(rocksdb::Slice &buf,int64_t* value);
    //======================key中String主键解码 Desc==============
    std::string DecodeUnsafeStringDescending(rocksdb::Slice &buf, std::string *str);
    //======================key中Float主键解码 Desc==============
    void DecodeFloatDescending(rocksdb::Slice &buf, double *data);
    //======================key中Decimal主键解码 Desc==============
    std::string DecodeDecimalDescending(rocksdb::Slice &buf, struct Decimal *value);
    //======================key中Time主键解码 Desc==============
    void DecodeTimeDescending(rocksdb::Slice &buf,  struct Time *value);



    //使用slice类型解码多列
    std::string DecodeSumRow(rocksdb::Slice &buf, uint32_t col_count, std::vector<rocksdb::Slice> *values);

    std::string DecodeSumRowReq(rocksdb::Slice &buf, std::vector<rocksdb::Slice> *values,std::vector<int> require);

    //解码int
    void DecodeIntValue(rocksdb::Slice buf,int64_t *data);
    //解码bool
    void DecodeBoolValue(rocksdb::Slice buf,bool *data);
    //解码string
    void DecodeBytesValue(rocksdb::Slice buf,std::string *data);
    //解码float
    void DecodeFloatValue(rocksdb::Slice buf,double *data);
    //解码decimal
    void DecodeDecimalValue(rocksdb::Slice buf,struct Decimal *data);
    //解码timestamp
    void DecodeTimeValue(rocksdb::Slice buf,struct Time *data);
    //解码uuid
    void DecodeUUIDValue(rocksdb::Slice buf, Uuid *data);


    //16进制转成string输出
    std::string convUUIDToString(unsigned char* source);
    //string类型的日期转成天数
    int convertDateToDay(std::string str);
    //string类型的DateTime转成秒数
    int64_t convertDateTimeToDay(std::string str);
    //string类型的input_str转成decimal
    void PharseDecimal(std::string input_str,struct Decimal &dd);
    //decimal类型值之间的比较
    int CompareDecimals(struct Decimal col_val,struct Decimal fil_val);


    // FmtStatus formats the given arguments printf-style into a DBStatus.
    inline std::string Errorf(const char *fmt_str, ...) {
        va_list ap;
        std::string str;
        while (true) {
            char *buffer = new char[1024];
            va_start(ap, fmt_str);
            int expected = vsnprintf(buffer, 1024, fmt_str, ap);
            va_end(ap);
            str += std::string(buffer);
            if (expected > -1 && expected < 1024)
                break;
        }
        return str;
    }


// EncodeUint32 encodes the uint32 value using a big-endian 4 byte
// representation. The bytes are appended to the supplied buffer.
    void EncodeUint32(std::string* buf, uint32_t v);

// EncodeUint64 encodes the uint64 value using a big-endian 8 byte
// representation. The encoded bytes are appended to the supplied buffer.
    void EncodeUint64(std::string* buf, uint64_t v);

// EncodeUvarint64 encodes the uint64 value using a variable-length
// representation. The encoded bytes are appended to the supplied buffer.
    void EncodeUvarint64(std::string* buf, uint64_t v);

// DecodedUint32 decodes a fixed-length encoded uint32 from a buffer, returning
// true on a successful decode. The decoded value is returned in *value.
    bool DecodeUint32(rocksdb::Slice* buf, uint32_t* value);

// DecodedUint64 decodes a fixed-length encoded uint64 from a buffer, returning
// true on a successful decode. The decoded value is returned in *value.
    bool DecodeUint64(rocksdb::Slice* buf, uint64_t* value);

// DecodeUvarint64 decodes a variable-length encoded uint64 from a buffer,
// returning true on a successful decode. The decoded value is returned in
// *value.
    bool DecodeUvarint64(rocksdb::Slice* buf, uint64_t* value);

    const int kMVCCVersionTimestampSize = 12;

    void EncodeTimestamp(std::string& s, int64_t wall_time, int32_t logical);
    std::string EncodeTimestamp(DBTimestamp ts);

// MVCC keys are encoded as <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
    std::string EncodeKey(const rocksdb::Slice& key, int64_t wall_time, int32_t logical);

// MVCC keys are encoded as <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
    std::string EncodeKey(DBKey k);


// SplitKey splits an MVCC key into key and timestamp slices. See also
// DecodeKey if you want to decode the timestamp. Returns true on
// success and false on any decoding error.
    WARN_UNUSED_RESULT inline bool SplitKey(rocksdb::Slice buf, rocksdb::Slice* key,
                                            rocksdb::Slice* timestamp) {
        if (buf.empty()) {
            return false;
        }
        const char ts_size = buf[buf.size() - 1];
        if (ts_size >= buf.size()) {
            return false;
        }
        *key = rocksdb::Slice(buf.data(), buf.size() - ts_size - 1);
        *timestamp = rocksdb::Slice(key->data() + key->size(), ts_size);
        return true;
    }


// DecodeTimestamp an MVCC encoded timestamp. Returns true on success
// and false on any decoding error.
    WARN_UNUSED_RESULT bool DecodeTimestamp(rocksdb::Slice* timestamp, int64_t* wall_time,
                                            int32_t* logical);
    WARN_UNUSED_RESULT bool DecodeTimestamp(rocksdb::Slice buf,
                                            znbase::util::hlc::Timestamp* timestamp);

// EmptyTimestamp returns whether ts represents an empty timestamp where both
// the wall_time and logical components are zero.
    bool EmptyTimestamp(DBTimestamp ts);

// DecodeKey splits an MVCC key into a key slice and decoded
// timestamp. See also SplitKey if you want to do not need to decode
// the timestamp. Returns true on success and false on any decoding
// error.
    WARN_UNUSED_RESULT bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key, int64_t* wall_time,
                                      int32_t* logical);
    WARN_UNUSED_RESULT inline bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key, DBTimestamp* ts) {
        return DecodeKey(buf, key, &ts->wall_time, &ts->logical);
    }

    const int kLocalSuffixLength = 4;

// DecodeRangeIDKey parses a local range ID key into range ID, infix,
// suffix, and detail.
    WARN_UNUSED_RESULT bool DecodeRangeIDKey(rocksdb::Slice buf, int64_t* range_id,
                                             rocksdb::Slice* infix, rocksdb::Slice* suffix,
                                             rocksdb::Slice* detail);

// KeyPrefix strips the timestamp from an MVCC encoded key, returning
// a slice that is still MVCC encoded. This is used by the prefix
// extractor used to build bloom filters on the prefix.
    rocksdb::Slice KeyPrefix(const rocksdb::Slice& src);

}  // namespace znbase