// Copyright 2014  The Cockroach Authors.
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

#include "encoding.h"
#include <rocksdb/slice.h>
#include "db.h"
#include "keys.h"

namespace znbase {

    rocksdb::Slice slice(rocksdb::Slice &src, int offset, size_t length) {
        assert((src.size() - offset) >= length);
        rocksdb::Slice str = rocksdb::Slice(src.data() + offset, length);
        return str;
    }

    rocksdb::Slice slice(rocksdb::Slice &src, int offset) {
        return slice(src, offset, src.size() - offset);
    }

    //=================解码ASC int key==============================
    bool DecodeUvarintASC64(rocksdb::Slice &buf, uint64_t *value) {
        if (buf.size() == 0) {
            return false;
        }
        int length = uint8_t((buf)[0]) - kIntZero;
        buf.remove_prefix(1);  // skip length byte
        if (length <= kIntSmall) {
            *value = uint64_t(length);
            return true;
        }
        length -= kIntSmall;
        if (length < 0 || length > 8) {
            return false;
        } else if ((int) buf.size() < length) {
            return false;
        }
        *value = 0;
        for (int i = 0; i < length; i++) {
            *value = (*value << 8) | uint8_t((buf)[i]);
        }
        buf.remove_prefix(length);
        return true;
    }

    bool DecodeVarintASC64(rocksdb::Slice &buf,int64_t *value){
        if (buf.size() == 0) {
            return false;
        }
        int length = uint8_t((buf)[0]) - kIntZero;
        if(length < 0){
            length = -length;
            buf.remove_prefix(1);
            if((int)buf.size()<length){
                *value = 0;
                return false;
            }
            *value = 0;
            for(int i=0;i < length;i++){
                *value = (*value << 8) | uint8_t(~(buf)[i]);
            }
            *value=~(*value);
            buf.remove_prefix(length);
            return true;
        }
        uint64_t res = 0;
        DecodeUvarintASC64(buf,&res);
//        if(res >(1<<63 - 1)){
//            *value = 0;
//            return true;
//        }
        *value = static_cast<int64_t>(res);
        return true;
    }

    //============decode string key Asc=====
    void DecodeBytesInternal(std::string& buf, std::string *str, struct escapes e, bool expectMarker) {
        if (expectMarker) {
            if (buf.length() == 0 || buf[0] != e.marker) {
                std::cerr << "error: id not find marker in buffer" << std::endl;
                exit(1);
            }
            buf = buf.substr(1);
        }
        while (1) {
            int i = buf.find_first_of(e.escape);
            if (i == -1) {
                std::cerr << "error: did not find terminator %#x in buffer" << std::endl;
                exit(1);
            }
            if ((i + 1) >= int(buf.length())) {
                std::cerr << "error: malformed escape in buffer" << std::endl;
                exit(1);
            }
            uint8_t v = uint8_t(buf[i + 1]);
            if (v == e.escapedTerm) {
                if (str == nullptr) {
                    *str = buf.substr(0,i);
                } else {
                    *str = *str+buf.substr(0,i);
                }
                buf = buf.substr(i+2);
                return;
            }
            if (v != e.escaped00)
            {
                std::cerr << "error: unknown escape sequence" << std::endl;
                exit(1);
            }
            *str = *str+buf.substr(0,i);
            str->push_back(e.escapedFF) ;
            buf = buf.substr(i+2);
        }
    }

    int32_t getColumnType(roachpb::PushDownExpr &pushdown, int32_t col_id) {
        for (int i = 0; i < pushdown.col_ids_size(); i++) {
            if(col_id == pushdown.col_ids(i))
                return pushdown.col_types(i);
        }
        return -1;
    }

    void DecodeTableKey(rocksdb::Slice &buf, roachpb::PushDownExpr &push_down, std::vector<rocksdb::Slice> *keys){
        *keys = std::vector<rocksdb::Slice>(push_down.col_ids_size()+1);
        int64_t col_val1=0;
        std::string str = " ";
        std::string output_str = "";
        std::string output_decimal = "";
        int direct_Flag = 0;
        double res_float;
        struct Decimal key_dec;
        struct Time key_time;
        int res = 0;
        for(int i=0;i<push_down.primary_cols_size();i++){
            int col = push_down.primary_cols(i);
            if(push_down.primary_cols_direct_size() != 0){
                direct_Flag = push_down.primary_cols_direct(i);
            }else{
                direct_Flag = 0;
            }
            int styp = getColumnType(push_down,col);
            switch (styp){
                case ColumnType_BOOL:case ColumnType_INT:case ColumnType_DATE:case ColumnType_TIME:{
                    rocksdb::Slice input_int = buf;
                    if(direct_Flag == 1){
                        DecodeVarintDescending(buf, &col_val1);
                    }else{
                        DecodeVarintAscending(buf, &col_val1);
                    }
                    res = input_int.size()-buf.size();
                    if(input_int.size()>=res){
                        input_int.remove_suffix(input_int.size()-res);
                    }
                    (*keys)[col] = input_int;
                    break;
                }
                case ColumnType_STRING:case ColumnType_BYTES:case ColumnType_UUID:{
                    rocksdb::Slice input_str = buf;
                    output_str.clear();
                    if(direct_Flag == 1){
                        output_str = DecodeUnsafeStringDescending(buf,&str);
                    }else{
                        output_str = DecodeUnsafeStringAscending(buf,&str);
                    }
                    res = input_str.size()-output_str.size();
                    buf.remove_prefix(res);
                    if(input_str.size()>=res){
                        input_str.remove_suffix(input_str.size()-res);
                    }
                    (*keys)[col] = input_str;
                    break;
                }
                case ColumnType_FLOAT:{
                    rocksdb::Slice input_float = buf;
                    if(direct_Flag == 1){
                        DecodeFloatDescending(buf,&res_float);
                    }else{
                        DecodeFloatAscending(buf,&res_float);
                    }
                    res = input_float.size()-buf.size();
                    if(input_float.size()>=res){
                        input_float.remove_suffix(input_float.size()-res);
                    }
                    (*keys)[col] = input_float;
                    break;
                }
                case ColumnType_DECIMAL:{
                    rocksdb::Slice input_decimal = buf;
                    output_decimal.clear();
                    if(direct_Flag == 1){
                        output_decimal =DecodeDecimalDescending(buf,&key_dec);
                    }else{
                        output_decimal =DecodeDecimalAscending(buf,&key_dec);
                    }
                    res = input_decimal.size()-output_decimal.size();
                    buf.remove_prefix(res);
                    if(input_decimal.size()>=res){
                        input_decimal.remove_suffix(input_decimal.size()-res);
                    }
                    (*keys)[col] = input_decimal;
                    break;
                }
                case ColumnType_TIMESTAMP:case ColumnType_TIMESTAMPTZ:{
                    rocksdb::Slice input_timestamp = buf;
                    if(direct_Flag == 1){
                        DecodeTimeDescending(buf,&key_time);
                    }else{
                        DecodeTimeAscending(buf,&key_time);
                    }
                    res = input_timestamp.size()-buf.size();
                    if(input_timestamp.size()>=res){
                        input_timestamp.remove_suffix(input_timestamp.size()-res);
                    }
                    (*keys)[col] = input_timestamp;
                    break;
                }
                default:
                    (*keys)[col] = buf;
                    break;
            }
        }
    }


    //解码tableid和索引id
    bool DecodeUvarint64(rocksdb::Slice &buf, uint64_t *value) {
        if (buf.size() == 0) {
            return false;
        }
        int length = uint8_t((buf)[0]) - kIntZero;
        buf.remove_prefix(1);  // skip length byte
        if (length <= kIntSmall) {
            *value = uint64_t(length);
            return true;
        }
        length -= kIntSmall;
        if (length < 0 || length > 8) {
            return false;
        } else if ((int) buf.size() < length) {
            return false;
        }
        *value = 0;
        for (int i = 0; i < length; i++) {
            *value = (*value << 8) | uint8_t((buf)[i]);
        }
        buf.remove_prefix(length);
        return true;
    }


    //=========================解码tableid===========================
    bool DecodeTableIDKey(rocksdb::Slice &key, uint64_t *table_id) {
        // IntMin      = 0x80 // 128
        // IntMax = 0xfd // 253
        //检查buf[0] 是否整型
        if (key.size() < 1) {
            *table_id = 0;
            return true;
        }
        uint p_type = key[0];
        if (p_type < 180) {
            return false;
        }
        // DecodeUvarint64会修改原始值
        // rocksmt::Slice buf(static_cast<char*>(malloc(key.size())), key.size());
        // memcpy((void*)buf.data_, key.data_, key.size());
//        rocksmt::Slice buf = key;

        bool ok = DecodeUvarint64(key, table_id);
        if (int(*table_id) < 0) {
            *table_id = 0;
        }
        // delete buf.data_;
        return ok;
    }

    //=========================解码indexid===========================
    bool DecodeIndexIDKey(rocksdb::Slice &key, uint64_t *index_id) {
        // IntMin      = 0x80 // 128
        // IntMax = 0xfd // 253
        //检查buf[0] 是否整型
        if (key.size() < 1) {
            *index_id = 0;
            return true;
        }
        uint p_type = key[0];
        if (p_type < 180) {
            return false;
        }
        // DecodeUvarint64会修改原始值
        // rocksmt::Slice buf(static_cast<char*>(malloc(key.size())), key.size());
        // memcpy((void*)buf.data_, key.data_, key.size());
//        rocksmt::Slice buf = key;

        bool ok = DecodeUvarint64(key, index_id);
        if (int(*index_id) < 0) {
            *index_id = 0;
        }
        // delete buf.data_;
        return ok;
    }

    //decode float key Asc
    int PeekType(rocksdb::Slice buf) {
        if (buf.size() >= 1) {
            int type = int(buf[0]);
            return type;
        }
        return 0;
    }

    std::string TransformUintToBit(uint64_t n) {
        std::string str = "";
        for (int i = 63; i >= 0; i--) {
            str += std::to_string(((n >> i) & 1));//与1做位操作，前面位数均为0
        }

        return str;
    }

    void OnesComplement(std::string &b, int64_t n) {    //TODO 字符串每个直接取反，效率可能比较低，需要参考go优化
        //加入参数n，表示从第几位开始取反，减少内存频繁复制
        for (int i = n; i < int(b.length()); i++) {
            b[i] = ~(b[i]);
        }
    }

    //========================string Desc==============================
    void DecodeBytesDescending(std::string& buf, std::string *str) {
        // Always pass an `r` to make sure we never get back a sub-slice of `b`,
        // since we're going to modify the contents of the slice.
        if ((*str).size() == 0) {
            *str = "";
        }
        struct escapes descendingEscapes{static_cast<uint8_t>(~(escape)), static_cast<uint8_t>(~(escapedTerm)),
                                         static_cast<uint8_t>(~(escaped00)), static_cast<uint8_t>(~(escapedFF)),
                                         bytesDescMarker};
        DecodeBytesInternal(buf, str, descendingEscapes, true);
        int64_t n = 0;
        OnesComplement(*str, n);
        return ;
    }

    std::string DecodeUvarintAscending(std::string b, int64_t *value) {
        if (b.length() == 0) {
            return b;
        }
        //强制类型转换将char*转成uint8型
        const uint8_t *c = reinterpret_cast<const uint8_t *>(b.data());
        int length = int(c[0]) - intZero;
        b.erase(0, 1);
        if (length <= intSmall) {
            *value = uint64_t(length);
            return b;
        }
        length -= intSmall;
        std::string s("");//空字符
        if ((length < 0) || (length > 8)) {
            *value = 0;
            return s;
        } else if (int(b.length()) < length) {
            *value = 0;
            return s;
        }
        const uint8_t *d = reinterpret_cast<const uint8_t *>(b.data());
        *value = 0;
        for (int i = 0; i < length; i++) {
            *value = (*value << 8) | uint8_t(d[i]);
        }
        std::string result(b, length, b.length() - length);
        return result;
    }

    int findDecimalTerminator(std::string buf) {
        int idx = buf.find(decimalTerminator);
        if (idx != -1) {
            return idx;
        }
        std::cerr << " error: did not find terminator  decimalTerminator in buffer" << std::endl;
        exit(1);
    }

    std::string DecodeUvarintDescending(std::string b, uint64_t *x) {
        if (b.length() == 0) {
            std::cerr << " error: insufficient bytes to decode uvarint value" << std::endl;
            exit(1);
        }
        int length = intZero - static_cast<uint8_t >(b[0]);
        b = b.substr(1); // skip length byte
        if (length < 0 || length > 8) {
            std::cerr << " error: invalid uvarint length " << std::endl;
            exit(1);
        } else if ((int) b.length() < length) {
            std::cerr << " error: insufficient bytes to decode uvarint value " << std::endl;
            exit(1);
        }
        int ex = 0;
        for (int i = 0; i < length; i++) {
            ex = (ex << 8) | uint64_t(~(b[i]));
        }
        *x = ex;
        return b.substr(length);
    }

    void decodeSmallNumber(bool negative, std::string buf, std::string tmp,
                           int *e, std::string *m, std::string *rest, std::string *newTmp) {
        uint64_t ex = 0;
        std::string r = "";
        if (negative) {
            r = DecodeUvarintAscending(buf.substr(1), reinterpret_cast<int64_t *>(&ex));
        } else {
            r = DecodeUvarintDescending(buf.substr(1), &ex);
        }
        int idx = findDecimalTerminator(r);

        *m = r.substr(0, idx);
        if (negative) {
            std::string mCpy = "";
            int k = m->length();
            if (k <= (int) tmp.length()) {
                mCpy = tmp.substr(0, k);
                tmp = tmp.substr(k);
            } else {
                mCpy.resize(k);
            }
            mCpy.replace(0, m->length(), *m);
            OnesComplement(mCpy, 0);
            *m = mCpy;
        }
        *e = int(-ex);
        *rest = r.substr(idx + 1);
        *newTmp = tmp;
    }

    void decodeMediumNumber(bool negative, std::string buf, std::string tmp,
                            int *e, std::string *m, std::string *rest, std::string *newTmp) {
        int idx = findDecimalTerminator(buf.substr(1));
        *m = buf.substr(1, idx);
        if (negative) {
            *e = int(decimalNegMedium - buf[0]);
            std::string mCpy = "";
            int k = m->length();
            if (k <= (int) tmp.length()) {
                mCpy = tmp.substr(0, k);
                tmp = tmp.substr(k);
            } else {
                mCpy.resize(k);
            }
            // copy(mCpy, m);
            mCpy.replace(0, m->length(), *m);
            OnesComplement(mCpy, 0);
            *m = mCpy;
        } else {
            *e = int(buf[0] - decimalPosMedium);
        }
        *rest = buf.substr(idx + 2);
        *newTmp = tmp;
    }

    void decodeLargeNumber(bool negative, std::string buf, std::string tmp,
                           int *e, std::string *m, std::string *rest, std::string *newTmp) {
        uint64_t ex = 0;
        std::string r = "";
        if (negative) {
            r = DecodeUvarintDescending(buf.substr(1), &ex);
        } else {
            r = DecodeUvarintAscending(buf.substr(1), reinterpret_cast<int64_t *>(&ex));
        }
        int idx = findDecimalTerminator(r);

        *m = r.substr(0, idx);
        if (negative) {
            std::string mCpy = "";
            int k = m->length();
            if (k <= (int) tmp.length()) {
                mCpy = tmp.substr(0, k);
                tmp = tmp.substr(k);
            } else {
                mCpy.resize(k);
            }
            mCpy.replace(0, m->length(), *m);
            OnesComplement(mCpy, 0);
            *m = mCpy;
        }
        *e = int(ex);
        *rest = r.substr(idx + 1);
        *newTmp = tmp;
    }

    // makeDecimalFromMandE reconstructs the decimal from the mantissa M and
    // exponent E.
    void makeDecimalFromMandE(bool negative, int e, std::string m, struct Decimal *value) {
        if (m.length() == 0) {
            std::cerr << " error: expected mantissa, got zero bytes" << std::endl;
            exit(1);
        }
        // ±dddd.
        std::string b = "";
        int n = m.length() * 2 + 1;
        if ((int) b.capacity() < n) {
            b.resize(0);
            b.reserve(n);
        }
        for (int i = 0; i < (int) m.length(); i++) {
            uint8_t mi = m[i];
            int t = int(mi / 2);
            if (t < 0 || t > 99) {
                std::cerr << " error: base-100 encoded digit out of range [0,99]" << std::endl;
                exit(1);
            }
            b.push_back(uint8_t(t / 10) + '0');
            b.push_back(uint8_t(t % 10) + '0');
        }

        if (b[b.length() - 1] == '0') {
            b = b.substr(0, b.length() - 1);
        }

        int exp = 2 * e - b.length();
        value->Exponent = int32_t(exp);
        // We unsafely convert the []byte to a string to avoid the usual allocation
        // when converting to a string.
        //        s := *(*string)(unsafe.Pointer(&b));
        //        _, ok := dec.Coeff.SetString(s, 10)；
        value->Coeff.abs.push_back(atoll(b.data()));          //TODO应当根据不同长度转换
        value->Negative = negative;
    }

    std::string decodeDecimal(std::string buf, struct Decimal *value, bool invert) {
        // Handle the simplistic cases first.
        switch (buf[0]) {
            case decimalNaN: {
                value->_Form = NaN;
                return buf.substr(1);
            }
            case decimalNaNDesc: {
                value->_Form = NaN;
                return buf.substr(1);
            }
            case decimalInfinity: {
                value->_Form = Infinite;
                value->Negative = invert;
                return buf.substr(1);
            }
            case decimalNegativeInfinity: {
                value->_Form = Infinite;
                value->Negative = !invert;
                return buf.substr(1);
            }
            case decimalZero: {
                return buf.substr(1);
            }
        }

        // tmp = tmp[len(tmp):cap(tmp)]
        //tmp.resize(tmp.capacity()-tmp.length());
        std::string tmp = "";
        if (buf[0] == decimalNegLarge) {
            // Negative large.
            int e = 0;
            std::string m = "";
            std::string r = "";
            std::string tmp2 = "";
            decodeLargeNumber(true, buf, tmp, &e, &m, &r, &tmp2);
            makeDecimalFromMandE(!invert, e, m, value);
            return r;
        } else if (buf[0] > decimalNegLarge && buf[0] <= decimalNegMedium) {
            int e = 0;
            std::string m = "";
            std::string r = "";
            std::string tmp2 = "";
            // Negative medium.
            decodeMediumNumber(true, buf, tmp, &e, &m, &r, &tmp2);
            makeDecimalFromMandE(!invert, e, m, value);
            return r;
        } else if (buf[0] == decimalNegSmall) {
            int e = 0;
            std::string m = "";
            std::string r = "";
            std::string tmp2 = "";
            // Negative small.
            decodeSmallNumber(true, buf, tmp, &e, &m, &r, &tmp2);
            makeDecimalFromMandE(!invert, e, m, value);
            return r;
        } else if (buf[0] == decimalPosLarge) {
            int e = 0;
            std::string m = "";
            std::string r = "";
            std::string tmp2 = "";
            // Positive large.
            decodeLargeNumber(false, buf, tmp, &e, &m, &r, &tmp2);
            makeDecimalFromMandE(invert, e, m, value);
            return r;
        } else if (buf[0] >= decimalPosMedium && buf[0] < decimalPosLarge) {
            int e = 0;
            std::string m = "";
            std::string r = "";
            std::string tmp2 = "";
            // Positive medium.
            decodeMediumNumber(false, buf, tmp, &e, &m, &r, &tmp2);
            makeDecimalFromMandE(invert, e, m, value);
            return r;
        } else if (buf[0] == decimalPosSmall) {
            int e = 0;
            std::string m = "";
            std::string r = "";
            std::string tmp2 = "";
            // Positive small.
            decodeSmallNumber(false, buf, tmp, &e, &m, &r, &tmp2);
            makeDecimalFromMandE(invert, e, m, value);
            return r;
        } else {
            std::cerr << "\"error: unknown prefix of the encoded byte" << std::endl;
            exit(1);
        }
    }

    void decodeTime(rocksdb::Slice &buf, struct Time *value,bool invert) {
        if (PeekType(buf) != timeMarker) {
            std::cerr << "error :did not find marker" << std::endl;
            exit(1);
        }
        buf.remove_prefix(1);
        int64_t sec;
        int64_t nsec;
        DecodeVarintAscending(buf, &sec);
        DecodeVarintAscending(buf, &nsec);
        if(invert){
            value->t_timespec.tv_sec = sec;
            value->t_timespec.tv_nsec = nsec;
        }else{
            value->t_timespec.tv_sec = ~(sec);
            value->t_timespec.tv_nsec = ~(nsec);
        }
        return ;
    }



    //===============decode Int key Asc============================
    void DecodeVarintAscending(rocksdb::Slice &buf,int64_t* value){
        if(!DecodeVarintASC64(buf,value)){
            std::cerr << "DecodeVarintAscending error " << std::endl;
            exit(1);
        }
    }

    //===============Decode String key ASC=========================
    std::string DecodeUnsafeStringAscending(rocksdb::Slice &buf, std::string *str) {
        std::string b = std::string(buf.data(),buf.size());
        struct escapes AscendingEscapes{escape, escapedTerm, escaped00, escapedFF, bytesMarker};
        DecodeBytesInternal(b, str, AscendingEscapes, true);
        return b;
    }

    //=================Decode Float key ASC============================
    void DecodeFloatAscending(rocksdb::Slice &buf, double *data) {
        if (PeekType(buf) != floatNaN && PeekType(buf) != floatNaNDesc
            && PeekType(buf) != floatNeg && PeekType(buf) != floatPos
            && PeekType(buf) != floatZero) {
            std::cerr << "DecodeFloatAscending error:did not find marker" << std::endl;
            exit(1);
        }
        switch (buf[0]) {
            case floatNaN:
            case floatNaNDesc: {
                uint64_t val = uvnan;
                std::string bit = ""+TransformUintToBit(val);
                *data = HexToDouble(bit);
                buf.remove_prefix(1);
                return ;
            }
            case floatNeg: {
                uint64_t val = 0;
                buf.remove_prefix(1);
                DecodeUint64Ascending(buf, &val);
                val = ~(val);
                std::string bit = ""+TransformUintToBit(val);
                *data = HexToDouble(bit);
                return ;
            }
            case floatZero: {
                *data = 0;
                buf.remove_prefix(1);
                return ;
            }
            case floatPos: {
                uint64_t val = 0;
                buf.remove_prefix(1);
                DecodeUint64Ascending(buf, &val);
                std::string bit = ""+TransformUintToBit(val);
                *data = HexToDouble(bit);
                return ;
            }
            default:
                std::cerr << "DecodeFloatAscending error: unknown prefix of the encoded byte slice" << std::endl;
                exit(1);
        }
    }

    //===============Decode Decimal key ASC=========================
    std::string DecodeDecimalAscending(rocksdb::Slice &buf, struct Decimal *value) {
        std::string b =std::string(buf.data(),buf.size());
        return decodeDecimal(b, value, false);
    }

    //===============Decode Time key ASC=========================
    void DecodeTimeAscending(rocksdb::Slice &buf, struct Time *value) {
        decodeTime(buf, value,true);
        return ;
    }

    //===============Decode Int key Desc============================
    void DecodeVarintDescending(rocksdb::Slice &buf,int64_t* value) {
        DecodeVarintAscending(buf, value);
        *value = ~(*value);
        return ;
    }

    //===============Decode String key Desc=========================
    std::string DecodeUnsafeStringDescending(rocksdb::Slice &buf, std::string *str) {
        std::string b = std::string(buf.data(),buf.size());
        DecodeBytesDescending(b, str);
        return b;
    }


    //=================Decode Float key Desc============================
    void DecodeFloatDescending(rocksdb::Slice &buf, double *data) {
        DecodeFloatAscending(buf, data);
        *data = (-(*data));
        return ;
    }

    //===============Decode Decimal key Desc=========================
    std::string DecodeDecimalDescending(rocksdb::Slice &buf, struct Decimal *value) {
        std::string b =std::string(buf.data(),buf.size());
        return decodeDecimal(b, value, true);
    }

    //===============Decode Time key Desc=========================
    void DecodeTimeDescending(rocksdb::Slice &buf,  struct Time *value) {
        decodeTime(buf, value, false);
        return ;
    }



    // decode value
    bool DecodeNonsortingUvarint(rocksdb::Slice &buf, rocksdb::Slice *remaining, int *length, uint64_t *value) {
        // TODO(dan): Handle overflow.
        *value = 0;
        unsigned char *bb = (unsigned char *) buf.data();
        for (size_t i = 0; i < buf.size(); i++) {
            *value += uint64_t(bb[i] & 0x7f);
            if (bb[i] < 0x80) {
                *remaining = slice(buf, i + 1);
                *length = i + 1;
                return true;
            }
            *value <<= 7;
        }
        return true;
    }

    bool DecodeValueTag(rocksdb::Slice &buf, int *typeOffset, int *dataOffset, uint32_t *colID, VType *typ) {
        assert(buf.size() > 0);
        int n=0;
        uint64_t tag;
        rocksdb::Slice remaining;
        if (!DecodeNonsortingUvarint(buf, &remaining, &n, &tag)) {
            *typ = Unknown;
            return false;
        }
        //rocksmt::Slice b = remaining; //能修改吗？
        *colID = uint32_t(tag >> 4);
        *typ = VType(tag & 0xf);
        *typeOffset = n - 1;
        *dataOffset = n;
        if (*typ == SentinelType) {
            rocksdb::Slice b;
            if (!DecodeNonsortingUvarint(remaining, &b, &n, &tag)) {
                *typ = Unknown;
                return false;
            }
            *typ = VType(tag);
            *dataOffset += n;
        }
        return true;
    }

    int Binary_Uvarint(rocksdb::Slice &buf, uint64_t *value) {
        // go标准库binary.Uvarint(buf)方法
        uint64_t x=0;
        uint s=0;
        for (size_t i = 0; i < buf.size(); i++) {
            unsigned char b = buf[i];
            if (b < 0x80) {
                if (i > 9 || (i == 9 && b > 1)) {
                    *value = 0;
                    return -(i + 1); // overflow
                }
                *value = x | uint64_t(b) << s;
                return i + 1;
            }
            x |= uint64_t(b & 0x7f) << s;
            s += 7;
        }
        return 0;
    }

    int Binary_Varint(rocksdb::Slice &buf, int64_t *value) {
        uint64_t ux=0;
        int n = Binary_Uvarint(buf, &ux); // ok to continue in presence of error
        int64_t x = int64_t(ux >> 1);
        if ((ux & 1) != 0) {
            x = ~x;//GO里是^,按位取反运算符
        }
        *value = x;
        return n;
    }

    std::string DecodeNonsortingStdlibUVarint(rocksdb::Slice &b, rocksdb::Slice *remaining, int *length, uint64_t *value) {
        *length = Binary_Uvarint(b, value);
        if (*length <= 0) {
            return Errorf("DecodeNonsortingStdlibUVarint int64 varint decoding failed: %d", length);
        }
        *remaining = slice(b, *length);
        return "";
    }

    std::string DecodeNonsortingStdlibVarint(rocksdb::Slice &b, rocksdb::Slice *remaining, int *length, int64_t *value) {
        *length = Binary_Varint(b, value);
        if (*length <= 0) {
            return Errorf("DecodeNonsortingStdlibVarint int64 varint decoding failed: %d", length);
        }
        *remaining = slice(b, *length);
        return "";
    }

    bool getMultiNonsortingVarintLen(rocksdb::Slice &buf,int num,int *length){
        size_t p = 0;
        rocksdb::Slice remaining;
        int n=0;
        int64_t value;
        for(int i=0;(i<num&&p<buf.size());i++){
            rocksdb::Slice slice1=slice(buf, p);
            DecodeNonsortingStdlibVarint(slice1, &remaining, &n, &value);
            p += n;
        }
        *length = p;
        return true;
    }

    std::string PeekValueLengthWithOffsetsAndType(rocksdb::Slice &buf, int dataOffset, VType typ, int *length) {
        rocksdb::Slice b = slice(buf, dataOffset);
        rocksdb::Slice remaining;
        int64_t value;
        uint64_t u_value;
        int n;
        switch (typ) {
            case Null:
                *length = dataOffset;
                break;
            case True:
                *length = dataOffset;
                break;
            case False:
                *length = dataOffset;
                break;
            case Int: {
                DecodeNonsortingStdlibVarint(b, &remaining, &n, &value);
                *length = dataOffset + n;
                break;
            }
            case Float: {
                *length= dataOffset + floatValueEncodedLength;
                break;
            }
            case Bytes:case Array:case JSON:{
                DecodeNonsortingUvarint(b, &remaining, &n, &u_value);
                *length = dataOffset + n + int(u_value);
                break;
            }
            case Decimal:{
                DecodeNonsortingStdlibUVarint(b, &remaining, &n, &u_value);
                *length = dataOffset + n + int(u_value);
                break;
            }
            case Time:{
                if(getMultiNonsortingVarintLen(b,3,&n)){
                    *length = dataOffset + n;
                }
                break;
            }
            case UUID:{
                *length = dataOffset + UuidValueEncodeLength;
                break;
            }
            default:
                return Errorf("unknown type %s", typ);
        }
        return "";
    }

    std::string DecodeValue(rocksdb::Slice &b, int typeOffset, int dataOffset, VType typ,
                            rocksdb::Slice *remaining, rocksdb::Slice *value) {
        int encLen;
        auto msg = PeekValueLengthWithOffsetsAndType(b, dataOffset, typ, &encLen);
        if (msg != "") {
            return msg;
        }

        //返回带类型的[]byte编码
        *value = slice(b, typeOffset, encLen);
        *remaining = slice(b, encLen);
        //ed := b[typeOffset:encLen]
        //return b[encLen:], ed, nil
        return "";
    }


    //====================解码多列tag,类型没有解===============================
    std::string DecodeSumRow(rocksdb::Slice &buf, uint32_t col_count, std::vector<rocksdb::Slice> *values) {
        if (col_count < 1) {
            col_count = DEFAULT_COL_ID;
        }
        buf = slice(buf, headerSize);//去除前面五位的校验位

        *values = std::vector<rocksdb::Slice>(col_count);
        uint32_t col_id = 0;
        while (buf.size() > 0) {
            int typeOffset;
            int dataOffset;
            uint32_t colIDDiff;
            VType typ;//此列的类型
            if (!DecodeValueTag(buf, &typeOffset, &dataOffset, &colIDDiff, &typ)) {
                break;
            }
            col_id += colIDDiff;
            rocksdb::Slice remaining, s;
            auto err = DecodeValue(buf, typeOffset, dataOffset, typ, &remaining, &s);
            if (err != "") {
                return err;
            }
            buf = remaining;// TODO b是引用类型，会修改调用者的值？
            if (col_id >= MAX_COL_ID) {
                return Errorf("parsed ColID[%d] larger than MAX_COL_ID[%d]", col_id, MAX_COL_ID);
            }
            if (col_id >= values->size()) {
                //如果列id超过value大小，则需要扩充
                //扩充数组：当前列ID × 2倍，避免频繁copy
                std::vector<rocksdb::Slice> new_b(col_id * 1.5);
                for (size_t j = 0; j < values->size(); j++) {
                    new_b[j] = (*values)[j];
                }
                *values = new_b;
            }
            (*values)[col_id] = s;
        }
        return "";
    }

    //====================根据require id 解码减少调用===============================
    std::string DecodeSumRowReq(rocksdb::Slice &buf, std::vector<rocksdb::Slice> *values,std::vector<int> require) {
        if(require.size() == 0){
            return "";
        }
        buf = slice(buf, headerSize);//去除前面五位的校验位

        uint32_t col_id = 0;
        int max =  *max_element(require.begin(),require.end());//获取require中的最大列id
        *values = std::vector<rocksdb::Slice>(max+1);
        while (buf.size() > 0) {
            int typeOffset;
            int dataOffset;
            uint32_t colIDDiff;
            VType typ;//此列的类型
            if (!DecodeValueTag(buf, &typeOffset, &dataOffset, &colIDDiff, &typ)) {
                break;
            }
            col_id += colIDDiff;
            if(col_id > max){
                return "";
            }
            rocksdb::Slice remaining, s;
            auto err = DecodeValue(buf, typeOffset, dataOffset, typ, &remaining, &s);
            if (err != "") {
                return err;
            }
            buf = remaining;// TODO b是引用类型，会修改调用者的值？
            if (col_id >= MAX_COL_ID) {
                return Errorf("parsed ColID[%d] larger than MAX_COL_ID[%d]", col_id, MAX_COL_ID);
            }
            if (col_id >= values->size()) {
                //如果列id超过value大小，则需要扩充
                //扩充数组：当前列ID × 2倍，避免频繁copy
                std::vector<rocksdb::Slice> new_b(col_id * 1.5);
                for (size_t j = 0; j < values->size(); j++) {
                    new_b[j] = (*values)[j];
                }
                *values = new_b;
            }
            (*values)[col_id] = s;
        }
        return "";
    }


    //把slice对应列的类型截掉
    rocksdb::Slice  decodeValueType(rocksdb::Slice b, VType expected){
        rocksdb::Slice remaining;
        int typeOffset, dataOffset;
        uint32_t colID;
        VType typ;
        if (!DecodeValueTag(b, &typeOffset, &dataOffset, &colID, &typ)) {
            return "DecodeValueTag error.";
        }
        remaining = slice(b, dataOffset);
        if (typ != expected) {
            //TODO 错误信息未在上层捕获
            throw Errorf("value type is not %s : %s", ToString(expected).c_str(), ToString(typ).c_str());
        }
        return remaining;
    }

    void DecodeNonsortingStdlibVarint(rocksdb::Slice &buf, int64_t *value) {
        int n = Binary_Varint(buf, value);
        buf.remove_prefix(n);
    }

    uint64_t binary_BigEndian_Uint64(rocksdb::Slice b) {
        uint64_t i = uint64_t(b[7]) & 0xFF;
        i |= (uint64_t(b[6]) & 0xFF) << 8;
        i |= (uint64_t(b[5]) & 0xFF) << 16;
        i |= (uint64_t(b[4]) & 0xFF) << 24;
        i |= (uint64_t(b[3]) & 0xFF) << 32;
        i |= (uint64_t(b[2]) & 0xFF) << 40;
        i |= (uint64_t(b[1]) & 0xFF) << 48;
        i |= (uint64_t(b[0]) & 0xFF) << 56;
        return i;
    }

    void DecodeUint64Ascending(rocksdb::Slice &buf, uint64_t *value) {
        if (buf.size() < 8) {
            std::cerr << "error:float长度太短" << std::endl;
        }
        *value = binary_BigEndian_Uint64(buf);
        buf.remove_prefix(8);
    }

    int stringToDouble(std::string temp)//二进制串到double（整数）
    {
        int res = 0;
        for (int i = 0; i < int(temp.length()); i++) {
            res = res * 2 + (temp[i] - '0');
        }
        return res;
    }

    //二进制串到double（小数）
    double BenToDex(std::string temp){
        int m = temp.length();
        double res = 0;
        for (int i = 0; i < m; i++) {
            res = res + (temp[i] - '0') * pow(2.0, -i - 1);
        }
        return res;
    }

    double HexToDouble(std::string S_Bin) {
        int sign = 0;//符号位（1位）
        if (S_Bin.at(0) == '1') {
            sign = 1;
        }
        std::string exponent = "";//获取阶码字符串（11位）
        for (int i = 1; i < 12; i++) {
            if (S_Bin.at(i) == '1')
                exponent = exponent + '1';
            else
                exponent = exponent + '0';
        }

        int exponent_double = 0;//阶码
        exponent_double = stringToDouble(exponent);
        exponent_double = exponent_double - 1023;//减去偏移值
        std::string mantissa_temp = "";//获取尾数字符串（52位）
        for (int i = 12; i < 64; i++) {
            if (S_Bin.at(i) == '1')
                mantissa_temp = mantissa_temp + '1';
            else
                mantissa_temp = mantissa_temp + '0';
        }

        double mantissa = 0;//尾数
        mantissa = BenToDex(mantissa_temp);
        mantissa = mantissa + 1.0;

        //双精度公式：
        // F=1.M(二进制)
        // V=(-1)^s*2^(E-1023)*F
        double res = 0;
        double a, c;
        a = pow((-1.0), sign);
        c = pow(2.0, exponent_double);
        res = a * c * mantissa;

        return res;
    }

    //============================decimal====================================
    bool DecodeUvarintAscending_Decimal(rocksdb::Slice &buf, uint64_t *value) {
        if (buf.size() == 0) {
            return false;
        }
        int length = uint8_t((buf)[0]) - kIntZero;
        buf.remove_prefix(1);  // skip length byte
        if (length <= kIntSmall) {
            *value = uint64_t(length);
            return true;
        }
        length -= kIntSmall;
        if (length < 0 || length > 8) {
            return false;
        } else if ((int) buf.size() < length) {
            return false;
        }
        *value = 0;
        for (int i = 0; i < length; i++) {
            *value = (*value << 8) | uint8_t((buf)[i]);
        }
        buf.remove_prefix(length);
        return true;
    }

    uint64_t bigEndianWord(rocksdb::Slice buf) {
        return binary_BigEndian_Uint64(buf);
    }

    void setBytes(rocksdb::Slice buf, struct Decimal *dec) {
        uint64_t abs1[(buf.size() + _S - 1) / _S];
        int i = buf.size();
        for (int k = 0; i >= _S; k++) {
            buf.remove_prefix(i - _S);
            abs1[k] = bigEndianWord(buf);
            i -= _S;
        }
        if (i > 0) {
            uintmax_t d=0;                      //go是否使用了该初始值
            for (uint s = uint(0); i > 0; s += 8) {
                d |= (uint64_t(buf[i - 1])& 0xFF) << s;
                i--;
            }
            abs1[sizeof(abs1)/8 - 1] = d;
        }
        //z.norm() :
        int j = sizeof(abs1)/8;
        while (j > 0 && abs1[j-1] == 0){
            j--;
        }
        for(int q=0;q<j;q++){
            dec->Coeff.abs.push_back(abs1[q]);
        }
    }

    void SetBytes(rocksdb::Slice buf, struct Decimal *dec) {
        setBytes(buf, dec);
        dec->Coeff.neg = false;
    }

    void decodeNonsortingDecimalValue(struct Decimal *dec, bool negExp, rocksdb::Slice buf) {
        // Decode the exponent.
        uint64_t e=0;
        DecodeUvarintAscending_Decimal(buf, &e);

        if (negExp) {
            e = -e;
        }
        SetBytes(buf, dec);

        int  nDigits = 0;
        // Set the decimal's scale.
        if(dec->Coeff.abs.size()) {
            uint count = 0;
            int64_t n = dec->Coeff.abs[0];
            while (n != 0) {
                n = n / 10;
                count++;
            }
            nDigits = count;
        } else{
            nDigits = 1;
        }
        int  exp = int(e)-nDigits;
        dec->Exponent = int32_t (exp);
    }

    void decodeNonsortingDecimalValueWithoutExp(struct Decimal *dec,rocksdb::Slice  buf) {
        SetBytes(buf,dec);
        // Set the decimal's scale.
        int64_t n=dec->Coeff.abs[0];
        uint count=0;
        while(n!=0)
        {
            n=n/10;
            count++;
        }
        int nDigits =  count;
        dec->Exponent = -int32_t(nDigits);
    }

    void DecodeIntoNonsortingDecimal(rocksdb::Slice &buf, struct Decimal *dec) {
        switch (buf[0]) {
            case decimalNaN:
                dec->_Form = NaN;
                return ;
            case decimalNegativeInfinity:
                dec->_Form = Infinite;
                dec->Negative = true;
                return ;
            case decimalInfinity:
                dec->_Form = Infinite;
                return ;
            case decimalZero:
                return ;
        }

        dec->_Form = Finite;
        switch (buf[0]) {
            case decimalNegLarge:
                buf.remove_prefix(1);
                decodeNonsortingDecimalValue(dec, false,buf );
                dec->Negative = true;
                break;
            case decimalNegMedium:
                buf.remove_prefix(1);
                decodeNonsortingDecimalValueWithoutExp(dec, buf);
                dec->Negative = true;
                break;
            case decimalNegSmall:
                buf.remove_prefix(1);
                decodeNonsortingDecimalValue(dec, true, buf);
                dec->Negative = true;
                break;
            case decimalPosSmall:
                buf.remove_prefix(1);
                decodeNonsortingDecimalValue(dec, true, buf);
                break;
            case decimalPosMedium:
                buf.remove_prefix(1);
                decodeNonsortingDecimalValueWithoutExp(dec, buf);
                break;
            case decimalPosLarge:
                buf.remove_prefix(1);
                decodeNonsortingDecimalValue(dec, false, buf);
                break;
            default:
                std::cerr << "erorr:unknown decimal prefix of the encoded byte slice" << std::endl;
                exit(1);
        }
    }

    void DecodeNonsortingStdlibUvarint_Decimal(rocksdb::Slice &buf, int *length, uint64_t *value) {
        *length = Binary_Uvarint(buf, value);
        if (*length <= 0) {
            std::cerr << "buff too small";
            exit(1);
        }
        buf.remove_prefix(*length);
    }

    void DecodeNonsortingDecimal(rocksdb::Slice buf, struct Decimal *dec) {
        DecodeIntoNonsortingDecimal(buf,  dec);
    }

    //========================decode UUID=======================================
    void DecodeUntaggedUUIDValue(rocksdb::Slice buf, Uuid *data) {
        buf.remove_suffix(buf.size()-UuidValueEncodeLength);
        if (buf.size() != Uuid_Size) {
            perror("UUID DecodeUntaggedValue length error");
            return ;
        }
        for (int i = 0; i < Uuid_Size; i++) {
            *(data++) = buf[i];
        }
        return ;
    }

    //==========================decode Time======================================
    void DecodeUntaggedTimeValue(rocksdb::Slice buf, struct Time *t) {
        int64_t t_unix;
        int64_t t_Nanosecond;
        int64_t t_abbv;
        //std::string remaining = "";
        DecodeNonsortingStdlibVarint(buf, &t_unix);
        DecodeNonsortingStdlibVarint(buf, &t_Nanosecond);
        if (buf.size() != 0){
            DecodeNonsortingStdlibVarint(buf, &t_abbv);
            t->t_abbv=t_abbv;
        }
        t->t_timespec.tv_sec = t_unix;
        t->t_timespec.tv_nsec = t_Nanosecond;
        return ;
    }

    //==========================decode Decimal===================================
    void DecodeUntaggedDecimalValue(rocksdb::Slice buf, struct Decimal *data) {
        uint64_t value = 0;
        int length = 0;
        DecodeNonsortingStdlibUvarint_Decimal(buf, &length, &value);
        int sum =buf.size();
        buf.remove_suffix(sum-value);
        DecodeNonsortingDecimal(buf, data);
    }

    //=======================decode float============================
    void DecodeUntaggedFloatValue(rocksdb::Slice buf, double *res) {
        if (buf.size() < 8) {
            std::cerr << "error:float长度太短" << std::endl;
            exit(1);
        }
        uint64_t val;
        DecodeUint64Ascending(buf, &val);
        if(val == 0 ){
            *res = 0;
            return ;
        }
        std::string bit = ""+TransformUintToBit(val);
        *res = HexToDouble(bit);
        return ;
    }

    //========================Bytes decode======================================
    void DecodeUntaggedBytesValue(rocksdb::Slice b, std::string *data) {
        uint64_t value = 0;
        int length = 0;
        rocksdb::Slice remaining;
        if(!DecodeNonsortingUvarint(b, &remaining, &length, &value)){
            std::cerr <<"DecodeUntaggedBytesValue error"<<std::endl;
            exit(1);
        }
        if(remaining.size()>value){
            remaining.remove_suffix(remaining.size()-value-1);
        }
        *data = std::string(remaining.data(),remaining.size());
    }

    void DecodeUntaggedIntValue(rocksdb::Slice buf,int64_t *value) {
        DecodeNonsortingStdlibVarint(buf, value);
    }

    void DecodeUntaggedBoolValue(rocksdb::Slice buf, bool *res) {
        int typeoffset = 0;
        int dataoffset = 0;
        uint32_t colID = 0;
        VType typ ;
        DecodeValueTag(buf, &typeoffset, &dataoffset, &colID, &typ);
        switch (typ) {
            case True:
                *res = true;
                return ;
            case False:
                *res = false;
                return ;
            default:
                *res = false;
                return ;
        }
    }

    //根据类型解码value的值 bool
    void DecodeBoolValue(rocksdb::Slice buf,bool *data){
        DecodeUntaggedBoolValue(buf,data);
    }

    void DecodeIntValue(rocksdb::Slice buf,int64_t *data) {
        rocksdb::Slice test_lex = decodeValueType(buf,znbase::Int);
        DecodeUntaggedIntValue(test_lex, data);
    }

    void DecodeBytesValue(rocksdb::Slice buf,std::string *data) {
        rocksdb::Slice test_lex = decodeValueType(buf,znbase::Bytes);
        DecodeUntaggedBytesValue(test_lex, data);
    }

    void DecodeFloatValue(rocksdb::Slice buf,double *data) {
        rocksdb::Slice test_lex = decodeValueType(buf,znbase::Float);
        DecodeUntaggedFloatValue(test_lex, data);
    }

    void DecodeDecimalValue(rocksdb::Slice buf,struct Decimal *data){
        rocksdb::Slice test_lex = decodeValueType(buf,znbase::Decimal);
        DecodeUntaggedDecimalValue(test_lex, data);
    }

    void DecodeTimeValue(rocksdb::Slice buf,struct Time *data) {
        rocksdb::Slice test_lex = decodeValueType(buf,znbase::Time);
        DecodeUntaggedTimeValue(test_lex, data);
    }

    void DecodeUUIDValue(rocksdb::Slice buf, Uuid *data){
        rocksdb::Slice test_lex = decodeValueType(buf,znbase::UUID);
        DecodeUntaggedUUIDValue(test_lex,data);
    }

    //char转16进制
    std::string typechange(int a)
    {
        char c[9];
        char d[9];
        int i=0;
        while(a!=0)
        {
            c[i]=a&(0xf);
            if(c[i]>9)
                c[i]+='a'-10;
            else
                c[i]+='0';

            a=a>>4;
            i++;
            if(i==9)
                break;
        }
        c[i]='\0';
        i--;
        int k=0;
        int t=i;
        for(;k<=t;k++)
        {
            d[k]=c[i];
            i--;
        }
        d[k]='\0';
        std::string b1(d);
        return b1;
    }

    //16进制转成string输出
    std::string convUUIDToString(unsigned char* source){
        std::string res;
        for(int k=0;k<16;k++)
        {
            if(k == 4 || k == 6 || k == 8 || k == 10){
                res += "-";
            }
            int t = *(source++);
            if(t>=16)
                res+=typechange(t);
            else if(t==0)
            {
                res+="00"+typechange(t);
            }else
                res+="0"+typechange(t);
        }
        return res;
    }

    //string类型的日期转成天数
    int convertDateToDay(std::string str){
        tm SysTime = {0};
        tm InTime = {0};
        SysTime.tm_sec = 0;
        SysTime.tm_min = 0;
        SysTime.tm_hour = 0;
        SysTime.tm_mday = 1;
        SysTime.tm_mon = 0;
        SysTime.tm_year = 1970;

        //2020-01-20
        std::string s1 = str.substr(0,4);//年
        std::string s2 = str.substr(5,2);//月
        std::string s3 = str.substr(8,2);//日

        InTime.tm_mday = stoi(s3);
        InTime.tm_mon = stoi(s2)-1;
        InTime.tm_year = stoi(s1);

        time_t Res1,Res2;
        Res1 = mktime(&SysTime);
        Res2 = mktime(&InTime);
        int DayNum = (Res2 - Res1)/(24*60*60);
        return DayNum;
    }


    //string类型的DateTime转成天数
    int64_t convertDateTimeToDay(std::string str){
        tm SysTime = {0};
        tm InTime = {0};
        SysTime.tm_sec = 0;
        SysTime.tm_min = 0;
        SysTime.tm_hour = 0;
        SysTime.tm_mday = 1;
        SysTime.tm_mon = 0;
        SysTime.tm_year = 1970;

        //2020-01-20 10:02:12
        std::string s1 = str.substr(0,4);//年
        std::string s2 = str.substr(5,2);//月
        std::string s3 = str.substr(8,2);//日

        std::string s4 = str.substr(11,2);//时
        std::string s5 = str.substr(14,2);//分
        std::string s6 = str.substr(17,2);//秒

        InTime.tm_sec = stoi(s6);
        InTime.tm_min = stoi(s5);
        InTime.tm_hour = stoi(s4);

        InTime.tm_mday = stoi(s3);
        InTime.tm_mon = stoi(s2)-1;
        InTime.tm_year = stoi(s1);


        time_t Res1,Res2;
        Res1 = mktime(&SysTime);
        Res2 = mktime(&InTime);
        int64_t SecNum = Res2 - Res1;
        return SecNum;
    }

    bool consumePrefix(std::string &str){
        if(str.size() >= 1 && str.substr(0,1) == "-"){
            str = str.substr(1,str.size()-1);
            return true;
        }else  if(str.size() >= 1 && str.substr(0,1) == "+"){
            str = str.substr(1,str.size()-1);
            return false;
        } else{
            return false;
        }
    }

    //string字符串转成decimal结构体
    void PharseDecimal(std::string input_str,struct Decimal &dd){
        dd.Negative = consumePrefix(input_str);
        dd.Exponent = 0;
        dd._Form = NaN;
        dd.Coeff.neg = false;
        std::string::size_type idx = input_str.find(".");
        if(idx != std::string::npos){
            int pos = input_str.find(".");
            int exp = input_str.size() - pos - 1;
            dd.Exponent = -exp;
            input_str.erase(input_str.find("."),1);
        }
        uint64_t val = stoull(input_str);
        dd.Coeff.abs.clear();
        if(val != 0 && dd.Coeff.abs.size() == 0){
            dd.Coeff.abs.push_back(val);
        }
        dd._Form = Finite;
        if(dd._Form == Finite && dd.IsZero() && dd.Negative){
            dd.Negative = false;
        }
    }

    int countSum(uint64_t val){
        int k=0;
        while(val)
        {
            val=val/10;
            k++;
        }
        return k;
    }

    //decimal大小之间的比较 -1：代表col_val 小于fil_val 1：代表col_val大于fil_val 0代表相等
    int CompareDecimals(struct Decimal col_val,struct Decimal fil_val){
        //正负的比较
        int ds = col_val.Sign();
        int xs = fil_val.Sign();
        if( ds <  xs){
            return -1;
        }else if(ds > xs){
            return 1;
        }else if(ds == 0 && xs == 0){
            return 0;
        }

        int gt = 1;
        int lt = 1;
        if (ds == -1){
            gt = -1;
            lt = 1;
        }

        if(col_val._Form == Infinite){
            if(fil_val._Form == Infinite){
                return 0;
            }
            return gt;
        }else if(fil_val._Form == Infinite){
            return lt;
        }

        //如果小数位相等
        int cmp = 0;
        if(col_val.Exponent == fil_val.Exponent){
            if(col_val.Coeff.abs.size() > 0 && fil_val.Coeff.abs.size() > 0){
                if(col_val.Coeff.abs[0] < fil_val.Coeff.abs[0]){
                    cmp = -1;
                }else if(col_val.Coeff.abs[0] > fil_val.Coeff.abs[0]){
                    cmp = 1;
                }
            }
            if(col_val.Coeff.neg){
                cmp = -cmp;
            }
            if(ds < 0){
                cmp = -cmp;
            }
            return cmp;
        }

        //如果小数位不相等
        int dn = countSum(col_val.Coeff.abs[0])+col_val.Exponent;
        int xn = countSum(fil_val.Coeff.abs[0])+fil_val.Exponent;
        if(dn < xn){
            return lt;
        } else if(dn > xn){
            return gt;
        }
        //补齐位数少的正数再进行比较
        int res = 0;
        if(col_val.Exponent < fil_val.Exponent){
            int val1 = pow(10,fil_val.Exponent-col_val.Exponent);
            fil_val.Coeff.abs[0] = fil_val.Coeff.abs[0] * val1;
            if(col_val.Coeff.abs.size() > 0 && fil_val.Coeff.abs.size() > 0){
                if(col_val.Coeff.abs[0] < fil_val.Coeff.abs[0]){
                    res = -1;
                }else if(col_val.Coeff.abs[0] > fil_val.Coeff.abs[0]){
                    res = 1;
                }
            }
        }else{
            int val1 = pow(10,col_val.Exponent-fil_val.Exponent);
            col_val.Coeff.abs[0] = col_val.Coeff.abs[0] * val1;
            if(col_val.Coeff.abs.size() > 0 && fil_val.Coeff.abs.size() > 0){
                if(col_val.Coeff.abs[0] < fil_val.Coeff.abs[0]){
                    res = -1;
                }else if(col_val.Coeff.abs[0] > fil_val.Coeff.abs[0]){
                    res = 1;
                }
            }
        }
        if(ds < 0){
            res = -res;
        }
        return res;

    }


    void EncodeUint32(std::string* buf, uint32_t v) {
        const uint8_t tmp[sizeof(v)] = {
                uint8_t(v >> 24),
                uint8_t(v >> 16),
                uint8_t(v >> 8),
                uint8_t(v),
        };
        buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
    }

    void EncodeUint64(std::string* buf, uint64_t v) {
        const uint8_t tmp[sizeof(v)] = {
                uint8_t(v >> 56), uint8_t(v >> 48), uint8_t(v >> 40), uint8_t(v >> 32),
                uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8),  uint8_t(v),
        };
        buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
    }

    void EncodeUvarint64(std::string* buf, uint64_t v) {
        if (v <= kIntSmall) {
            const uint8_t tmp[1] = {
                    uint8_t(v + kIntZero),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        } else if (v <= 0xff) {
            const uint8_t tmp[2] = {
                    uint8_t(kIntMax - 7),
                    uint8_t(v),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        } else if (v <= 0xffff) {
            const uint8_t tmp[3] = {
                    uint8_t(kIntMax - 6),
                    uint8_t(v >> 8),
                    uint8_t(v),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        } else if (v <= 0xffffff) {
            const uint8_t tmp[4] = {
                    uint8_t(kIntMax - 5),
                    uint8_t(v >> 16),
                    uint8_t(v >> 8),
                    uint8_t(v),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        } else if (v <= 0xffffffff) {
            const uint8_t tmp[5] = {
                    uint8_t(kIntMax - 4), uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8), uint8_t(v),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        } else if (v <= 0xffffffffff) {
            const uint8_t tmp[6] = {
                    uint8_t(kIntMax - 3), uint8_t(v >> 32), uint8_t(v >> 24),
                    uint8_t(v >> 16),     uint8_t(v >> 8),  uint8_t(v),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        } else if (v <= 0xffffffffffff) {
            const uint8_t tmp[7] = {
                    uint8_t(kIntMax - 2), uint8_t(v >> 40), uint8_t(v >> 32), uint8_t(v >> 24),
                    uint8_t(v >> 16),     uint8_t(v >> 8),  uint8_t(v),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        } else if (v <= 0xffffffffffffff) {
            const uint8_t tmp[8] = {
                    uint8_t(kIntMax - 1), uint8_t(v >> 48), uint8_t(v >> 40), uint8_t(v >> 32),
                    uint8_t(v >> 24),     uint8_t(v >> 16), uint8_t(v >> 8),  uint8_t(v),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        } else {
            const uint8_t tmp[9] = {
                    uint8_t(kIntMax), uint8_t(v >> 56), uint8_t(v >> 48), uint8_t(v >> 40), uint8_t(v >> 32),
                    uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8),  uint8_t(v),
            };
            buf->append(reinterpret_cast<const char*>(tmp), sizeof(tmp));
        }
    }

    bool DecodeUint32(rocksdb::Slice* buf, uint32_t* value) {
        const int N = sizeof(*value);
        if (buf->size() < N) {
            return false;
        }
        const uint8_t* b = reinterpret_cast<const uint8_t*>(buf->data());
        *value = (uint32_t(b[0]) << 24) | (uint32_t(b[1]) << 16) | (uint32_t(b[2]) << 8) | uint32_t(b[3]);
        buf->remove_prefix(N);
        return true;
    }

    bool DecodeUint64(rocksdb::Slice* buf, uint64_t* value) {
        const int N = sizeof(*value);
        if (buf->size() < N) {
            return false;
        }
        const uint8_t* b = reinterpret_cast<const uint8_t*>(buf->data());
        *value = (uint64_t(b[0]) << 56) | (uint64_t(b[1]) << 48) | (uint64_t(b[2]) << 40) |
                 (uint64_t(b[3]) << 32) | (uint64_t(b[4]) << 24) | (uint64_t(b[5]) << 16) |
                 (uint64_t(b[6]) << 8) | uint64_t(b[7]);
        buf->remove_prefix(N);
        return true;
    }

    bool DecodeUvarint64(rocksdb::Slice* buf, uint64_t* value) {
        if (buf->size() == 0) {
            return false;
        }
        int length = uint8_t((*buf)[0]) - kIntZero;
        buf->remove_prefix(1);  // skip length byte
        if (length <= kIntSmall) {
            *value = uint64_t(length);
            return true;
        }
        length -= kIntSmall;
        if (length < 0 || length > 8) {
            return false;
        } else if (buf->size() < length) {
            return false;
        }
        *value = 0;
        for (int i = 0; i < length; i++) {
            *value = (*value << 8) | uint8_t((*buf)[i]);
        }
        buf->remove_prefix(length);
        return true;
    }

    void EncodeTimestamp(std::string& s, int64_t wall_time, int32_t logical) {
        EncodeUint64(&s, uint64_t(wall_time));
        if (logical != 0) {
            EncodeUint32(&s, uint32_t(logical));
        }
    }

    std::string EncodeTimestamp(DBTimestamp ts) {
        std::string s;
        s.reserve(kMVCCVersionTimestampSize);
        EncodeTimestamp(s, ts.wall_time, ts.logical);
        return s;
    }

    bool EmptyTimestamp(DBTimestamp ts) {
        return ts.wall_time == 0 && ts.logical == 0;
    }

// MVCC keys are encoded as <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
    std::string EncodeKey(const rocksdb::Slice& key, int64_t wall_time, int32_t logical) {
        std::string s;
        const bool ts = wall_time != 0 || logical != 0;
        s.reserve(key.size() + 1 + (ts ? 1 + kMVCCVersionTimestampSize : 0));
        s.append(key.data(), key.size());
        if (ts) {
            // Add a NUL prefix to the timestamp data. See DBPrefixExtractor.Transform
            // for more details.
            s.push_back(0);
            EncodeTimestamp(s, wall_time, logical);
        }
        s.push_back(char(s.size() - key.size()));
        return s;
    }

// MVCC keys are encoded as <key>\x00[<wall_time>[<logical>]]<#timestamp-bytes>. A
// custom RocksDB comparator (DBComparator) is used to maintain the desired
// ordering as these keys do not sort lexicographically correctly.
    std::string EncodeKey(DBKey k) { return EncodeKey(ToSlice(k.key), k.wall_time, k.logical); }

    WARN_UNUSED_RESULT bool DecodeTimestamp(rocksdb::Slice* timestamp, int64_t* wall_time,
                                            int32_t* logical) {
        uint64_t w;
        if (!DecodeUint64(timestamp, &w)) {
            return false;
        }
        *wall_time = int64_t(w);
        *logical = 0;
        if (timestamp->size() > 0) {
            // TODO(peter): Use varint decoding here.
            uint32_t l;
            if (!DecodeUint32(timestamp, &l)) {
                return false;
            }
            *logical = int32_t(l);
        }
        return true;
    }

    WARN_UNUSED_RESULT bool DecodeTimestamp(rocksdb::Slice buf,
                                            znbase::util::hlc::Timestamp* timestamp) {
        int64_t wall_time;
        int32_t logical;
        if (!DecodeTimestamp(&buf, &wall_time, &logical)) {
            return false;
        }
        timestamp->set_wall_time(wall_time);
        timestamp->set_logical(logical);
        return true;
    }

    WARN_UNUSED_RESULT bool DecodeKey(rocksdb::Slice buf, rocksdb::Slice* key, int64_t* wall_time,
                                      int32_t* logical) {
        key->clear();

        rocksdb::Slice timestamp;
        if (!SplitKey(buf, key, &timestamp)) {
            return false;
        }
        if (timestamp.size() > 0) {
            timestamp.remove_prefix(1);  // The NUL prefix.
            if (!DecodeTimestamp(&timestamp, wall_time, logical)) {
                return false;
            }
        }
        return timestamp.empty();
    }

    WARN_UNUSED_RESULT bool DecodeRangeIDKey(rocksdb::Slice buf, int64_t* range_id,
                                             rocksdb::Slice* infix, rocksdb::Slice* suffix,
                                             rocksdb::Slice* detail) {
        if (!buf.starts_with(kLocalRangeIDPrefix)) {
            return false;
        }
        // Cut the prefix, the Range ID, and the infix specifier.
        buf.remove_prefix(kLocalRangeIDPrefix.size());
        uint64_t range_id_uint;
        if (!DecodeUvarint64(&buf, &range_id_uint)) {
            return false;
        }
        *range_id = int64_t(range_id_uint);
        if (buf.size() < kLocalSuffixLength + 1) {
            return false;
        }
        *infix = rocksdb::Slice(buf.data(), 1);
        buf.remove_prefix(1);
        *suffix = rocksdb::Slice(buf.data(), kLocalSuffixLength);
        buf.remove_prefix(kLocalSuffixLength);
        *detail = buf;
        return true;
    }

    rocksdb::Slice KeyPrefix(const rocksdb::Slice& src) {
        rocksdb::Slice key;
        rocksdb::Slice ts;
        if (!SplitKey(src, &key, &ts)) {
            return src;
        }
        // RocksDB requires that keys generated via Transform be comparable with
        // normal encoded MVCC keys. Encoded MVCC keys have a suffix indicating the
        // number of bytes of timestamp data. MVCC keys without a timestamp have a
        // suffix of 0. We're careful in EncodeKey to make sure that the user-key
        // always has a trailing 0. If there is no timestamp this falls out
        // naturally. If there is a timestamp we prepend a 0 to the encoded
        // timestamp data.
        assert(src.size() > key.size() && src[key.size()] == 0);
        return rocksdb::Slice(key.data(), key.size() + 1);
    }
    std::string ToString(VType t){
        switch (t){
            case Unknown :
                return "Unknown";
            case Null:
                return "Null";
            case NotNull:
                return "NotNull";
            case Int:
                return "Int";
            case Float:
                return "Float";
            case Decimal:
                return "Decimal";
            case Bytes:
                return "Bytes";
            case BytesDesc:
                return "BytesDesc";
            case Time:
                return "Time";
            case Duration:
                return "Duration";
            case True:
                return "True";
            case False:
                return "False";
            case UUID:
                return "UUID";
            case Array:
                return "Array";
            case IPAddr:
                return "IPAddr";
            case SentinelType:
                return "SentinelType";
//        case JSON:
//          return "JSON";
            case BitArray:
                return "BitArray";
            case BitArrayDesc:
                return "BitArrayDesc";
        }
        return "Unknown";
    }

}  // namespace znbase
