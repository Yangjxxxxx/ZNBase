//
// Created by zh on 2020/7/2.
//

#include "plpgsql.h"

// DecodeNonsortingUvarint decodes a value encoded by EncodeNonsortingUvarint. It
// returns the length of the encoded varint and value.
decodeNonSort DecodeNonsortingUvarint(char *buffer) {

    decodeNonSort resNonSort = {buffer, 0,0};
    int i;
    uint64 value = 0;
    for (i = 0; i < strlen(buffer); i++) {
        int b = buffer[i];
        value += (uint64)(b & 0x7f);
        if (b < 0x80) {
            resNonSort.buffer += (i + 1);
            resNonSort.dataOffset = (i + 1);
            resNonSort.value = value;
            return resNonSort;
        }
        value <<= 7;
    }
    return resNonSort;
}

decodeTag DecodeValueTag(char *buffer) {
    decodeTag resTag = {0, 0, 0, 0};
    if (strlen(buffer) == 0) {
        ereport(ERROR,
                (errmsg("empty array")));
    }

    int n = 0;
    uint64 tag = 0;
    decodeNonSort resNonSort = DecodeNonsortingUvarint(buffer);
    resTag.colID = (uint32)(resNonSort.value >> 4);
    resTag.typ = (int)(resNonSort.value & 0xf);
    resTag.typeOffset = resNonSort.dataOffset - 1;
    resTag.dataOffset = resNonSort.dataOffset;

    if (resTag.typ == 15) {
        // TODO(zh): need to complete
        /*
         *
         * */
    }
    return resTag;
}

Datum decodeTableValue(char *buffer, int coltyp) {
    decodeTag resTag = DecodeValueTag(buffer);
    if (resTag.typ == 1) {
         return NULL;
    }
    if (coltyp != BOOLOID) {
        buffer += resTag.dataOffset;
    }
    return decodeUnTaggedDatum(buffer, coltyp);
}

Datum decodeUnTaggedDatum(char *buffer, int coltype) {
    switch (coltype) {
    case 20: {
        int64 res64 = decodeInt64(buffer);
        return Int64GetDatum(res64);
    }
    default:{
        printf("ready to complete decode function");
        return -1;
    }
    }
}

int64 decodeInt64(char *buffer) {
    int64 value = 0;
    // int length = 0;

    // copy from go source code
    {
        uint64 x = 0;
        uint s = 0;
        int ok = 0;
        int i;
        for (i = 0; i < strlen(buffer); i++) {
	        uint8 b = (uint8)buffer[i];
            if ( b < 0x80) {
                if (i > 9 || (i == 9 && b > 1)) {
                    ereport(ERROR, (errmsg("int64 varint decoding failed: overflow")));
                }
                value = x | (uint64)b<<s;
                buffer += i + 1;
                ok = 1;
                break;
            }
            x |= (uint64)(b & 0x7f) << s;
            s += 7;
        }
        if (!ok) {
            ereport(ERROR, (errmsg("int64 varint decoding failed: overflow")));
        }
    }

    int64 x = value >> 1;
    if ((value & 1) != 0) {
        x = ~x;
    }
    return x;
}