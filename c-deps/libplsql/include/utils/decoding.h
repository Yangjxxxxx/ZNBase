//
// Created by zh on 2020/7/2.
//

#ifndef PLSQL_DECODING_H
#define PLSQL_DECODING_H

#endif // PLSQL_DECODING_H
typedef uintptr_t Datum;

typedef struct decodeTag{
    int typeOffset;
    int dataOffset;
    uint32 colID;
    int typ;
} decodeTag;

typedef struct decodeNonSort {
    char *buffer;
    int dataOffset;
    uint64 value;
} decodeNonSort;

decodeNonSort DecodeNonsortingUvarint(char *buffer);
decodeTag DecodeValueTag(char *buffer);
Datum decodeTableValue(char *buffer, int coltyp);
int64 decodeInt64(char *buffer);
Datum decodeUnTaggedDatum(char *buffer, int coltype);
