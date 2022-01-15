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
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/server/telemetry"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgwirebase"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqltelemetry"
	"github.com/znbasedb/znbase/pkg/util/duration"
	"github.com/znbasedb/znbase/pkg/util/ipaddr"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeofday"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/transform"
)

// pgType contains type metadata used in RowDescription messages.
type pgType struct {
	oid oid.Oid

	// Variable-size types have size=-1.
	// Note that the protocol has both int16 and int32 size fields,
	// so this attribute is an unsized int and should be cast
	// as needed.
	// This field does *not* correspond to the encoded length of a
	// data type, so it's unclear what, if anything, it is used for.
	// To get the right value, "SELECT oid, typlen FROM pg_type"
	// on a postgres server.
	size int
}

func pgTypeForParserType(t types.T) pgType {
	size := -1
	if s, variable := tree.DatumTypeSize(t); !variable {
		size = int(s)
	}
	return pgType{
		oid:  t.Oid(),
		size: size,
	}
}

const secondsInDay = 24 * 60 * 60

func (b *writeBuffer) writeTextDatum(
	ctx context.Context, d tree.Datum, conv sessiondata.DataConversionConfig,
) {
	if log.V(2) {
		log.Infof(ctx, "pgwire writing TEXT datum of type: %T, %#v", d, d)
	}
	if d == tree.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	switch v := tree.UnwrapDatum(nil, d).(type) {
	case *tree.DBitArray:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DBool:
		b.textFormatter.FormatNode(v)
		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DInt:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendInt(b.putbuf[4:4], int64(*v), 10)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DFloat:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := strconv.AppendFloat(b.putbuf[4:4], float64(*v), 'g', conv.GetFloatPrec(), 64)
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DDecimal:
		b.writeLengthPrefixedDatum(v)

	case *tree.DBytes:
		result := lex.EncodeByteArrayToRawBytes(
			string(*v), conv.BytesEncodeFormat, false /* skipHexPrefix */)
		b.putInt32(int32(len(result)))
		b.write([]byte(result))

	case *tree.DUuid:
		b.writeLengthPrefixedString(v.UUID.String())

	case *tree.DIPAddr:
		b.writeLengthPrefixedString(v.IPAddr.String())

	case *tree.DString:
		tmpbuf, err := ClientEncoding(conv.ClientEncoding, []byte(*v))
		if err != nil && log.V(2) {
			log.Infof(ctx, "ClientEncoding failed %v", err)
		}
		b.writeLengthPrefixedString(string(tmpbuf))

	case *tree.DCollatedString:
		tmpbuf, err := ClientEncoding(conv.ClientEncoding, []byte(v.Contents))
		if err != nil && log.V(2) {
			log.Infof(ctx, "ClientEncoding failed %v", err)
		}
		b.writeLengthPrefixedString(string(tmpbuf))

	case *tree.DDate:
		t := timeutil.Unix(int64(*v)*secondsInDay, 0)
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatDate(t, nil, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DTime:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTime(timeofday.TimeOfDay(*v), b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DTimestamp:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTs(v.Time, nil, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DTimestampTZ:
		// Start at offset 4 because `putInt32` clobbers the first 4 bytes.
		s := formatTs(v.Time, conv.Location, b.putbuf[4:4])
		b.putInt32(int32(len(s)))
		b.write(s)

	case *tree.DInterval:
		b.writeLengthPrefixedString(v.ValueAsString())

	case *tree.DJSON:
		tmpbuf, err := ClientEncoding(conv.ClientEncoding, []byte(v.JSON.String()))
		if err != nil && log.V(2) {
			log.Infof(ctx, "ClientEncoding failed %v", err)
		}
		b.writeLengthPrefixedString(string(tmpbuf))

	case *tree.DTuple:
		b.textFormatter.FormatNode(v)

		//encode ctx with client_encoding
		bufLen := b.textFormatter.Buffer.Len()
		tmpbuf := make([]byte, bufLen)
		_, _ = b.textFormatter.Buffer.Read(tmpbuf)
		tmpbuf, err := ClientEncoding(conv.ClientEncoding, tmpbuf)
		if err != nil {
			fmt.Printf("ClientEncoding failed %v", err)
		}
		b.textFormatter.Buffer.Write(tmpbuf)

		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DArray:
		switch d.ResolvedType().Oid() {
		case oid.T_int2vector, oid.T_oidvector:
			// vectors are serialized as a string of space-separated values.
			sep := ""
			// TODO(justin): add a test for nested arrays when #32552 is
			// addressed.
			for _, d := range v.Array {
				b.textFormatter.WriteString(sep)
				b.textFormatter.FormatNode(d)
				sep = " "
			}
		default:
			// Uses the default pgwire text format for arrays.
			b.textFormatter.FormatNode(v)
		}

		//encode ctx with client_encoding
		bufLen := b.textFormatter.Buffer.Len()
		tmpbuf := make([]byte, bufLen)
		_, _ = b.textFormatter.Buffer.Read(tmpbuf)
		tmpbuf, err := ClientEncoding(conv.ClientEncoding, tmpbuf)
		if err != nil {
			fmt.Printf("ClientEncoding failed %v", err)
		}
		b.textFormatter.Buffer.Write(tmpbuf)

		b.writeFromFmtCtx(b.textFormatter)

	case *tree.DOid:
		b.writeLengthPrefixedDatum(v)

	default:
		b.setError(errors.Errorf("unsupported type %T", d))
	}
}

// writeBinaryDatum writes d to the buffer. Oid must be specified for types
// that have various width encodings. It is ignored (and can be 0) for types
// with a 1:1 datum:oid mapping.
func (b *writeBuffer) writeBinaryDatum(
	ctx context.Context, d tree.Datum, sessionLoc *time.Location, Oid oid.Oid,
) {
	if log.V(2) {
		log.Infof(ctx, "pgwire writing BINARY datum of type: %T, %#v", d, d)
	}
	if d == tree.DNull {
		// NULL is encoded as -1; all other values have a length prefix.
		b.putInt32(-1)
		return
	}
	switch v := tree.UnwrapDatum(nil, d).(type) {
	case *tree.DBitArray:
		words, lastBitsUsed := v.EncodingParts()
		if len(words) == 0 {
			b.putInt32(4)
		} else {
			// Encode the length of the output bytes. It is computed here so we don't
			// have to keep a buffer.
			// 4: the int32 of the bitLen.
			// 8*(len(words)-1): number of 8-byte words except the last one since it's
			//   partial.
			// (lastBitsUsed+7)/8: number of bytes that will be written in the last
			//   partial word. The /8 rounds down, such that the +7 will cause 1-or-more
			//   bits to use a byte, but 0 will not.
			b.putInt32(4 + int32(8*(len(words)-1)) + int32((lastBitsUsed+7)/8))
		}
		bitLen := v.BitLen()
		b.putInt32(int32(bitLen))
		var byteBuf [8]byte
		for i := 0; i < len(words)-1; i++ {
			w := words[i]
			binary.BigEndian.PutUint64(byteBuf[:], w)
			b.write(byteBuf[:])
		}
		if len(words) > 0 {
			w := words[len(words)-1]
			for i := uint(0); i < uint(lastBitsUsed); i += 8 {
				c := byte(w >> (56 - i))
				b.writeByte(c)
			}
		}

	case *tree.DBool:
		b.putInt32(1)
		if *v {
			b.writeByte(1)
		} else {
			b.writeByte(0)
		}

	case *tree.DInt:
		switch Oid {
		case oid.T_int2:
			b.putInt32(2)
			b.putInt16(int16(*v))
		case oid.T_int4:
			b.putInt32(4)
			b.putInt32(int32(*v))
		case oid.T_int8:
			b.putInt32(8)
			b.putInt64(int64(*v))
		default:
			b.setError(errors.Errorf("unsupported int oid: %v", Oid))
		}

	case *tree.DFloat:
		switch Oid {
		case oid.T_float4:
			b.putInt32(4)
			b.putInt32(int32(math.Float32bits(float32(*v))))
		case oid.T_float8:
			b.putInt32(8)
			b.putInt64(int64(math.Float64bits(float64(*v))))
		default:
			b.setError(errors.Errorf("unsupported float oid: %v", Oid))
		}

	case *tree.DDecimal:
		if v.Form != apd.Finite {
			b.putInt32(8)
			// 0 digits.
			b.putInt32(0)
			// https://github.com/postgres/postgres/blob/ffa4cbd623dd69f9fa99e5e92426928a5782cf1a/src/backend/utils/adt/numeric.c#L169
			b.write([]byte{0xc0, 0, 0, 0})

			if v.Form == apd.Infinite {
				// TODO(mjibson): #32489
				// The above encoding is not correct for Infinity, but since that encoding
				// doesn't exist in postgres, it's unclear what to do. For now use the NaN
				// encoding and count it to see if anyone even needs this.
				telemetry.Inc(sqltelemetry.BinaryDecimalInfinityCounter)
			}

			return
		}

		alloc := struct {
			pgNum pgwirebase.PGNumeric

			bigI big.Int
		}{
			pgNum: pgwirebase.PGNumeric{
				// Since we use 2000 as the exponent limits in tree.DecimalCtx, this
				// conversion should not overflow.
				Dscale: int16(-v.Exponent),
			},
		}

		if v.Sign() >= 0 {
			alloc.pgNum.Sign = pgwirebase.PGNumericPos
		} else {
			alloc.pgNum.Sign = pgwirebase.PGNumericNeg
		}

		isZero := func(r rune) bool {
			return r == '0'
		}

		// Mostly cribbed from libpqtypes' str2num.
		digits := strings.TrimLeftFunc(alloc.bigI.Abs(&v.Coeff).String(), isZero)
		dweight := len(digits) - int(alloc.pgNum.Dscale) - 1
		digits = strings.TrimRightFunc(digits, isZero)

		if dweight >= 0 {
			alloc.pgNum.Weight = int16((dweight+1+pgwirebase.PGDecDigits-1)/pgwirebase.PGDecDigits - 1)
		} else {
			alloc.pgNum.Weight = int16(-((-dweight-1)/pgwirebase.PGDecDigits + 1))
		}
		offset := (int(alloc.pgNum.Weight)+1)*pgwirebase.PGDecDigits - (dweight + 1)
		alloc.pgNum.Ndigits = int16((len(digits) + offset + pgwirebase.PGDecDigits - 1) / pgwirebase.PGDecDigits)

		if len(digits) == 0 {
			offset = 0
			alloc.pgNum.Ndigits = 0
			alloc.pgNum.Weight = 0
		}

		digitIdx := -offset

		nextDigit := func() int16 {
			var ndigit int16
			for nextDigitIdx := digitIdx + pgwirebase.PGDecDigits; digitIdx < nextDigitIdx; digitIdx++ {
				ndigit *= 10
				if digitIdx >= 0 && digitIdx < len(digits) {
					ndigit += int16(digits[digitIdx] - '0')
				}
			}
			return ndigit
		}

		b.putInt32(int32(2 * (4 + alloc.pgNum.Ndigits)))
		b.putInt16(alloc.pgNum.Ndigits)
		b.putInt16(alloc.pgNum.Weight)
		b.putInt16(int16(alloc.pgNum.Sign))
		b.putInt16(alloc.pgNum.Dscale)

		for digitIdx < len(digits) {
			b.putInt16(nextDigit())
		}

	case *tree.DBytes:
		b.putInt32(int32(len(*v)))
		b.write([]byte(*v))

	case *tree.DUuid:
		b.putInt32(16)
		b.write(v.GetBytes())

	case *tree.DIPAddr:
		// We calculate the Postgres binary format for an IPAddr. For the spec see,
		// https://github.com/postgres/postgres/blob/81c5e46c490e2426db243eada186995da5bb0ba7/src/backend/utils/adt/network.c#L144
		// The pgBinary encoding is as follows:
		//  The int32 length of the following bytes.
		//  The family byte.
		//  The mask size byte.
		//  A 0 byte for is_cidr. It's ignored on the postgres frontend.
		//  The length of our IP bytes.
		//  The IP bytes.
		const pgIPAddrBinaryHeaderSize = 4
		if v.Family == ipaddr.IPv4family {
			b.putInt32(net.IPv4len + pgIPAddrBinaryHeaderSize)
			b.writeByte(pgwirebase.PGBinaryIPv4family)
			b.writeByte(v.Mask)
			b.writeByte(0)
			b.writeByte(byte(net.IPv4len))
			err := v.Addr.WriteIPv4Bytes(b)
			if err != nil {
				b.setError(err)
			}
		} else if v.Family == ipaddr.IPv6family {
			b.putInt32(net.IPv6len + pgIPAddrBinaryHeaderSize)
			b.writeByte(pgwirebase.PGBinaryIPv6family)
			b.writeByte(v.Mask)
			b.writeByte(0)
			b.writeByte(byte(net.IPv6len))
			err := v.Addr.WriteIPv6Bytes(b)
			if err != nil {
				b.setError(err)
			}
		} else {
			b.setError(errors.Errorf("error encoding inet to pgBinary: %v", v.IPAddr))
		}

	case *tree.DString:
		b.writeLengthPrefixedString(string(*v))

	case *tree.DCollatedString:
		b.writeLengthPrefixedString(v.Contents)

	case *tree.DTimestamp:
		b.putInt32(8)
		b.putInt64(timeToPgBinary(v.Time, nil))

	case *tree.DTimestampTZ:
		b.putInt32(8)
		b.putInt64(timeToPgBinary(v.Time, sessionLoc))

	case *tree.DDate:
		b.putInt32(4)
		b.putInt32(dateToPgBinary(v))

	case *tree.DTime:
		b.putInt32(8)
		b.putInt64(int64(*v))

	case *tree.DInterval:
		b.putInt32(16)
		b.putInt64(v.Nanos() / int64(time.Microsecond/time.Nanosecond))
		b.putInt32(int32(v.Days))
		b.putInt32(int32(v.Months))

	case *tree.DTuple:
		// TODO(andrei): We shouldn't be allocating a new buffer for every array.
		subWriter := newWriteBuffer(nil /* bytecount */)
		// Put the number of datums.
		subWriter.putInt32(int32(len(v.D)))
		for _, elem := range v.D {
			oid := elem.ResolvedType().Oid()
			subWriter.putInt32(int32(oid))
			subWriter.writeBinaryDatum(ctx, elem, sessionLoc, oid)
		}
		b.writeLengthPrefixedBuffer(&subWriter.wrapped)

	case *tree.DArray:
		if v.ParamTyp.FamilyEqual(types.AnyArray) {
			b.setError(pgerror.UnimplementedWithIssueDetailError(32552,
				"binenc", "unsupported binary serialization of multidimensional arrays"))
			return
		}
		// TODO(andrei): We shouldn't be allocating a new buffer for every array.
		subWriter := newWriteBuffer(nil /* bytecount */)
		// Put the number of dimensions. We currently support 1d arrays only.
		subWriter.putInt32(1)
		hasNulls := 0
		if v.HasNulls {
			hasNulls = 1
		}
		oid := v.ParamTyp.Oid()
		subWriter.putInt32(int32(hasNulls))
		subWriter.putInt32(int32(oid))
		subWriter.putInt32(int32(v.Len()))
		// Lower bound, we only support a lower bound of 1.
		subWriter.putInt32(1)
		for _, elem := range v.Array {
			subWriter.writeBinaryDatum(ctx, elem, sessionLoc, oid)
		}
		b.writeLengthPrefixedBuffer(&subWriter.wrapped)
	case *tree.DJSON:
		s := v.JSON.String()
		b.putInt32(int32(len(s) + 1))
		// Postgres version number, as of writing, `1` is the only valid value.
		b.writeByte(1)
		b.writeString(s)
	case *tree.DOid:
		b.putInt32(4)
		b.putInt32(int32(v.DInt))
	default:
		b.setError(errors.Errorf("unsupported type %T", d))
	}
}

const (
	pgTimeFormat              = "15:04:05.999999"
	pgDateFormat              = "2006-01-02"
	pgTimeStampFormatNoOffset = pgDateFormat + " " + pgTimeFormat
	pgTimeStampFormat         = pgTimeStampFormatNoOffset + "-07:00"
)

// formatTime formats t into a format lib/pq understands, appending to the
// provided tmp buffer and reallocating if needed. The function will then return
// the resulting buffer.
func formatTime(t timeofday.TimeOfDay, tmp []byte) []byte {
	return t.ToTime().AppendFormat(tmp, pgTimeFormat)
}

func formatTs(t time.Time, offset *time.Location, tmp []byte) (b []byte) {
	var format string
	if offset != nil {
		format = pgTimeStampFormat
	} else {
		format = pgTimeStampFormatNoOffset
	}
	return formatTsWithFormat(format, t, offset, tmp)
}

func formatDate(t time.Time, offset *time.Location, tmp []byte) []byte {
	return formatTsWithFormat(pgDateFormat, t, offset, tmp)
}

// formatTsWithFormat formats t with an optional offset into a format
// lib/pq understands, appending to the provided tmp buffer and
// reallocating if needed. The function will then return the resulting
// buffer. formatTsWithFormat is mostly cribbed from github.com/lib/pq.
func formatTsWithFormat(format string, t time.Time, offset *time.Location, tmp []byte) (b []byte) {
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	if offset != nil {
		t = t.In(offset)
	}

	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}

	b = t.AppendFormat(tmp, format)
	if bc {
		b = append(b, " BC"...)
	}
	return b
}

// timeToPgBinary calculates the Postgres binary format for a timestamp. The timestamp
// is represented as the number of microseconds between the given time and Jan 1, 2000
// (dubbed the PGEpochJDate), stored within an int64.
func timeToPgBinary(t time.Time, offset *time.Location) int64 {
	if offset != nil {
		t = t.In(offset)
	} else {
		t = t.UTC()
	}
	return duration.DiffMicros(t, pgwirebase.PGEpochJDate)
}

// dateToPgBinary calculates the Postgres binary format for a date. The date is
// represented as the number of days between the given date and Jan 1, 2000
// (dubbed the PGEpochJDate), stored within an int32.
func dateToPgBinary(d *tree.DDate) int32 {
	return int32(*d) - pgwirebase.PGEpochJDateFromUnix
}

// ClientEncoding support UTF2GBK
func ClientEncoding(charset string, in []byte) ([]byte, error) {
	switch charset {
	case "gbk":
		tmpbuf, err := Utf8ToGbk(in)
		if err != nil {
			return in, err
		}
		return tmpbuf, nil

	case "gb18030":
		tmpbuf, err := Utf8ToGb18030(in)
		if err != nil {
			return in, err
		}
		return tmpbuf, nil
	case "big5":
		tmpbuf, err := Utf8ToBig5(in)
		if err != nil {
			return in, err
		}
		return tmpbuf, nil
	default:
		return in, nil
	}
}

// ClientDecoding support GBK2UTF
func ClientDecoding(charset string, in []byte) ([]byte, error) {
	switch charset {
	case "gbk":
		tmpbuf, err := GbkToUtf8(in)
		if err != nil {
			return in, err
		}
		return tmpbuf, nil
	case "gb18030":
		tmpbuf, err := Gb18030ToUtf8(in)
		if err != nil {
			return in, err
		}
		return tmpbuf, nil
	case "big5":
		tmpbuf, err := Big5ToUtf8(in)
		if err != nil {
			return in, err
		}
		return tmpbuf, nil
	default:
		return in, nil
	}
}

// GbkToUtf8 make gbk to utf8
func GbkToUtf8(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("GbkToUtf8", string(s), "to ", string(d))
	return d, nil
}

// Gb18030ToUtf8 make gb18030 to utf8
func Gb18030ToUtf8(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GB18030.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("Gb18030ToUtf8", string(s), "to ", string(d))
	return d, nil
}

// Big5ToUtf8 make big5 to utf8
func Big5ToUtf8(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), traditionalchinese.Big5.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("Big5ToUtf8", string(s), "to ", string(d))
	return d, nil
}

// Utf8ToGbk make utf8 to gbk
func Utf8ToGbk(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewEncoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("Utf8ToGbk", string(s), "to ", string(d))
	return d, nil
}

// Utf8ToGb18030 make utf8 to gb18030
func Utf8ToGb18030(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GB18030.NewEncoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("Utf8ToGb18030", string(s), "to ", string(d))
	return d, nil
}

// Utf8ToBig5 make utf8 to big5
func Utf8ToBig5(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), traditionalchinese.Big5.NewEncoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("Utf8ToBig5", string(s), "to ", string(d))
	return d, nil
}
