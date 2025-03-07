// Copyright 2018  The Cockroach Authors.
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

package pgdate

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"
)

var daysInMonth = [2][13]int{
	{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
	{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
}

// dateToJulianDay is based on the date2j function in PostgreSQL 10.5.
func dateToJulianDay(year int, month int, day int) int {
	if month > 2 {
		month++
		year += 4800
	} else {
		month += 13
		year += 4799
	}

	century := year / 100
	jd := year*365 - 32167
	jd += year/4 - century + century/4
	jd += 7834*month/256 + day

	return jd
}

// isLeap returns true if the given year is a leap year.
func isLeap(year int) bool {
	return (year%4 == 0) && (year%100 != 0 || year%400 == 0)
}

// julianDayToDate is based on the j2date function in PostgreSQL 10.5.
func julianDayToDate(j int) (year int, month int, day int) {
	jd := uint(j)
	jd += 32044
	quad := jd / 146097
	extra := (jd-quad*146097)*4 + 3
	jd += 60 + quad*3 + extra/146097
	quad = jd / 1461
	jd -= quad * 1461
	y := jd * 4 / 1461
	if y != 0 {
		jd = (jd + 305) % 365
	} else {
		jd = (jd + 306) % 366
	}
	jd += 123
	y += quad * 4
	year = int(y - 4800)
	quad = jd * 2141 / 65536
	day = int(jd - 7834*quad/256)
	month = int((quad+10)%12 + 1)

	return
}

// stringChunk is returned by chunk().
type stringChunk struct {
	// The contiguous span of characters that did not match the filter and
	// which appear immediately before Match.
	NotMatch string
	// The contiguous span of characters that matched the filter.
	Match string
}

// chunk filters the runes in a string and populates the buffer with
// contiguous spans of alphanumeric characters.  The number of
// chunks will be returned along with any leftover, unmatching text.
// If the string cannot be stored entirely within the buffer,
// -1 will be returned.
func chunk(s string, buf []stringChunk) (int, string) {
	// pprof says that passing the buffer into chunk instead
	// of returning one is significantly faster than returning one here.
	// BenchmarkChunking went from 180 ns/op down to 78 ns/op,
	// presumably because the compiler can stack-allocate the
	// initial make().

	matchStart := 0
	matchEnd := 0
	previousMatchEnd := 0
	count := 0
	maxIdx := len(buf) - 1

	flush := func() bool {
		if matchEnd > matchStart {
			notMatch := s[previousMatchEnd:matchStart]
			match := s[matchStart:matchEnd]

			// Special-case to handle ddThh delimiter
			if len(match) == 5 && (match[2] == 'T' || match[2] == 't') {
				if count+1 > maxIdx {
					return false
				}
				buf[count] = stringChunk{
					NotMatch: notMatch,
					Match:    match[:2],
				}
				buf[count+1] = stringChunk{
					NotMatch: "t",
					Match:    match[3:],
				}
				count += 2
			} else {
				if count > maxIdx {
					return false
				}
				buf[count] = stringChunk{
					NotMatch: notMatch,
					Match:    match,
				}
				count++
			}
			previousMatchEnd = matchEnd
			matchStart = matchEnd
		}
		return true
	}

	// 对于类似于20001010101010这样的无空格长数字，如果想要把它加入到时间戳类型的列中，则需要手动添加一个空格，
	// 因为年月日与具体的时间是单独识别的，此时，也必须规定年月日的数字要写全，年份不能像日期一样卡亚只写两位
	if len(s) > 8 && !strings.Contains(s, " ") && !strings.Contains(s, " ") {
		for i := 0; i < len(s); i++ {
			if !unicode.IsDigit(rune(s[i])) {
				goto WithoutAddBlank
			}
		}
		sDate := s[:8]
		sTime := s[8:]
		var StrBuffer bytes.Buffer
		StrBuffer.WriteString(sDate)
		StrBuffer.WriteString(" ")
		StrBuffer.WriteString(sTime)
		s = StrBuffer.String()
	}
WithoutAddBlank:
	for offset, r := range s {
		if unicode.IsDigit(r) || unicode.IsLetter(r) {
			if matchStart >= matchEnd {
				matchStart = offset
			}
			// We're guarded by IsDigit() || IsLetter() above, so
			// RuneLen() should always return a reasonable value.
			matchEnd = offset + utf8.RuneLen(r)
		} else if !flush() {
			return -1, ""
		}
	}
	if !flush() {
		return -1, ""
	}

	return count, s[matchEnd:]
}
