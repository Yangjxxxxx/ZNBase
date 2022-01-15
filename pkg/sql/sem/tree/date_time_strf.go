package tree

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/znbasedb/apd"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

var longDayNames = []string{
	"Sunday",
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday",
}

var shortDayNames = []string{
	"Sun",
	"Mon",
	"Tue",
	"Wed",
	"Thu",
	"Fri",
	"Sat",
}

var shortMonthNames = []string{
	"---",
	"Jan",
	"Feb",
	"Mar",
	"Apr",
	"May",
	"Jun",
	"Jul",
	"Aug",
	"Sep",
	"Oct",
	"Nov",
	"Dec",
}

var longMonthNames = []string{
	"---",
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"August",
	"September",
	"October",
	"November",
	"December",
}

const (
	minYear         = 1
	maxYear         = 294276
	minutePerHour   = 3600
	secondPerYear   = 31104000
	secondPerMonth  = 2592000
	secondPerDay    = 86400
	secondPerHour   = 3600
	secondPerMinute = 60
	secondPerSecond = 1
	dayPerWeek      = 7
	monPerYear      = 12
	// argsMaxLenth
	argsMaxLenth = 7
)

// StrFormatOfTime returns time format string for compatibility with the  to_char in Postgresql.
func StrFormatOfTime(t time.Time, f string) (string, error) {
	sep := func(r rune) bool {
		return r == ':' || r == ' ' || r == '-' || r == '/' || r == ',' || r == '.'
	}

	f = strings.ReplaceAll(f, "A.M.", "A>M")
	f = strings.ReplaceAll(f, "P.M.", "P>M")
	f = strings.ReplaceAll(f, "a.m.", "a>m")
	f = strings.ReplaceAll(f, "p.m.", "p>m")
	f = strings.ReplaceAll(f, "Y,YYY", "Y>YYY")
	f = strings.ReplaceAll(f, "y,yyy", "y>yyy")

	indexOfSep := 0
	var result []string
	var format []string
	format = strings.FieldsFunc(f, sep)
	add := func(str string) {
		result = append(result, str)
	}
	for i := 0; i < len(format); i++ {
		switch format[i] {
		case "YYYYMMDD", "yyyymmdd":
			add(fmt.Sprintf("%04d", t.Year()))
			add(fmt.Sprintf("%02d", t.Month()))
			add(fmt.Sprintf("%02d", t.Day()))
		case "HHMISS", "HH12MISS", "hhmiss", "hh12miss":
			if t.Hour() == 0 {
				add(fmt.Sprintf("%02d", 12))
			} else if t.Hour() > 12 {
				add(fmt.Sprintf("%02d", t.Hour()-12))
			} else {
				add(fmt.Sprintf("%02d", t.Hour()))
			}
			add(fmt.Sprintf("%02d", t.Minute()))
			add(fmt.Sprintf("%02d", t.Second()))
		case "HH24MISS", "hh24miss":
			add(fmt.Sprintf("%02d", t.Hour()))
			add(fmt.Sprintf("%02d", t.Minute()))
			add(fmt.Sprintf("%02d", t.Second()))
		case "HH", "HH12", "hh", "hh12":
			if t.Hour() == 0 {
				add(fmt.Sprintf("%02d", 12))
			} else if t.Hour() > 12 {
				add(fmt.Sprintf("%02d", t.Hour()-12))
			} else {
				add(fmt.Sprintf("%02d", t.Hour()))
			}
		case "HH24", "hh24":
			add(fmt.Sprintf("%02d", t.Hour()))
		case "MI", "mi":
			add(fmt.Sprintf("%02d", t.Minute()))
		case "SS", "ss":
			add(fmt.Sprintf("%02d", t.Second()))
		case "MS", "ms":
			add(fmt.Sprintf("%03d", t.Nanosecond()/1000000))
		case "US", "us":
			add(fmt.Sprintf("%06d", t.Nanosecond()/1000))
		case "SSSS", "ssss":
			add(fmt.Sprintf("%d", t.Hour()*3600+t.Minute()*60+t.Second()))
		case "Y", "I", "y", "i":
			add(fmt.Sprintf("%d", t.Year()%10))
		case "YY", "IY", "yy", "iy":
			add(fmt.Sprintf("%02d", t.Year()%100))
		case "YYY", "IYY", "yyy", "iyy":
			add(fmt.Sprintf("%03d", t.Year()%1000))
		case "YYYY", "IYYY", "yyyy", "iyyy":
			add(fmt.Sprintf("%04d", t.Year()))
		case "Y>YYY", "y>yyy":
			add(fmt.Sprintf("%d", t.Year()/1000) + "," + fmt.Sprintf("%03d", t.Year()%1000))
		case "MONTH":
			add(strings.ToUpper(longMonthNames[t.Month()]))
		case "Month":
			add(longMonthNames[t.Month()])
		case "month":
			add(strings.ToLower(longMonthNames[t.Month()]))
		case "MON":
			add(strings.ToUpper(shortMonthNames[t.Month()]))
		case "Mon":
			add(shortMonthNames[t.Month()])
		case "mon":
			add(strings.ToLower(shortMonthNames[t.Month()]))
		case "MM", "mm":
			add(fmt.Sprintf("%02d", t.Month()))
		case "DAY":
			add(fmt.Sprintf("%-9s", strings.ToUpper(longDayNames[t.Weekday()])))
		case "Day":
			add(fmt.Sprintf("%-9s", longDayNames[t.Weekday()]))
		case "FMDay":
			add(fmt.Sprintf("%s", longDayNames[t.Weekday()]))
		case "day":
			add(fmt.Sprintf("%-9s", strings.ToLower(longDayNames[t.Weekday()])))
		case "DY":
			add(fmt.Sprintf("%-9s", strings.ToUpper(shortDayNames[t.Weekday()])))
		case "Dy":
			add(fmt.Sprintf("%-9s", shortDayNames[t.Weekday()]))
		case "dy":
			add(fmt.Sprintf("%-9s", strings.ToLower(shortDayNames[t.Weekday()])))
		case "DDD", "ddd":
			add(fmt.Sprintf("%03d", t.YearDay()))
		case "DD", "dd":
			add(fmt.Sprintf("%02d", t.Day()))
		case "FMDD":
			add(fmt.Sprintf("%d", t.Day()))
		case "D", "d":
			add(fmt.Sprintf("%d", int(t.Weekday())+1))
		case "ID", "id":
			w := t.Weekday()
			if w == 0 {
				w = 7
			}
			add(fmt.Sprintf("%d", w))
		case "W", "w":
			add(fmt.Sprintf("%d", (t.Day()-1)/7+1))
		case "WW", "ww":
			add(fmt.Sprintf("%d", (t.YearDay()-1)/7+1))
		case "AM", "PM":
			if t.Hour() < 12 {
				add("AM")
			} else {
				add("PM")
			}
		case "A>M", "P>M":
			if t.Hour() < 12 {
				add("A.M.")
			} else {
				add("P.M.")
			}
		case "am", "pm":
			if t.Hour() < 12 {
				add("am")
			} else {
				add("pm")
			}
		case "a>m", "p>m":
			if t.Hour() < 12 {
				add("a.m.")
			} else {
				add("p.m.")
			}
		case "CC", "cc":
			add(fmt.Sprintf("%02d", t.Year()/100+1))
		default:
			add(format[i])
		}
		if i < len(format)-1 {
			indexOfSep = strings.Index(f, format[i]) + len(format[i])
			temp := []byte(f)
			for j := 0; j < indexOfSep; j++ {
				temp[j] = ' '
			}
			f = string(temp[:])
			add(f[indexOfSep : strings.Index(f[indexOfSep:], format[i+1])+indexOfSep])
		}
	}
	return strings.Join(result, ""), nil
}

// StrFormatOfInterval returns time format string for compatibility with the  to_char in Postgresql.
func StrFormatOfInterval(seconds int64, nanoseconds int64, f string) (string, error) {
	sep := func(r rune) bool {
		return r == ':' || r == ' ' || r == '-' || r == '/' || r == '.'
	}
	strFormat := f
	indexOfSep := 0
	var result []string
	var format []string
	format = strings.FieldsFunc(f, sep)
	add := func(str string) {
		result = append(result, str)
	}
	addY := func(strY, str string) error {
		l := len(str)
		formatStr := "%0" + fmt.Sprintf("%dd", l)
		if len(strY) <= l {
			add(fmt.Sprintf(formatStr, seconds/secondPerYear))
		} else {
			y, err := strconv.Atoi(strY[len(strY)-l:])
			if err != nil {
				return err
			}
			add(fmt.Sprintf(formatStr, y))
		}
		return nil
	}

	for i := 0; i < len(format); i++ {
		strY := fmt.Sprint(seconds / secondPerYear)
		switch format[i] {
		case "YYYY", "yyyy", "YYY", "yyy", "YY", "yy":
			if err := addY(strY, format[i]); err != nil {
				return "", err
			}
		case "Y", "y":
			add(fmt.Sprintf("%s", string(strY[len(strY)-1])))
		case "MM", "mm":
			if strings.Contains(strFormat, "Y") ||
				strings.Contains(strFormat, "y") {
				add(fmt.Sprintf("%02d", (seconds%secondPerYear)/secondPerMonth))
			} else {
				add(fmt.Sprintf("%d", seconds/secondPerMonth))
			}
		case "DD", "dd":
			if strings.Contains(strFormat, "Y") ||
				strings.Contains(strFormat, "MM") ||
				strings.Contains(strFormat, "y") ||
				strings.Contains(strFormat, "mm") {
				add(fmt.Sprintf("%02d", (seconds%secondPerMonth)/secondPerDay))
			} else {
				add(fmt.Sprintf("%d", seconds/secondPerDay))
			}
		case "HH", "HH24", "hh", "hh24", "HH12", "hh12":
			if strings.Contains(strFormat, "Y") ||
				strings.Contains(strFormat, "MM") ||
				strings.Contains(strFormat, "DD") ||
				strings.Contains(strFormat, "y") ||
				strings.Contains(strFormat, "mm") ||
				strings.Contains(strFormat, "dd") {
				if strings.Contains(strFormat, "24") {
					add(fmt.Sprintf("%02d", (seconds%secondPerDay)/secondPerHour))
				} else {
					add(fmt.Sprintf("%02d", (seconds%secondPerDay)/secondPerHour%12))
				}
			} else {
				add(fmt.Sprintf("%d", seconds/secondPerHour))
			}
		case "MI", "mi":
			if strings.Contains(strFormat, "Y") ||
				strings.Contains(strFormat, "MM") ||
				strings.Contains(strFormat, "DD") ||
				strings.Contains(strFormat, "HH") ||
				strings.Contains(strFormat, "y") ||
				strings.Contains(strFormat, "mm") ||
				strings.Contains(strFormat, "dd") ||
				strings.Contains(strFormat, "hh") {
				add(fmt.Sprintf("%02d", (seconds%secondPerHour)/secondPerMinute))
			} else {
				add(fmt.Sprintf("%d", seconds/secondPerMinute))
			}
		case "SS", "ss":
			if strings.Contains(strFormat, "Y") ||
				strings.Contains(strFormat, "MM") ||
				strings.Contains(strFormat, "DD") ||
				strings.Contains(strFormat, "HH") ||
				strings.Contains(strFormat, "MI") ||
				strings.Contains(strFormat, "y") ||
				strings.Contains(strFormat, "mm") ||
				strings.Contains(strFormat, "dd") ||
				strings.Contains(strFormat, "hh") ||
				strings.Contains(strFormat, "mi") {
				add(fmt.Sprintf("%02d", seconds%secondPerMinute))
			} else {
				add(fmt.Sprintf("%d", secondPerSecond))
			}
		case "MS", "ms":
			add(fmt.Sprintf("%03d", nanoseconds/1000000))
		case "US", "us":
			add(fmt.Sprintf("%06d", nanoseconds/1000))
		case "SSSS", "ssss":
			add(fmt.Sprintf("%d", seconds%secondPerDay))
		default:
			add(format[i])
		}
		if i < len(format)-1 {
			indexOfSep = strings.Index(f, format[i]) + len(format[i])
			temp := []byte(f)
			for j := 0; j < indexOfSep; j++ {
				temp[j] = ' '
			}
			f = string(temp[:])
			add(f[indexOfSep : strings.Index(f[indexOfSep:], format[i+1])+indexOfSep])
		}
	}
	return strings.Join(result, ""), nil
}

// ParseNumToTime returns time for builtin function: make_timestamp and make_timestamptz
func ParseNumToTime(
	year, mon, day, hour, min int, sec float64, loc string, current string,
) (time.Time, error) {
	if err := isValidTime(year, mon, day, hour, min, sec); err != nil {
		return time.Time{}, err
	}

	secInt, secDecimal := math.Modf(sec)
	nSecond, err := time.ParseDuration(fmt.Sprintf("%fs", secDecimal))
	if err != nil {
		return timeutil.Now(), err
	}
	nSec := nSecond.Nanoseconds()
	if loc == "time" {
		return time.Date(0, time.Month(0), 0, hour, min, int(secInt), int(nSec), time.UTC), nil
	}
	if loc == "date" {
		return time.Date(year, time.Month(mon), day, 0, 0, 0, 0, time.UTC), nil
	}
	if loc == "utc" {
		return time.Date(year, time.Month(mon), day, hour, min, int(secInt), int(nSec), time.UTC), nil
	}

	cur, err := timeutil.TimeZoneStringToLocation(current, timeutil.TimeZoneStringToLocationISO8601Standard)
	if err != nil {
		return timeutil.Now(), err
	}
	t := time.Date(year, time.Month(mon), day, hour, min, int(sec), int(nSec), cur)
	if loc == "local" {
		return t, nil
	}
	location, err := timeutil.TimeZoneStringToLocation(loc, timeutil.TimeZoneStringToLocationISO8601Standard)
	if err != nil {
		return timeutil.Now(), err
	}
	locationTime := time.Date(year, time.Month(mon), day, hour, min, int(sec), int(nSec), location)
	_, offsetCur := t.Zone()
	_, offsetInput := locationTime.Zone()
	res := time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+offsetCur/minutePerHour-offsetInput/minutePerHour, t.Minute(), t.Second(), t.Nanosecond(), cur)
	return res, nil
}

func isValidTime(year, mon, day, hour, min int, sec float64) error {
	// check second, minute and hour
	if sec < 0.0 || sec > 60.0 || min < 0 || min > 59 || hour < 0 || hour > 23 {
		return fmt.Errorf("time field value out of range: %02d:%02d:%f", hour, min, sec)
	}
	// check year
	if year < minYear || year > maxYear {
		return fmt.Errorf("date field value out of range: %d-%02d-%02d", year, mon, day)
	}
	// check month
	if mon > 12 || mon < 1 {
		return fmt.Errorf("date field value out of range: %d-%02d-%02d", year, mon, day)
	}
	// check day
	isLeap := (year%4 == 0 && year%100 != 0) || year%400 == 0
	invalidDay := false
	switch mon {
	case 2:
		if isLeap {
			invalidDay = day < 1 || day > 29
		} else {
			invalidDay = day < 1 || day > 28
		}
	case 1, 3, 5, 7, 8, 10, 12:
		invalidDay = day < 1 || day > 31
	case 4, 6, 9, 11:
		invalidDay = day < 1 || day > 30
	}
	if invalidDay {
		return fmt.Errorf("date field value out of range: %d-%02d-%02d", year, mon, day)
	}

	return nil
}

func checkArgInt(arg interface{}) bool {
	switch arg.(type) {
	case *DInt:
		return true
	}
	return false
}

func checkArgFloat(arg interface{}) bool {
	switch arg.(type) {
	case *DDecimal:
		return true
	case *DInt:
		return true
	}
	return false
}
func checkArgs(args Datums) (bool, error) {
	if len(args) == argsMaxLenth {
		for i := 0; i < 6; i++ {
			if !checkArgInt(args[i]) {
				return false, fmt.Errorf("incorrect Parameter Format at  arg%d", i+1)
			}
		}
		if !checkArgFloat(args[6]) {
			return false, fmt.Errorf("incorrect Parameter Format at arg%d", 7)
		}
	} else {
		for k, v := range args {
			if !checkArgInt(v) {
				return false, fmt.Errorf("incorrect Parameter Format at  arg%d", k+1)
			}
		}
	}
	return true, nil
}

//ParseArg make_interval args parse
func ParseArg(args Datums) (int, int, int, error) {
	argArray := [argsMaxLenth]int{0, 0, 0, 0, 0, 0, 0}
	second := 0
	if ok, err1 := checkArgs(args); !ok {
		return -1, -1, -1, err1
	}
	if len(args) == argsMaxLenth {
		for i := 0; i < 6; i++ {
			argArray[i] = int(MustBeDInt(args[i]))
		}
		//check whether the last parameter is decimal or int
		if !checkArgInt(args[6]) {
			d, err := args[6].(*DDecimal).Float64()
			if err != nil {
				return 0, 0, 0, err
			}
			second = int(d * float64(time.Second))
		} else {
			second = int(MustBeDInt(args[6])) * int(time.Second)
		}
	} else {
		for k, v := range args {
			argArray[k] = int(MustBeDInt(v))
		}
		second = 0
	}
	Month := argArray[0]*monPerYear + argArray[1]
	Day := argArray[2]*dayPerWeek + argArray[3]
	Nanos := argArray[4]*int(time.Hour) + argArray[5]*int(time.Minute) + second
	return Month, Day, Nanos, nil
}

//MathType 参数类型判断和转换
func MathType(x *apd.Decimal) (float64, error) {
	var err2 error
	var argFloat float64
	if x.Exponent == 0 { //精度为0
		argInt, err := x.Int64()
		if err != nil {
			return -1, err
		}
		argFloat, err2 = strconv.ParseFloat(strconv.Itoa(int(argInt)), 64)
		if err2 != nil {
			return -1, err2
		}
	} else { //精度为负数
		argFloat, err2 = x.Float64()
		if err2 != nil {
			return -1, err2
		}
	}
	return argFloat, nil
}
