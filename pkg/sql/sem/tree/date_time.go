package tree

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/util/timeofday"
)

var sMonthNames = map[int64]string{
	1:  "JAN",
	2:  "FEB",
	3:  "MAR",
	4:  "APR",
	5:  "MAY",
	6:  "JUN",
	7:  "JUL",
	8:  "AUG",
	9:  "SEP",
	10: "OCT",
	11: "NOV",
	12: "DEC",
}

const (
	maxMicrosecondsFloat = 0.999999
	maxMillisecondFloat  = 0.999
	maxMicrosecondsInt   = 999999
	maxMillisecondInt    = 999
)

var months = map[string]int64{
	"01": 1, "02": 2, "03": 3, "04": 4, "05": 5, "06": 6, "07": 7, "08": 8, "09": 9,
	"1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9, "10": 10, "11": 11, "12": 12,
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
	"january": 1, "february": 2, "march": 3, "april": 4, "june": 6, "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}

var weeks = map[string]int64{
	"mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6, "sun": 7,
	"monday": 1, "tuesday": 2, "wednesday": 3, "thursday": 4, "friday": 5, "saturday": 6, "sunday": 7,
}

type toDate struct {
	year        string
	month       string
	day         string
	hour        string
	minute      string
	second      string
	microsecond string
}

//StrToDate returns time format string.
func StrToDate(d, f string, ms bool) (string, error) {
	sep := func(r rune) bool {
		return r == ':' || r == ' ' || r == '-' || r == ',' || r == '/' || r == '.' || r == '+'
	}
	var temp toDate
	var t = toDate{
		"0001", "01", "01", "00", "00", "00", "000000",
	}
	null := ""
	var outDateStr string
	var indate, format, outdate, outtime, result []string

	//This is to match the format (20200101 && YYYY-MM-DD)
	var numFmt1 bool
	if len(strings.FieldsFunc(f, sep)) > 0 {
		if _, err := strconv.ParseFloat(d, 64); err == nil {
			numFmt1 = true
		}
	}

	//This is to match the format (20200101 && YYYYMMDD)
	if strings.Contains(f, "ym") ||
		strings.Contains(f, "yd") ||
		strings.Contains(f, "my") ||
		strings.Contains(f, "md") ||
		strings.Contains(f, "dy") ||
		strings.Contains(f, "dm") ||
		strings.Contains(f, "dh") ||
		strings.Contains(f, "hm") ||
		strings.Contains(f, "is") || numFmt1 {
		tempDate := strings.Join(strings.FieldsFunc(d, sep), "")
		tempFormat := strings.Join(strings.FieldsFunc(f, sep), "")
		for i, v := range tempFormat {
			switch string(v) {
			case "y":
				if i >= len(tempDate) {
					break
				}
				temp.year += string(tempDate[i])
			case "m", "o", "n":
				if i >= len(tempDate) {
					break
				}
				if len(tempFormat) > i+1 {
					if len(tempDate) >= i && string(tempFormat[i+1]) == "i" {
						temp.minute += string(tempDate[i])
						break
					}
				}
				temp.month += string(tempDate[i])
			case "i":
				if i >= len(tempDate) {
					break
				}
				temp.minute += string(tempDate[i])
			case "d":
				if i >= len(tempDate) {
					break
				}
				temp.day += string(tempDate[i])
			case "h":
				if i >= len(tempDate) {
					break
				}
				temp.hour += string(tempDate[i])
			case "s":
				if i >= len(tempDate) {
					break
				}
				temp.second += string(tempDate[i])
			case "f":
				if i >= len(tempDate) {
					break
				}
				if i == len(tempFormat)-1 {
					temp.microsecond += tempDate[i:]
					break
				}
				temp.microsecond += string(tempDate[i])
			default:
				return null, fmt.Errorf("The date format do not match")
			}
		}

		//Give the null value a default value
		if len(temp.year) == 0 {
			temp.year = "2020"
		}
		if len(temp.month) == 0 {
			temp.month = "01"
		}
		if len(temp.day) == 0 {
			temp.day = "01"
		}
		if len(temp.hour) == 0 {
			temp.hour = "00"
		}
		if len(temp.minute) == 0 {
			temp.minute = "00"
		}
		if len(temp.second) == 0 {
			temp.second = "00"
		}
		//judge the year, month and day are legal
		year, err := strconv.ParseInt(temp.year, 10, 64)
		if err != nil {
			return null, fmt.Errorf("the year part needs to be an integer")
		}
		mon := months[strings.ToLower(temp.month)]
		if mon == 0 {
			err := fmt.Errorf("The month in the date is out of range")
			return null, err
		}
		temp.month = fmt.Sprintf("%02d", mon)
		day, err := strconv.ParseInt(temp.day, 10, 64)
		if err != nil {
			return null, fmt.Errorf("the day part needs to be an integer")
		}
		temp.day = fmt.Sprintf("%02d", day)
		err1 := rangeOfMonth(day, mon, year)
		if err1 == true {
			err := fmt.Errorf("The day in the date is out of range")
			return null, err
		}

		//judge the hour, minutes and second are legal
		hour, err := strconv.ParseInt(temp.hour, 10, 64)
		if err != nil {
			return null, fmt.Errorf("the hour part needs to be an integer")
		}
		if hour < 0 || hour > 23 {
			err := fmt.Errorf("hour must be between 1 and 12")
			return null, err
		}
		minute, err := strconv.ParseInt(temp.minute, 10, 64)
		if err != nil {
			return null, fmt.Errorf("the minute part needs to be an integer")
		}
		if minute < 0 || minute > 59 {
			err := fmt.Errorf("minutes must be between 0 and 59")
			return null, err
		}
		sec, err := strconv.ParseInt(temp.second, 10, 64)
		if err != nil {
			return null, fmt.Errorf("the second part needs to be an integer")
		}
		if sec < 0 || sec > 59 {
			err := fmt.Errorf("second must be between 0 and 59")
			return null, err
		}

		outdate = append(outdate, temp.year, temp.month, temp.day)
		outDateStr = strings.Join(outdate, "-")

		outtime = append(outtime, temp.hour, temp.minute, temp.second)
		timeStr := strings.Join(outtime, ":")
		if ms == true {
			timeStr = timeStr + "." + temp.microsecond
		}
		result = append(result, outDateStr, timeStr)
		return strings.Join(result, " "), nil
	}

	//This is to match the format (2020-01-01 && YYYY-MM-DD)
	indate = strings.FieldsFunc(d, sep)
	format = strings.FieldsFunc(f, sep)
	var h int64
	lenDate := len(indate)
	for i, val := range format {
		switch strings.ToLower(val) {
		case "month", "mon", "mm":
			if i > lenDate-1 {
				break
			}
			t.month = sMonthNames[months[strings.ToLower(indate[i])]]
			if t.month == "" {
				err := fmt.Errorf("The month in the date is out of range")
				return null, err
			}
		case "y", "yy", "yyy", "yyyy":
			if i > lenDate-1 {
				break
			}
			y, err := strconv.ParseInt(indate[i], 10, 64)
			if err != nil {
				return null, fmt.Errorf("the year part needs to be an integer")
			}
			t.year = fmt.Sprintf("%d", y)
		case "day", "dd", "d":
			if i > lenDate-1 {
				break
			}
			d, err := strconv.ParseInt(indate[i], 10, 64)
			if err != nil {
				return null, fmt.Errorf("the day part needs to be an integer")
			}
			t.day = fmt.Sprintf("%02d", d)
		case "hh", "hh24", "hh12":
			if i > lenDate-1 {
				break
			}
			h, err := strconv.ParseInt(indate[i], 10, 64)
			if err != nil {
				return null, fmt.Errorf("the hour part needs to be an integer")
			}
			if h < 0 || h > 23 {
				err := fmt.Errorf("hour must be between 1 and 12")
				return null, err
			}
			t.hour = fmt.Sprintf("%02d", h)
		case "pm":
			if i > lenDate-1 {
				break
			}
			if h < 0 || h > 11 {
				err := fmt.Errorf("hour must be between 0 and 11")
				return null, err
			}
			h = h + 12
			t.hour = fmt.Sprintf("%02d", h)
		case "mi":
			if i > lenDate-1 {
				break
			}
			mi, err := strconv.ParseInt(indate[i], 10, 64)
			if err != nil {
				return null, fmt.Errorf("the minute part needs to be an integer")
			}
			if mi < 0 || mi > 59 {
				err := fmt.Errorf("minutes must be between 0 and 59")
				return null, err
			}
			t.minute = fmt.Sprintf("%02d", mi)
		case "ss":
			if i > lenDate-1 {
				break
			}
			s, err := strconv.ParseInt(indate[i], 10, 64)
			if err != nil {
				return null, fmt.Errorf("the second part needs to be an integer")
			}
			if s < 0 || s > 59 {
				err := fmt.Errorf("second must be between 0 and 59")
				return null, err
			}
			t.second = fmt.Sprintf("%02d", s)
		case "ff":
			if i > lenDate-1 {
				break
			}
			f, err := strconv.ParseInt(indate[i], 10, 64)
			if err != nil {
				return null, fmt.Errorf("the part needs to be an integer")
			}
			t.microsecond = fmt.Sprintf("%d", f)
		case "us":
			if i > lenDate-1 {
				break
			}
			tempF := fmt.Sprintf("0.%s", indate[i])
			f, err := strconv.ParseFloat(tempF, 64)
			if err != nil {
				return null, fmt.Errorf("the part needs to be a number")
			}
			if f == 0 {
				break
			} else if f > maxMicrosecondsFloat || f < 0 {
				return null, fmt.Errorf("time field value out of range")
			}
			if len(indate[i]) <= 6 {
				t.microsecond = indate[i]
			} else {
				intF, _ := strconv.ParseInt(indate[i], 10, 64)
				if intF > maxMicrosecondsInt {
					return null, fmt.Errorf("time field value out of range")
				}
				t.microsecond = fmt.Sprintf("%06d", intF)
			}
		case "ms":
			if i > lenDate-1 {
				break
			}
			tempMs := fmt.Sprintf("0.%s", indate[i])
			msec, err := strconv.ParseFloat(tempMs, 64)
			if err != nil {
				return null, fmt.Errorf("the part needs to be a number")
			}
			if msec == 0 {
				break
			} else if msec > maxMillisecondFloat || msec < 0 {
				return null, fmt.Errorf("time field value out of range")
			}
			if len(indate[i]) <= 3 {
				t.microsecond = indate[i]
			} else {
				intMs, _ := strconv.ParseInt(indate[i], 10, 64)
				if intMs > maxMillisecondInt {
					return null, fmt.Errorf("time field value out of range")
				}
				t.microsecond = fmt.Sprintf("%03d", intMs)
			}
		case "ww", "am", "cc", "ssss":
		default:
			return null, fmt.Errorf("The date format do not match")
		}
	}
	day, _ := strconv.ParseInt(t.day, 10, 64)
	mon := months[strings.ToLower(t.month)]
	year, _ := strconv.ParseInt(t.year, 10, 64)
	err1 := rangeOfMonth(day, mon, year)
	if err1 == true {
		err := fmt.Errorf("The day in the date is out of range")
		return null, err
	}
	outdate = append(outdate, t.year, t.month, t.day)
	outDateStr = strings.Join(outdate, "-")

	outtime = append(outtime, t.hour, t.minute, t.second)
	timeStr := strings.Join(outtime, ":")
	if ms == true {
		timeStr = timeStr + "." + t.microsecond
	}
	result = append(result, outDateStr, timeStr)
	return strings.Join(result, " "), nil
}

// TimestampToFormatStr Convert data of timestamp type into a string of type "yyyy-mm-dd hh:mm:ss".
func TimestampToFormatStr(t DTimestamp) (string, error) {
	fromStr := t.String()
	result := strings.Split(fromStr, "+")[0]
	return result[1 : len(result)-1], nil
}

//SubOfDate just return months between two dates.
func SubOfDate(f string, s string) (Datum, error) {
	sep := func(r rune) bool {
		return r == '-' || r == ' ' || r == ':' || r == '[' || r == ']'
	}
	var firstDate, secondDate []string
	f1 := fmt.Sprintf("%s", strings.Split(f, "'"))
	s2 := fmt.Sprintf("%s", strings.Split(s, "'"))
	firstDate = strings.FieldsFunc(f1, sep)
	secondDate = strings.FieldsFunc(s2, sep)
	//var day,mon,year
	year1, _ := strconv.ParseInt(firstDate[0], 10, 64)
	year2, _ := strconv.ParseInt(secondDate[0], 10, 64)
	month1 := months[strings.ToLower(firstDate[1])]
	month2 := months[strings.ToLower(secondDate[1])]
	day1, _ := strconv.ParseInt(firstDate[2], 10, 64)
	day2, _ := strconv.ParseInt(secondDate[2], 10, 64)
	err1 := rangeOfMonth(day1, month1, year1)
	err2 := rangeOfMonth(day2, month2, year2)
	if year1 < 1000 || year2 < 1000 || month1 == 0 || month2 == 0 || err1 == true || err2 == true {
		return nil, fmt.Errorf("The date is not as expected")
	}
	subDay := float64(day1-day2) * 0.0322580645161 //1/31
	subMonth := float64(month1 - month2)
	subYear := float64(year1-year2) * 12

	//Judge the special situation at the end of the month
	err3 := lastOfMonth(day1, month1, year1)
	err4 := lastOfMonth(day2, month2, year2)
	if err3 == true && err4 == true {
		subDay = 0
	}
	//Add the subYear and the subMonth and the subDay,then take the absolute value
	Sum := subDay + subMonth + subYear
	absSum := math.Abs(Sum)
	accurSum, _ := strconv.ParseFloat(fmt.Sprintf("%.9f", absSum), 64)
	err5 := judgeOfDate(subYear, subMonth, subDay)
	if err5 == false {
		return NewDFloat(DFloat(0 - accurSum)), nil
	}
	return NewDFloat(DFloat(accurSum)), nil
}

//Judge the positive and negative between the two dates
//var y=subYeaer, m=subMonth, d=subDay
func judgeOfDate(y, m, d float64) (err bool) {

	if y < 0 {
		return false
	} else if y == 0 {
		if m < 0 {
			return false
		} else if m == 0 {
			if d <= 0 {
				return false
			}
		} else {
			return true
		}
	}
	return true
}

//Determine if the day is out of range
func rangeOfMonth(d, m, y int64) (err bool) {
	day, month, year := d, m, y
	if day < 1 || day > 31 {
		return true
	}
	if day == 30 && month == 2 {
		return true
	}
	if day == 31 && (month == 2 || month == 4 || month == 6 || month == 9 || month == 11) {
		return true
	}
	if day == 29 && month == 2 {
		if (year%4 == 0 && year%100 != 0) || year%400 == 0 {
			return false
		}
		return true
	}

	return false
}

//Judge if it's the last day of the month
func lastOfMonth(d, m, y int64) (err bool) {
	day, month, year := d, m, y
	if day == 31 {
		if month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12 {
			return true
		}
		return false
	}
	if day == 30 && (month == 4 || month == 6 || month == 9 || month == 11) {
		return true
	}
	if day == 29 && month == 2 && ((year%4 == 0 && year%100 != 0) || year%400 == 0) {
		return true
	}
	if day == 28 && month == 2 {
		if (year%4 == 0 && year%100 != 0) || year%400 == 0 {
			return false
		}
		return true
	}
	return false
}

//NextDate returns the date of the first week after a specific date
func NextDate(indate, week string, days *DDate) (DDate, error) {
	seq := func(r rune) bool {
		return r == '-' || r == ':' || r == ' ' || r == ',' || r == '[' || r == ']'
	}
	//Gets the year, month and day of the date.
	var null DDate
	var nowDate []string //nextDate
	var intDays = int64(*days)
	date := fmt.Sprintf("%s", strings.Split(indate, "'"))
	nowDate = strings.FieldsFunc(date, seq)
	year, err1 := strconv.ParseInt(nowDate[0], 10, 64)
	if err1 != nil {
		return null, fmt.Errorf("Please enter the correct year")
	}
	if year < 1000 {
		return null, fmt.Errorf("Please enter the correct year")
	}
	mon := months[strings.ToLower(nowDate[1])]
	if mon == 0 {
		return null, fmt.Errorf("The month of date is out of range")
	}
	day, err2 := strconv.ParseInt(nowDate[2], 10, 64)
	if err2 != nil {
		return null, fmt.Errorf("Please enter the correct day")
	}
	err3 := rangeOfMonth(day, mon, year)
	if err3 == true {
		return null, fmt.Errorf("The day of date is out of range")
	}
	var y, m, c, nowWeek int64
	if mon >= 3 {
		m = mon
		y = year % 100
		c = year / 100
	} else {
		m = mon + 12
		y = (year - 1) % 100
		c = year / 100
	}
	nowWeek = y + (y / 4) + (c / 4) - 2*c + ((26 * (m + 1)) / 10) + day - 1
	if nowWeek < 0 {
		nowWeek = 7 - (-nowWeek)%7
	} else {
		nowWeek = nowWeek % 7
	}
	nextWeek := weeks[strings.ToLower(week)]
	if nextWeek == 0 {
		return null, fmt.Errorf("Please enter the correct weekday")
	}
	subDay := int64(math.Abs(float64(nowWeek - nextWeek)))
	if nowWeek >= nextWeek {
		intDays += nextWeek + 7 - nowWeek
	} else {
		switch subDay {
		case 1:
			intDays++
		case 2:
			intDays += 2
		case 3:
			intDays += 3
		case 4:
			intDays += 4
		case 5:
			intDays += 5
		case 6:
			intDays += 6
		}
	}
	return DDate(intDays), nil

}

func isLeap(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

// GetXV 模仿time包下的ISOWeek函数; 模式：返回值范围1~53,以周日作为一周的第一天
func GetXV(t time.Time) (year, week int) {
	year, month, day := t.Date()
	yday := day - 1
	wday := int(t.Weekday()+1+6) % 7 // weekday but Monday = 1.
	const (
		Mon int = iota
		Tue
		Wed
		Thu
		Fri
		Sat
	)

	// Calculate week as number of Mondays in year up to
	// and including today, plus 1 because the first week is week 0.
	// Putting the + 1 inside the numerator as a + 7 keeps the
	// numerator from being negative, which would cause it to
	// round incorrectly.
	week = (yday - wday + 7) / 7

	// The week number is now correct under the assumption
	// that the first Monday of the year is in week 1.
	// If Jan 1 is a Tuesday, Wednesday, or Thursday, the first Monday
	// is actually in week 2.
	jan1wday := (wday - yday + 7*53) % 7
	if Tue <= jan1wday && jan1wday <= Thu {
		week++
	}

	// If the week number is still 0, we're in early January but in
	// the last week of last year.
	if week == 0 {
		year--
		week = 52
		// A year has 53 weeks when Jan 1 or Dec 31 is a Thursday,
		// meaning Jan 1 of the next year is a Friday
		// or it was a leap year and Jan 1 of the next year is a Saturday.
		if jan1wday == Fri || (jan1wday == Sat && isLeap(year)) {
			week++
		}
	}

	// December 29 to 31 are in week 1 of next year if
	// they are after the last Thursday of the year and
	// December 31 is a Monday, Tuesday, or Wednesday.
	if month == 12 && day >= 29 && wday < Thu {
		if dec31wday := (wday + 31 - day) % 7; Mon <= dec31wday && dec31wday <= Wed {
			year++
			week = 1
		}
	}

	return
}

func weekNumber(t time.Time, char int) (result int) {
	weekday := int(t.Weekday())
	switch char {
	case 'u':
		// Monday as the first day of the week
		// 理想状态下，应为星期几
		originWeek := t.YearDay() % 7
		weekShift := weekday - originWeek
		// 如果偏差小于0，则补7
		if weekShift < 0 {
			weekShift = weekShift + 7
		}
		// 利用偏差重新计算
		newYDay := t.YearDay() + weekShift
		weekYear := newYDay / 7
		weekYearShift := newYDay % 7
		if weekYearShift != 0 {
			weekYear = weekYear + 1
		}
		result = weekYear
	case 'U':
		result = (t.YearDay() + 6 - weekday) / 7
	case 'V':
		_, result = GetXV(t)
	case 'X':
		result, _ = GetXV(t)
	case 'v':
		_, result = t.ISOWeek()
	case 'x':
		result, _ = t.ISOWeek()
	}
	return result
}

//day_suffix 将天转换为数字加英文后缀的形式
//例如：1 -> 1st, 2 -> 2nd,
func daySuffix(day int) (result string) {
	tenPlace := day / 10
	if tenPlace == 1 {
		result = fmt.Sprintf("%dth", day)
	} else {
		onePlace := day % 10
		switch onePlace {
		case 1:
			result = fmt.Sprintf("%dst", day)
		case 2:
			result = fmt.Sprintf("%dnd", day)
		case 3:
			result = fmt.Sprintf("%drd", day)
		default:
			result = fmt.Sprintf("%dth", day)
		}
	}
	return
}

//ParseDDateToFormat returns a string with time t formatted by format f.
func ParseDDateToFormat(t time.Time, f string) (string, error) {
	var result []string
	format := []rune(f)
	add := func(str string) {
		result = append(result, str)
	}
	for i := 0; i < len(format); i++ {
		switch format[i] {
		case '%':
			if i < len(format)-1 {
				switch format[i+1] {
				case 'a':
					add(shortDayNames[t.Weekday()])
				case 'b':
					add(shortMonthNames[t.Month()])
				case 'c':
					add(fmt.Sprintf("%2d", t.Month()))
				case 'd':
					add(fmt.Sprintf("%02d", t.Day()))
				case 'D':
					add(daySuffix(t.Day()))
				case 'e':
					add(fmt.Sprintf("%2d", t.Day()))
				case 'f':
					add(fmt.Sprintf("%09d", t.Nanosecond()))
				case 'H':
					add(fmt.Sprintf("%02d", t.Hour()))
				case 'h', 'I':
					if t.Hour() == 0 {
						add(fmt.Sprintf("%02d", 12))
					} else if t.Hour() > 12 {
						add(fmt.Sprintf("%02d", t.Hour()-12))
					} else {
						add(fmt.Sprintf("%02d", t.Hour()))
					}
				case 'i':
					add(fmt.Sprintf("%02d", t.Minute()))
				case 'j':
					add(fmt.Sprintf("%03d", t.YearDay()))
				case 'k':
					add(fmt.Sprintf("%2d", t.Hour()))
				case 'l':
					if t.Hour() == 0 {
						add(fmt.Sprintf("%2d", 12))
					} else if t.Hour() > 12 {
						add(fmt.Sprintf("%2d", t.Hour()-12))
					} else {
						add(fmt.Sprintf("%2d", t.Hour()))
					}
				case 'm':
					add(fmt.Sprintf("%02d", t.Month()))
				case 'M':
					add(longMonthNames[t.Month()])
				case 'p':
					if t.Hour() < 12 {
						add("AM")
					} else {
						add("PM")
					}
				case 'r':
					s, _ := ParseDDateToFormat(t, "%I:%M:%S %p")
					add(s)
				case 'R':
					add(fmt.Sprintf("%02d:%02d", t.Hour(), t.Minute()))
				case 's', 'S':
					add(fmt.Sprintf("%02d", t.Second()))
				case 'T':
					add(fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second()))
				case 'u':
					add(fmt.Sprintf("%02d", weekNumber(t, 'u')))
				case 'U':
					add(fmt.Sprintf("%02d", weekNumber(t, 'U')))
				case 'v':
					add(fmt.Sprintf("%02d", weekNumber(t, 'v')))
				case 'V':
					add(fmt.Sprintf("%02d", weekNumber(t, 'V')))
				case 'w':
					add(fmt.Sprintf("%d", t.Weekday()))
				case 'W':
					add(longDayNames[t.Weekday()])
				case 'x':
					add(fmt.Sprintf("%02d", weekNumber(t, 'x')))
				case 'X':
					add(fmt.Sprintf("%02d", weekNumber(t, 'X')))
				case 'y':
					add(fmt.Sprintf("%02d", t.Year()%100))
				case 'Y':
					add(fmt.Sprintf("%02d", t.Year()))
				case '%':
					add("%")
				default:
					add(string(format[i+1]))
				}
				i++
			}
		default:
			add(string(format[i]))
		}
	}
	return strings.Join(result, ""), nil
}

//StrToTime returns time format string.
func StrToTime(Ctx *EvalContext, Dtime Datum) (Datum, error) {
	var t toDate
	t.hour, t.minute, t.second = "00", "00", "00"
	stime := string(MustBeDString(Dtime))
	var mark bool
	var outTime []string
	for _, val := range stime {
		if val == ':' {
			mark = true
		}
	}
	if mark {
		lenIgnoreSpace := len(strings.Split(stime, " "))
		if lenIgnoreSpace == 2 {
			sstime := strings.Split(stime, " ")[1]
			targetTime := strings.Split(sstime, ":")
			for i, v := range targetTime {
				switch i {
				case 0:
					v0, _ := strconv.ParseFloat(v, 64)
					if v0 >= 24 || v0 < 0 {
						return DNull, nil
					}
					t.hour = v
				case 1:
					v1, _ := strconv.ParseFloat(v, 64)
					if v1 >= 60 || v1 < 0 {
						return DNull, nil
					}
					t.minute = v
				case 2:
					v2, _ := strconv.ParseFloat(v, 64)
					if v2 >= 60 || v2 < 0 {
						return DNull, nil
					}
					t.second = v
				}
			}
		} else {
			targetTime := strings.Split(stime, ":")
			for i, v := range targetTime {
				switch i {
				case 0:
					v0, _ := strconv.ParseFloat(v, 64)
					if v0 >= 24 || v0 < 0 {
						return DNull, nil
					}
					t.hour = v
				case 1:
					v1, _ := strconv.ParseFloat(v, 64)
					if v1 >= 60 || v1 < 0 {
						return DNull, nil
					}
					t.minute = v
				case 2:
					v2, _ := strconv.ParseFloat(v, 64)
					if v2 >= 60 || v2 < 0 {
						return DNull, nil
					}
					t.second = v
				}
			}
		}
		outTime = append(outTime, t.hour, t.minute, t.second)
		timeStr := NewDString(strings.Join(outTime, ":"))
		res, err := PerformCast(Ctx, timeStr, coltypes.Time)
		if err != nil {
			t.hour, t.minute, t.second = "00", "00", "00"
			var outTime2 []string
			ftime, err := PerformCast(Ctx, Dtime, coltypes.Float)
			if err != nil {
				return DNull, nil
			}
			dtime := float64(MustBeDFloat(ftime))
			if dtime >= 235960 {
				res, err := PerformCast(Ctx, Dtime, coltypes.Time)
				if err != nil {
					return DNull, nil
				}
				return res, nil
			}
			h := math.Floor(dtime / 10000)
			m := math.Floor(dtime/100) - h*100
			s, _ := strconv.ParseFloat(fmt.Sprintf("%.6f", dtime-h*10000-m*100), 64)
			t.hour, t.minute, t.second = fmt.Sprint(h), fmt.Sprint(m), fmt.Sprint(s)
			outTime2 = append(outTime2, t.hour, t.minute, t.second)
			timeStr := NewDString(strings.Join(outTime2, ":"))
			res, err := PerformCast(Ctx, timeStr, coltypes.Time)
			if err != nil {
				return DNull, nil
			}
			return res, nil
		}
		return res, nil
	}
	ftime, err := PerformCast(Ctx, Dtime, coltypes.Float)
	if err != nil {
		return DNull, nil
	}
	dtime := float64(MustBeDFloat(ftime))
	if dtime >= 235960 {
		res, err := PerformCast(Ctx, Dtime, coltypes.Time)
		if err != nil {
			return DNull, nil
		}
		return res, nil
	}
	h := math.Floor(dtime / 10000)
	m := math.Floor(dtime/100) - h*100
	s, _ := strconv.ParseFloat(fmt.Sprintf("%.6f", dtime-h*10000-m*100), 64)
	t.hour, t.minute, t.second = fmt.Sprint(h), fmt.Sprint(m), fmt.Sprint(s)

	outTime = append(outTime, t.hour, t.minute, t.second)
	timeStr := NewDString(strings.Join(outTime, ":"))
	res, err := PerformCast(Ctx, timeStr, coltypes.Time)
	if err != nil {
		//res2, err2 := PerformCast(Ctx, Dtime, coltypes.Time)
		//if err2 != nil {
		return DNull, nil
		//}
		//return res2, nil
	}
	return res, nil
}

//StrToNum returns number string.
func StrToNum(strNum, format string) (string, error) {
	var finalNum = make([]string, len(format))
	var count = 0
	var NULL = ""

	//invalid input handle
	if (strings.Contains(format, ".") ||
		strings.Contains(format, "D")) &&
		strings.Contains(format, "V") {
		return NULL, fmt.Errorf("cannot use \"V\" and decimal point together")
	}
	if strings.Contains(format, ".") &&
		strings.Contains(format, "D") {
		return NULL, fmt.Errorf("multiple decimal points")
	}
	if strings.Contains(format, "RN") {
		return NULL, fmt.Errorf("\"RN\" not supported for input")
	}
	if strings.Contains(format, "EEEE") {
		return NULL, fmt.Errorf("\"EEEE\" not supported for input")
	}

	//handle "-" negative number
	//handle PR format
	if strings.Contains(format, "PR") {
		for i := strings.Index(format, "PR"); i < len(format); i++ {
			if '0' <= format[i] && format[i] <= '9' {
				return NULL, fmt.Errorf("%s must be ahead of \"PR\"", "number")
			}
		}
		if strings.Contains(format, "S") ||
			strings.Contains(format, "PL") ||
			strings.Contains(format, "MI") ||
			strings.Contains(format, "SG") {
			return NULL, fmt.Errorf("cannot use \"PR\" and \"S\"\\/\"PL\"\\/\"MI\"\\/\"SG\" together")
		}
		if strings.Contains(string(strNum[0]), "<") {
			finalNum[count] = "-"
			count++
			strNum = strings.Replace(strNum, "<", "", 1)
		} else if strings.Contains(string(strNum[0]), "-") {
			finalNum[count] = "-"
			count++
			strNum = strings.Replace(strNum, "-", "", 1)
		}
		format = strings.ReplaceAll(format, "PR", "L")
	}
	//handle S format
	if strings.Contains(format, "S") && (strings.Index(format, "S") != strings.Index(format, "SG")) {
		if strings.Contains(format, "PL") ||
			strings.Contains(format, "MI") ||
			strings.Contains(format, "SG") {
			return NULL, fmt.Errorf("cannot use  \"S\"\\/\"PL\"\\/\"MI\"\\/\"SG\" together")
		}
		if strings.Count(format, "S") > 1 {
			return NULL, fmt.Errorf("cannot use \"S\" twice")
		}
		if strings.Contains(strNum, "-") {
			if strings.Contains(strNum, "+") {
				if strings.Index(strNum, "-") < strings.Index(strNum, "+") {
					finalNum[count] = "-"
					count++
					strNum = strings.Replace(strNum, "-", "", 1)
				}
				strNum = strings.Replace(strNum, "+", "", 1)
			} else {
				finalNum[count] = "-"
				count++
				strNum = strings.Replace(strNum, "-", "", 1)
			}
		} else if strings.Contains(strNum, "+") {
			finalNum[count] = "+"
			count++
			strNum = strings.Replace(strNum, "+", "", 1)
		}
	}
	//useless  B,C,L,TH
	//handle MI PL SG format
	if strings.Contains(format, "MI") ||
		strings.Contains(format, "PL") ||
		strings.Contains(format, "SG") {
		if strings.Contains(strNum, "-") {
			if strings.Contains(strNum, "+") {
				if strings.Index(strNum, "-") < strings.Index(strNum, "+") {
					finalNum[count] = "-"
					count++
					if strings.Index(strNum, "-") == 0 {
						strNum = strings.Replace(strNum, "-", "", 1)
					}
				}
				if strings.Index(strNum, "-") == 0 {
					strNum = strings.Replace(strNum, "+", "", 1)
				}
			} else {
				finalNum[count] = "-"
				count++
				if strings.Index(strNum, "-") == 0 {
					strNum = strings.Replace(strNum, "-", "", 1)
				}
			}
		}
	}
	format = strings.ReplaceAll(format, ".", "D")
	//handle useless  SG,PL,MI,B,C,L,TH
	format = strings.ReplaceAll(format, "SG", "L")
	format = strings.ReplaceAll(format, "PL", "L")
	format = strings.ReplaceAll(format, "MI", "L")
	format = strings.ReplaceAll(format, "S", "")
	format = strings.ReplaceAll(format, "B", "")
	format = strings.ReplaceAll(format, "C", "")
	format = strings.ReplaceAll(format, "TH", "LL")

	if strings.Index(strNum, "-") == 0 {
		finalNum[count] = "-"
		count++
		strNum = strings.Replace(strNum, "-", "", 1)
	}
	if strings.Index(strNum, "+") == 0 {
		finalNum[count] = "+"
		count++
		strNum = strings.Replace(strNum, "+", "", 1)
	}
	//handle decimal number
	if (strings.Contains(format, "D") && strings.Contains(strNum, ".")) || (strings.Contains(format, "D") && strings.Index(strNum, ".") == -1) {
		if strings.Contains(format, "D") && strings.Count(strNum, ".") > 1 {
			temp := strings.Split(strNum, ".")
			strNum = temp[0] + "." + temp[1]
			for i := 2; i < len(temp); i++ {
				strNum += temp[i]
			}
		}
		var intFinal = make([]string, len(strNum))
		var decFinal = make([]string, len(strNum))
		var DFinal = make([]string, 10)
		var j, k int
		DstrNum := strings.Index(strNum, ".")
		if DstrNum == -1 {
			DstrNum = len(strNum)
			strNum += "."
		}
		Dformat := strings.Index(format, "D")
		for i := 1; i <= 9; i++ {
			j += strings.Count(strNum[:DstrNum], fmt.Sprintf("%d", i))
			k += strings.Count(format[:Dformat], fmt.Sprintf("%d", i))
		}
		k += strings.Count(format[:Dformat], "0")
		if j <= k {
			intNumStr := strings.Split(strNum, ".")[0]
			decNumStr := strings.Split(strNum, ".")[1]
			intFormat := strings.Split(format, "D")[0]
			decFormat := strings.Split(format, "D")[1]
			var intCount, decCount = 0, 0
			for _, val := range intFormat {
				switch string(val) {
				case "L":
					if intCount >= len(intNumStr) {
						break
					}
					_, jump := strconv.Atoi(string(intNumStr[intCount]))
					if jump != nil {
						intCount++
					}
				case "9", "0":
					if intCount >= len(intNumStr) {
						break
					}
					d, err := strconv.Atoi(string(intNumStr[intCount]))
					if err == nil {
						intFinal[intCount] = fmt.Sprintf("%d", d)
					}
					intCount++
				case ",":
					if string(intNumStr[intCount]) == "," {
						intCount++
					}
				default:
					intCount++
				}
			}

			for _, val := range decFormat {
				switch string(val) {
				case "L":
					if decCount >= len(decNumStr) {
						break
					}
					_, jump := strconv.Atoi(string(decNumStr[decCount]))
					if jump != nil {
						decCount++
					}
				case "9", "0":
					if decCount >= len(decNumStr) {
						break
					}
					d, err := strconv.Atoi(string(decNumStr[decCount]))
					if err == nil {
						decFinal[decCount] = fmt.Sprintf("%d", d)
					}
					decCount++
				case ",":
					if string(decNumStr[decCount]) == "," {
						decCount++
					}
				default:
					decCount++
				}
			}
		} else {
			return NULL, fmt.Errorf("numeric field overflow")
		}
		DFinal[0] = finalNum[0]
		DFinal[1] = strings.Join(intFinal, "")
		DFinal[2] = "."
		DFinal[3] = strings.Join(decFinal, "")
		return strings.Join(DFinal, ""), nil
	}
	var finaCount = 0
	for _, val := range format {
		switch string(val) {
		case "L":
			if finaCount >= len(strNum) {
				break
			}
			_, jump := strconv.Atoi(string(strNum[finaCount]))
			if jump != nil {
				finaCount++
			}
		case "9", "0":
			if finaCount >= len(strNum) {
				break
			}
			d, err := strconv.Atoi(string(strNum[finaCount]))
			if err == nil {
				finalNum[count] = fmt.Sprintf("%d", d)
				count++
			}
			finaCount++
		case "V":

		case ",":
			if string(strNum[finaCount]) == "," {
				finaCount++
			}
		default:
			finaCount++
		}
	}
	return strings.Join(finalNum, ""), nil
}

//SubInterval calculate the Interval between two dates;t1-t2
func SubInterval(ctx *EvalContext, t1, t2 time.Time) (string, error) {
	var yInterval, mInterval, dInterval int
	y1, m1, d1 := t1.Year(), int(t1.Month()), t1.Day()
	y2, m2, d2 := t2.Year(), int(t2.Month()), t2.Day()
	month1 := t1.Month()
	date1, _ := MakeDate(y1, m1, d1)
	date2, _ := MakeDate(y2, m2, d2)
	dateInterval := int(*date1 - *date2)
	time1, err := ParseDTime(ctx, strings.Split(fmt.Sprint(t1), " ")[1])
	if err != nil {
		return "", err
	}
	time2, err := ParseDTime(ctx, strings.Split(fmt.Sprint(t2), " ")[1])
	if err != nil {
		return "", err
	}
	timeInterval := (timeofday.Difference(timeofday.TimeOfDay(*time1), timeofday.TimeOfDay(*time2))).Nanos()
	dInterval = d1 - d2
	mInterval = m1 - m2
	yInterval = y1 - y2

	if dateInterval != 0 {
		if dateInterval < 0 {
			if timeInterval > 0 {
				timeInterval -= 24 * time.Hour.Nanoseconds()
				dInterval++
			}
			if dInterval > 0 {
				switch month1 {
				case time.January, time.March, time.May, time.July, time.August, time.October, time.December:
					dInterval -= 31
				case time.April, time.June, time.September, time.November:
					dInterval -= 30
				case time.February:
					if isLeap(y1) {
						dInterval -= 29
					} else {
						dInterval -= 28
					}
				}
				mInterval++
			}
			mInterval += yInterval * 12
		} else {
			if timeInterval < 0 {
				timeInterval += 24 * time.Hour.Nanoseconds()
				dInterval--
			}
			if dInterval < 0 {
				switch month1 {
				case time.January, time.March, time.May, time.July, time.August, time.October, time.December:
					dInterval += 31
				case time.April, time.June, time.September, time.November:
					dInterval += 30
				case time.February:
					if isLeap(y1) {
						dInterval += 29
					} else {
						dInterval += 28
					}
				}
				mInterval--
			}
			mInterval += yInterval * 12
		}
	}
	sInterval := float64(timeInterval) / float64(time.Second.Nanoseconds())
	result := fmt.Sprintf("%d mons %d days %f seconds", mInterval, dInterval, sInterval)
	return result, nil
}

//AddDate add interval for time.time.
func AddDate(ctx *EvalContext, timeStamp time.Time, inter string) (time.Time, error) {

	year, month, day := timeStamp.Year(), timeStamp.Month(), timeStamp.Day()
	if strings.Contains(inter, "year") {
		addYear, _ := strconv.Atoi(strings.Split(inter, " ")[0])
		if isLeap(year) && month == 2 && day == 29 {
			if !isLeap(year + addYear) {
				day--
			}
		}
		year += addYear
	}

	if strings.Contains(inter, "mon") {
		interSlice := strings.Split(inter, " ")
		var monIndex int
		for i := 0; i < len(interSlice); i++ {
			if strings.Contains(interSlice[i], "mon") {
				monIndex = i - 1
				break
			}
		}
		addMonth, _ := strconv.Atoi(interSlice[monIndex])
		for addMonth > 0 {
			if month < 12 {
				month++
				addMonth--
			} else {
				addMonth--
				year++
				month = 1
			}
		}
		switch day {
		case 29:
			if day == 29 && month == 2 {
				if !isLeap(year) {
					day = 28
				}
			}
		case 30:
			if day == 30 && month == 2 {
				if isLeap(year) {
					day = 29
				} else {
					day = 28
				}
			}
		case 31:
			if day == 31 {
				switch month {
				case time.April, time.June, time.September, time.November:
					day = 30
				case time.February:
					if isLeap(year) {
						day = 29
					} else {
						day = 28
					}
				}
			}
		}
	}
	hour, minute, second := timeStamp.Clock()
	nanosecond := timeStamp.Nanosecond()
	timeStStr := fmt.Sprintf("%d-%d-%d %d:%d:%d.%d", year, month, day, hour, minute, second, nanosecond)
	dTimestamp, err := ParseDTimestamp(ctx, timeStStr, time.Microsecond)
	if err != nil {
		return timeStamp, err
	}
	timeStamp = dTimestamp.Time
	if strings.Contains(inter, "day") {
		interSlice := strings.Split(inter, " ")
		var dayIndex int
		for i := 0; i < len(interSlice); i++ {
			if strings.Contains(interSlice[i], "day") {
				dayIndex = i - 1
				break
			}
		}
		day, _ := strconv.Atoi(interSlice[dayIndex])
		timeStamp = timeStamp.AddDate(0, 0, day)
	}

	if strings.Contains(inter, ":") {
		interSlice := strings.Split(inter, " ")
		var timeIndex int
		for i := 0; i < len(interSlice); i++ {
			if strings.Contains(interSlice[i], ":") {
				timeIndex = i
				break
			}
		}
		timeSlice := strings.Split(interSlice[timeIndex], ":")
		h, _ := strconv.Atoi(timeSlice[0])
		m, _ := strconv.Atoi(timeSlice[1])
		s, _ := strconv.Atoi(timeSlice[2][0:2])
		interToSecond := h*3600 + m*60 + s
		timeStamp = timeStamp.Add(time.Duration(interToSecond) * time.Second)
	}

	if strings.Contains(inter, ".") {
		interSlice := strings.Split(inter, ".")
		var interMicro int
		var interMilli int
		if len(interSlice[1]) > 3 {
			interMilli, _ = strconv.Atoi(interSlice[1][0:3])
			timeStamp = timeStamp.Add(time.Duration(interMilli) * time.Millisecond)
			switch len(interSlice[1][3:]) {
			case 1:
				interMicro, _ = strconv.Atoi(interSlice[1][3:])
				interMicro *= 100
			case 2:
				interMicro, _ = strconv.Atoi(interSlice[1][3:])
				interMicro *= 10
			case 3:
				interMicro, _ = strconv.Atoi(interSlice[1][3:])
			}
			timeStamp = timeStamp.Add(time.Duration(interMicro) * time.Microsecond)
		} else {
			switch len(interSlice[1]) {
			case 1:
				interMilli, _ = strconv.Atoi(interSlice[1])
				interMilli *= 100
			case 2:
				interMilli, _ = strconv.Atoi(interSlice[1])
				interMilli *= 10
			case 3:
				interMilli, _ = strconv.Atoi(interSlice[1])
			}
			timeStamp = timeStamp.Add(time.Duration(interMilli) * time.Millisecond)
		}
	}
	return timeStamp, nil
}

//SubDate subtract interval for time.time.
func SubDate(ctx *EvalContext, timeStamp time.Time, inter string) (time.Time, error) {
	year, month, day := timeStamp.Year(), timeStamp.Month(), timeStamp.Day()
	if strings.Contains(inter, "year") {
		subYear, _ := strconv.Atoi(strings.Split(inter, " ")[0])
		if isLeap(year) && month == 2 && day == 29 {
			if !isLeap(year - subYear) {
				day--
			}
		}
		year -= subYear
	}

	if strings.Contains(inter, "mon") {
		interSlice := strings.Split(inter, " ")
		var monIndex int
		for i := 0; i < len(interSlice); i++ {
			if strings.Contains(interSlice[i], "mon") {
				monIndex = i - 1
				break
			}
		}
		subMonth, _ := strconv.Atoi(interSlice[monIndex])
		for subMonth > 0 {
			if month > 1 {
				month--
				subMonth--
			} else {
				subMonth--
				year--
				month = 12
			}
		}
		switch day {
		case 29:
			if month == 2 {
				if !isLeap(year) {
					day = 28
				}
			}
		case 30:
			if month == 2 {
				if isLeap(year) {
					day = 29
				} else {
					day = 28
				}
			}
		case 31:
			switch month {
			case time.April, time.June, time.September, time.November:
				day = 30
			case time.February:
				if isLeap(year) {
					day = 29
				} else {
					day = 28
				}

			}
		}
	}
	hour, minute, second := timeStamp.Clock()
	nanosecond := timeStamp.Nanosecond()
	timeStStr := fmt.Sprintf("%d-%d-%d %d:%d:%d.%d", year, month, day, hour, minute, second, nanosecond)
	dTimestamp, err := ParseDTimestamp(ctx, timeStStr, time.Microsecond)
	if err != nil {
		return timeStamp, err
	}
	timeStamp = dTimestamp.Time
	if strings.Contains(inter, "day") {
		interSlice := strings.Split(inter, " ")
		var dayIndex int
		for i := 0; i < len(interSlice); i++ {
			if strings.Contains(interSlice[i], "day") {
				dayIndex = i - 1
				break
			}
		}
		day, _ := strconv.Atoi(interSlice[dayIndex])
		timeStamp = timeStamp.AddDate(0, 0, -day)
	}

	if strings.Contains(inter, ":") {
		interSlice := strings.Split(inter, " ")
		var timeIndex int
		for i := 0; i < len(interSlice); i++ {
			if strings.Contains(interSlice[i], ":") {
				timeIndex = i
				break
			}
		}
		timeSlice := strings.Split(interSlice[timeIndex], ":")
		h, _ := strconv.Atoi(timeSlice[0])
		m, _ := strconv.Atoi(timeSlice[1])
		s, _ := strconv.Atoi(timeSlice[2][0:2])
		interToSecond := h*3600 + m*60 + s
		timeStamp = timeStamp.Add(-(time.Duration(interToSecond) * time.Second))
	}

	if strings.Contains(inter, ".") {
		interSlice := strings.Split(inter, ".")
		var interMicro int
		var interMilli int
		if len(interSlice[1]) > 3 {
			interMilli, _ = strconv.Atoi(interSlice[1][0:3])
			timeStamp = timeStamp.Add(-(time.Duration(interMilli) * time.Millisecond))
			switch len(interSlice[1][3:]) {
			case 1:
				interMicro, _ = strconv.Atoi(interSlice[1][3:])
				interMicro *= 100
			case 2:
				interMicro, _ = strconv.Atoi(interSlice[1][3:])
				interMicro *= 10
			case 3:
				interMicro, _ = strconv.Atoi(interSlice[1][3:])
			}
			timeStamp = timeStamp.Add(-(time.Duration(interMicro) * time.Microsecond))
		} else {
			switch len(interSlice[1]) {
			case 1:
				interMilli, _ = strconv.Atoi(interSlice[1])
				interMilli *= 100
			case 2:
				interMilli, _ = strconv.Atoi(interSlice[1])
				interMilli *= 10
			case 3:
				interMilli, _ = strconv.Atoi(interSlice[1])
			}
			timeStamp = timeStamp.Add(-(time.Duration(interMilli) * time.Millisecond))
		}
	}
	return timeStamp, nil
}
