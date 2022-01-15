package tree

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// WeekByDate returns weeks in year.
func WeekByDate(t time.Time) int {
	var week int
	yearDay := t.YearDay()
	yearFirstDay := t.AddDate(0, 0, -yearDay+1)
	firstDayInWeek := int(yearFirstDay.Weekday())
	firstWeekDays := 1
	if firstDayInWeek != 0 {
		firstWeekDays = 7 - firstDayInWeek + 1
	}
	if yearDay <= firstWeekDays {
		week = 1
	} else {
		week = (yearDay-firstWeekDays)/7 + 2
	}
	return week
}

// DatepartTimestamp returns time format string for compatibility with the  date_part in Postgresql.
func DatepartTimestamp(f string, t time.Time) (int, error) {
	var getResult int
	switch f {
	case "year", "YEAR", "y", "yy", "yyy", "YYY", "Y", "YY":
		if t.Year() < 70 {
			getResult = t.Year() + 2000
		} else if t.Year() <= 100 {
			getResult = t.Year() + 1900
		} else {
			getResult = t.Year()
		}
	case "CENTURY", "C", "century", "centuries", "c":
		getResult = t.Year()/100 + 1

	case "DECADE", "decade", "DECADES", "decades":
		var Decade = t.Year() / 10
		getResult = Decade

	case "months", "month", "mon", "mm", "MONTH", "MONTHS", "MON", "MM":

		getResult = int(t.Month())
	case "WEEKDAY", "WEEKDAYS", "WD", "weekday", "weekdays", "wd":
		getResult = int(t.Weekday())

	case "week", "weeks", "w", "WEEK", "WEEKS", "W":
		getResult = WeekByDate(t)

	case "QUARTER", "QUA", "quarter", "qua":
		mon := t.Month()
		var quarter = int((mon + 2) / 3)
		getResult = quarter

	case "day", "d", "DAY", "D":
		getResult = t.Day()
	case "hour", "h", "HOUR", "H":
		if t.Hour() == 24 {
			getResult = 0
		} else {
			getResult = t.Hour()
		}

	case "MINUTE", "MIN", "minute", "min":
		getResult = t.Minute()

	case "SECOND", "SEC", "S", "second", "sec":
		getResult = t.Second()

	case "MILLISECONDS", "milliseconds", "MIS", "mis":
		getResult = t.Second()*1000 + t.Nanosecond()/1000000

	case "MICROSECONDS", "microseconds", "MRS", "mrs":
		getResult = t.Second()*1000000 + t.Nanosecond()/1000

	case "epoch", "epochs", "EPOCH", "EPOCHS":
		getResult = int(t.Unix())

	default:
		getResult = -1
		return getResult, fmt.Errorf("ERROR: timestamp units %s is not recognized", f)
	}
	return getResult, nil
}

// DatePartInterval apply interval(timestamp-timestamp) type in date_part.
func DatePartInterval(f string, r string) (int, error) {

	var getResult int
	match, err := regexp.MatchString("^[-0-9]*:[0-9]*:[0-9.]*$", r)
	if err != nil {
		return 0, err
	}
	if match {
		sep := func(r rune) bool {
			return r == ':'
		}
		var format []string
		var hours int
		format = strings.FieldsFunc(r, sep)
		if strings.Contains(format[0], "-") {
			format[1] = "-" + format[1]
			format[2] = "-" + format[2]
		}
		switch f {
		case "century", "CENTURY", "centuries", "CENTURIES":
			getResult = 0
		case "QUARTER", "quarter", "QUARTERS", "quarters":
			getResult = 1
		case "DECADE", "decade", "DECADES", "decades":
			getResult = 0
		case "year", "YEAR", "years", "YEARS":
			getResult = 0

		case "month", "MONTH", "months", "MONTHS":
			getResult = 0

		case "day", "DAY", "days", "DAYS":
			hours, err = strconv.Atoi(format[0])
			getResult = hours / 24

		case "HOUR", "hour", "HOURS", "hours":

			hours, err = strconv.Atoi(format[0])
			getResult = hours % 24

		case "minute", "MINUTE", "minutes", "MINUTES":
			getResult, err = strconv.Atoi(format[1])

		case "second", "SECOND", "seconds", "SECONDS":
			Msec, err := strconv.ParseFloat(format[2], 64)
			if err != nil {
				return 0, err
			}
			getResult = int(Msec)

		case "MILLISECONDS", "milliseconds":
			if strings.Contains(format[2], ".") {
				Msec, err := strconv.ParseFloat(format[2], 64)
				if err != nil {
					return 0, err
				}
				Msec = Msec * 1000
				getResult = int(Msec)
			} else {
				getResult, err = strconv.Atoi(format[2])
				getResult = getResult * 1000
				if err != nil {
					return 0, err
				}
			}

		case "MICROSECONDS", "microseconds":
			if strings.Contains(format[2], ".") {
				Msec, err := strconv.ParseFloat(format[2], 64)
				if err != nil {
					return 0, err
				}
				Msec = Msec * 1000000
				getResult = int(Msec)
			} else {
				getResult, err = strconv.Atoi(format[2])
				if err != nil {
					return 0, err
				}
				getResult = getResult * 1000000
			}
		case "epoch", "EPOCH", "epochs", "EPOCHS":
			Dinterval, err := ParseDInterval(r)
			if err != nil {
				return 0, err
			}
			getepoch, ok := Dinterval.AsInt64()
			if ok {
				getResult = int(getepoch)
			}
		default:
			getResult = -1
			return getResult, fmt.Errorf("ERROR: timestamp units %s is not not recognized", f)
		}
	} else {
		sep := func(r rune) bool {
			return r == ':' || r == ' ' || r == '/'
		}
		var format []string
		var format2 []string
		format = strings.FieldsFunc(r, sep)
		if strings.Contains(r, ":") && len(format) > 3 {
			format2 = format[len(format)-3:]
			if strings.Contains(format2[0], "-") {
				format2[1] = "-" + format2[1]
				format2[2] = "-" + format2[2]
			}
		}
		switch f {
		case "CENTURY", "century", "CENTURIES", "centuries":
			if strings.Contains(r, "year") {
				for key, value := range format {
					if value == "years" {
						getYear := format[key-1]
						getResult, err = strconv.Atoi(getYear)
						getResult = getResult / 100
						if err != nil {
							return 0, err
						}
						break
					}
				}
			} else {
				getResult = 0
			}
		case "DECADE", "decade", "DECADES", "decades":
			if strings.Contains(r, "year") {
				for key, value := range format {
					if value == "years" {
						getYear := format[key-1]
						getResult, err = strconv.Atoi(getYear)
						getResult = getResult / 10
						if err != nil {
							return 0, err
						}
						break
					}
				}
			} else {
				getResult = 0
			}

		case "QUARTER", "quarter", "QUARTERS", "quarters":
			if strings.Contains(r, "mons") {
				for key, value := range format {
					if value == "mons" {
						getMonth := format[key-1]
						getResult, err = strconv.Atoi(getMonth)
						getResult = (getResult + 2) / 3
						if err != nil {
							return 0, err
						}
						break
					}
				}
			} else {
				getResult = 0
			}
		case "year", "YEAR", "years", "YEARS":
			if strings.Contains(r, "year") {
				for key, value := range format {
					if value == "years" {
						getYear := format[key-1]
						getResult, err = strconv.Atoi(getYear)
						if err != nil {
							return 0, err
						}
						break
					}
				}
			} else {
				getResult = 0
			}
		case "month", "months", "MONTH", "MONTHS":
			if strings.Contains(r, "mons") || strings.Contains(r, "mon") {
				for key, value := range format {
					if value == "mons" || value == "mon" {
						getMonth := format[key-1]
						getResult, err = strconv.Atoi(getMonth)
						if err != nil {
							return 0, err
						}
						break
					}
				}
			} else {
				getResult = 0
			}
		case "day", "days", "DAY", "DAYS":
			if strings.Contains(r, "day") {
				for key, value := range format {
					if value == "days" {
						getDay := format[key-1]
						getResult, err = strconv.Atoi(getDay)
						break
					}
				}
			} else {
				getResult = 0
			}
		case "epoch", "EPOCH", "epochs", "EPOCHS":
			dinterval, err := ParseDInterval(r)
			if err != nil {
				return 0, err
			}
			getepoch, ok := dinterval.AsInt64()
			if ok {
				getResult = int(getepoch)
			}

		case "HOUR", "hour", "HOURS", "hours":
			if format2 != nil {
				getResult, err = strconv.Atoi(format2[0])
				if err != nil {
					return 0, err
				}
			} else {
				return 0, nil
			}
		case "minute", "MINUTE", "minutes", "MINUTES":
			if format2 != nil {
				getResult, err = strconv.Atoi(format2[1])
				if err != nil {
					return 0, err
				}
			} else {
				return 0, nil
			}
		case "second", "SECOND", "seconds", "SECONDS":
			if format2 != nil {
				Msec, err := strconv.ParseFloat(format2[2], 64)
				if err != nil {
					return 0, err
				}
				getResult = int(Msec)
			} else {
				return 0, nil
			}
		case "MILLISECONDS", "milliseconds", "millisecond", "MILLISECOND":
			if format2 != nil {
				if strings.Contains(format2[2], ".") {
					Msec, err := strconv.ParseFloat(format2[2], 64)
					if err != nil {
						return 0, err
					}
					Msec = Msec * 1000
					getResult = int(Msec)
				} else {
					getResult, err = strconv.Atoi(format2[2])
					if err != nil {
						return 0, err
					}
					getResult = getResult * 1000
				}

			} else {
				return 0, nil
			}

		case "MICROSECONDS", "microseconds", "microsecond", "MICROSECOND":
			if format2 != nil {
				if strings.Contains(format2[2], ".") {
					Msec, err := strconv.ParseFloat(format2[2], 64)
					if err != nil {
						return 0, err
					}
					Msec = Msec * 1000000
					getResult = int(Msec)
				} else {
					getResult, err = strconv.Atoi(format2[2])
					if err != nil {
						return 0, err
					}
					getResult = getResult * 1000000
				}
			} else {
				return 0, nil
			}

		default:
			getResult = -1
			return getResult, fmt.Errorf("ERROR: timestamp units %s is  not recognized", f)
		}
	}
	return getResult, nil
}

// DatePartIntervalsec apply interval(timestamp-timestamp) type in date_part.
func DatePartIntervalsec(f string, seconds int64) (int, error) {

	var getResult int
	switch f {
	case "year", "YEAR", "Y", "y":
		getResult = int(seconds / 31104000)

	case "month", "m", "MONTH", "M":
		getResult = int(seconds / 2592000)

	case "day", "DAY", "DD", "dd":
		getResult = int(seconds / 86400)

	case "HOUR", "H", "hour", "h":
		getResult = int((seconds % 86400) / 3600)

	case "HS", "hours", "HOURS":
		getResult = int(seconds / 3600)

	case "minute", "min", "MINUTE", "MIN":
		getResult = int((seconds % 3600) / 60)

	case "MINUTES", "minutes":
		getResult = int(seconds / 60)

	case "second", "sec", "SECOND", "SEC":
		getResult = int(seconds)
	case "frac_seconds", "FRAC_SECOND", "milliseconds", "MILLISECONDS":
		getResult = int(seconds * 1000)
	case "microseconds", "MICROSECONDS":
		getResult = int(seconds * 1000000)
	case "epoch":
		getResult = int(seconds)
	default:
		return 0, fmt.Errorf("ERROR: timestamp units %s is  not recognized", f)
	}
	return getResult, nil
}
