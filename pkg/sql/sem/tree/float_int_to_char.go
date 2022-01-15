package tree

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// DMark define double quotation marks
type DMark struct {
	Cont  string
	left  int
	right int
}

// FindDMark return double quotation marks
func FindDMark(str string) []DMark {

	var DMarks []DMark
	left, right := 0, 0
	leafflag := false
	for k, v := range str {
		if v == '"' {
			if k >= left && k >= right && !leafflag {
				left = k
				leafflag = true
				continue
			}
			if k > left && k > right && leafflag {
				right = k
				leafflag = false
				var temp DMark
				temp.left = left
				temp.right = right
				temp.Cont = str[left+1 : right]
				DMarks = append(DMarks, temp)
				continue
			}
		}

	}
	if leafflag {
		var temp DMark
		temp.left = left
		temp.right = len(str) - 1
		temp.Cont = str[left+1:]
		DMarks = append(DMarks, temp)
	}
	return DMarks
}

// IntToEEEE return format int
func IntToEEEE(num int, format string) (string, error) {

	i, j := strings.Index(format, "."), 0
	if 4 <= strings.Count(format, "E") &&
		strings.Count(format, "E") <= 7 &&
		strings.Contains(format, "EEEE") {
		for lastIndex := strings.LastIndex(format, "EEEE"); lastIndex < len(format); lastIndex++ {
			if format[lastIndex] == '#' {
				return "", errors.New("\"EEEE\" must be the last pattern used")
			}
		}

		if i == -1 {
			numFl := float64(num) / (math.Pow(10.0, float64(len(strconv.Itoa(num))-1)))
			tmp := strconv.FormatFloat(math.Abs(numFl), 'f', -1, 64)
			if tmp[len(tmp)-1] == '5' && len(tmp)-strings.Index(tmp, ".")-2 == 0 {
				if numFl >= 0 {
					numFl += 1 / math.Pow10(1)
				} else {
					numFl -= 1 / math.Pow10(1)
				}
			}
			return fmt.Sprintf("%.[2]*[1]f", numFl, 0) + "e+" +
				fmt.Sprintf("%02d", len(strconv.Itoa(num))-1), nil
		}
		count := 0
		for j = i; j < len(format); j++ {
			if format[j] == '#' {
				count++
			}
		}
		numFl := float64(num) / (math.Pow(10.0, float64(len(strconv.Itoa(int(math.Abs(float64(num)))))-1)))
		tmp := strconv.FormatFloat(math.Abs(numFl), 'f', -1, 64)
		if tmp[len(tmp)-1] == '5' && len(tmp)-strings.Index(tmp, ".")-2 == count {
			if numFl >= 0 {
				numFl += 1 / math.Pow10(count+1)
			} else {
				numFl -= 1 / math.Pow10(count+1)
			}
		}
		return fmt.Sprintf("%.[2]*[1]f", numFl, count) + "e+" +
			fmt.Sprintf("%02d", len(strconv.Itoa(int(math.Abs(float64(num)))))-1), nil

	}
	if strings.Count(format, "EEEE") >= 2 {
		return "", fmt.Errorf("cannot use %s twice", "EEEE")
	}
	return format, nil

}

// FillNum return number in format
func FillNum(num int, format string) string {
	strNum := strconv.Itoa(int(math.Abs(float64(num))))
	strSlice := strings.Split(format, ".")
	tmp1 := []byte(strSlice[0])
	numLen := len(strNum)
	if strings.Count(strSlice[0], "0")+
		strings.Count(strSlice[0], "9") >= numLen {
		for i := len(tmp1) - 1; i >= 0; i-- {
			if numLen > 0 && (tmp1[i] == '0' || tmp1[i] == '9') {
				tmp1[i] = strNum[numLen-1]
				numLen--
			}

			if numLen == 0 {
				numLen--
				continue
			}
			if numLen < 0 {
				if tmp1[i] == '9' {
					tmp1[i] = ' '
				}
			}
		}
		if len(strSlice) == 2 {
			tmp2 := []byte(strSlice[1])
			for i := 0; i < len(tmp2); i++ {
				if tmp2[i] == '9' {
					tmp2[i] = '0'
					if strings.Contains(format, "FM") {
						tmp2[i] = ' '
					}
				}
			}
			res := string(tmp1) + "." + string(tmp2)
			if strings.Contains(format, "FM") {
				res = fillFM(res)
			} else {
				for i := strings.Index(format, "0"); i >= 0 && i < len(tmp1); i++ {
					if tmp1[i] == ' ' {
						tmp1[i] = '0'
					}
				}
				res = string(tmp1) + "." + string(tmp2)
			}
			return strings.ReplaceAll(res, "FM", "")
		}
		str := string(tmp1)
		if !strings.Contains(format, "FM") {
			for i := strings.Index(format, "0"); i >= 0 && i < len(tmp1); i++ {
				if tmp1[i] == ' ' {
					tmp1[i] = '0'
				}
			}
			return string(tmp1)
		}
		str = fillFM(string(tmp1))
		return strings.ReplaceAll(str, "FM", "")

	}
	return strings.ReplaceAll(strings.ReplaceAll(format, "0", "#"), "9", "#")
}

// ReplaceChar return replace char
func ReplaceChar(num int, format string) (string, error) {
	if strings.Contains(format, "PL") {
		if num >= 0 {
			format = strings.ReplaceAll(format, "PL", "+")
		}
		format = strings.ReplaceAll(format, "PL", " ")
	}

	format = strings.ReplaceAll(format, "L", "￥")
	format = strings.ReplaceAll(format, "G", ",")
	if strings.Contains(format, ".") && strings.Contains(format, "D") ||
		strings.Count(format, "D") > 1 {
		return "", errors.New("multiple decimal points")
	}
	if !strings.Contains(format, ".") && strings.Contains(format, "D") {
		format = strings.ReplaceAll(format, "D", ".")
	}
	if strings.Contains(format, "th") || strings.Contains(format, "TH") {
		if num < 0 || strings.Contains(format, ".") || strings.Contains(format, "D") {
			s := strings.ReplaceAll(strings.ReplaceAll(format, "th", ""), "TH", "")
			return s, nil
		}
		remainder := num % 10
		switch remainder {
		case 1:
			return strings.ReplaceAll(strings.ReplaceAll(format, "th", "st"), "TH", "ST"), nil
		case 2:
			return strings.ReplaceAll(strings.ReplaceAll(format, "th", "nd"), "TH", "ND"), nil
		case 3:
			return strings.ReplaceAll(strings.ReplaceAll(format, "th", "rd"), "TH", "RD"), nil
		default:
			return strings.ReplaceAll(strings.ReplaceAll(format, "th", "th"), "TH", "TH"), nil
		}
	}
	return format, nil
}

func fillFM(format string) string {
	strSlice := []byte(format)
	head, end := strings.Index(format, "0"), 0
	if strings.Contains(format, ".") {
		if strings.Index(format, ".") < head {
			head = strings.Index(format, ".")
		}
	}
	for i := len(strSlice) - 1; i >= 0; i-- {

		if strSlice[i] == '0' {
			end = i
			if end < strings.Index(format, ".") {
				end = strings.Index(format, ".")
			}
			break
		}
	}
	for i := head; i >= 0 && i < end; i++ {
		if strSlice[i] == ' ' {
			strSlice[i] = '0'
		}
	}
	return strings.ReplaceAll(string(strSlice), " ", "")
}

// FillPR handler format with PR
func FillPR(num int, format string) (string, error) {
	for i := strings.Index(format, "PR"); i < len(format); i++ {
		if '0' <= format[i] && format[i] <= '9' {
			return "", fmt.Errorf("%s must be ahead of \"PR\"", "number")
		}
	}
	if strings.Contains(format, "S") ||
		strings.Contains(format, "PL") ||
		strings.Contains(format, "MI") ||
		strings.Contains(format, "SG") {
		return "", errors.New("cannot use \"PR\" and \"S\"\\/\"PL\"\\/\"MI\"\\/\"SG\" together")
	}
	head, end := 0, 0
	for i := 0; i < len(format); i++ {
		if '0' <= format[i] && format[i] <= '9' {
			head = i
			break
		}
	}
	for i := len(format) - 1; i >= 0; i-- {
		if '0' <= format[i] && format[i] <= '9' {
			end = i
			break
		}
	}
	if num < 0 {
		return format[:head] + "<" + format[head:end+1] + ">" + format[end+1:], nil
	}
	return format[:head] + " " + format[head:end+1] + " " + format[end+1:], nil
}

// FillV return fill V
func FillV(num int, format string) (int, string) {

	strSlice := strings.Split(format, "V")
	if len(strSlice) == 1 {
		return num, format
	}
	i := strings.Count(strSlice[1], "9")
	strings.ReplaceAll(strSlice[1], "9", "0")
	num = num * int(math.Pow(10, float64(i)))
	return num, strSlice[0] + strSlice[1]
}

// FillFloatV return fill V
func FillFloatV(num float64, format string) (float64, string) {

	strSlice := strings.Split(format, "V")
	if len(strSlice) == 1 {
		return num, format
	}
	i := strings.Count(strSlice[1], "9")
	strings.ReplaceAll(strSlice[1], "9", "0")
	num = num * math.Pow(10, float64(i))
	return num, strSlice[0] + strSlice[1]
}

// FloatToEEEE return format float
func FloatToEEEE(num float64, format string) (string, error) {
	i := strings.Index(format, ".")
	if 4 <= strings.Count(format, "E") &&
		strings.Count(format, "E") <= 7 &&
		strings.Contains(format, "EEEE") {
		tmp := math.Abs(num)
		dotNum := 0
		for j := strings.Index(format, "."); j < len(format); j++ {
			if format[j] == '0' || format[j] == '9' {
				dotNum++
			}
		}
		for lastIndex := strings.LastIndex(format, "EEEE"); lastIndex < len(format); lastIndex++ {
			if format[lastIndex] == '#' {
				return "", errors.New("\"EEEE\" must be the last pattern used")
			}
		}
		if tmp >= 1 {
			if i == -1 {
				numFl := tmp / (math.Pow(10.0, float64(len(strconv.Itoa(int(tmp)))-1)))
				tmp0 := strconv.FormatFloat(math.Abs(numFl), 'f', -1, 64)
				if tmp0[len(tmp0)-1] == '5' && len(tmp0)-strings.Index(tmp0, ".")-2 == 0 {
					if numFl >= 0 {
						numFl += 1 / math.Pow10(1)
					} else {
						numFl -= 1 / math.Pow10(1)
					}
				}
				if num > 0 {
					return fmt.Sprintf(" %.[2]*[1]f", numFl, 0) + "e+" +
						fmt.Sprintf("%02d", len(strconv.Itoa(int(tmp)))-1), nil
				}
				return fmt.Sprintf("-%.[2]*[1]f", numFl, 0) + "e+" +
					fmt.Sprintf("%02d", len(strconv.Itoa(int(tmp)))-1), nil
			}
			numFl := tmp / (math.Pow(10.0, float64(len(strconv.Itoa(int(tmp)))-1)))
			tmp0 := strconv.FormatFloat(math.Abs(numFl), 'f', -1, 64)
			if tmp0[len(tmp0)-1] == '5' && len(tmp0)-strings.Index(tmp0, ".")-2 == dotNum {
				if numFl >= 0 {
					numFl += 1 / math.Pow10(dotNum+1)
				} else {
					numFl -= 1 / math.Pow10(dotNum+1)
				}
			}
			if num > 0 {
				return fmt.Sprintf(" %.[2]*[1]f", numFl, dotNum) + "e+" +
					fmt.Sprintf("%02d", len(strconv.Itoa(int(tmp)))-1), nil
			}
			return fmt.Sprintf("-%.[2]*[1]f", numFl, dotNum) + "e+" +
				fmt.Sprintf("%02d", len(strconv.Itoa(int(tmp)))-1), nil

		}
		if math.Abs(num) < 1 {
			count := 0
			for tmp < 1 {
				tmp *= 10
				count++
			}
			for lastIndex := strings.LastIndex(format, "EEEE") + 3; lastIndex < len(format); lastIndex++ {
				if format[lastIndex] == '#' {
					return "", errors.New("\"EEEE\" must be the last pattern used")
				}
			}
			if i == -1 {
				if num > 0 {
					tmp0 := strconv.FormatFloat(math.Abs(tmp), 'f', -1, 64)
					if tmp0[len(tmp0)-1] == '5' && len(tmp0)-strings.Index(tmp0, ".")-2 == 0 {
						if tmp >= 0 {
							tmp += 1 / math.Pow10(1)
						} else {
							tmp -= 1 / math.Pow10(1)
						}
					}
					return fmt.Sprintf("%.[2]*[1]f", tmp, 0) + "e-" +
						fmt.Sprintf("%02d", count), nil
				}
				tmp0 := strconv.FormatFloat(math.Abs(tmp), 'f', -1, 64)
				if tmp0[len(tmp0)-1] == '5' && len(tmp0)-strings.Index(tmp0, ".")-2 == 0 {
					if tmp >= 0 {
						tmp += 1 / math.Pow10(1)
					} else {
						tmp -= 1 / math.Pow10(1)
					}
				}
				return fmt.Sprintf("-%.[2]*[1]f", tmp, 0) + "e-" +
					fmt.Sprintf("%02d", count), nil

			}
			if num > 0 {
				tmp0 := strconv.FormatFloat(math.Abs(tmp), 'f', -1, 64)
				if tmp0[len(tmp0)-1] == '5' && len(tmp0)-strings.Index(tmp0, ".")-2 == dotNum {
					if tmp >= 0 {
						tmp += 1 / math.Pow10(dotNum+1)
					} else {
						tmp -= 1 / math.Pow10(dotNum+1)
					}
				}

				return fmt.Sprintf("%.[2]*[1]f", tmp, dotNum) + "e-" +
					fmt.Sprintf("%02d", count), nil
			}
			tmp0 := strconv.FormatFloat(math.Abs(tmp), 'f', -1, 64)
			if tmp0[len(tmp0)-1] == '5' && len(tmp0)-strings.Index(tmp0, ".")-2 == dotNum {
				if tmp >= 0 {
					tmp += 1 / math.Pow10(dotNum+1)
				} else {
					tmp -= 1 / math.Pow10(dotNum+1)
				}
			}
			return fmt.Sprintf("-%.[2]*[1]f", tmp, dotNum) + "e-" +
				fmt.Sprintf("%02d", count), nil

		}
	}
	if strings.Count(format, "EEEE") >= 2 {
		return "", fmt.Errorf("cannot use %s twice", "EEEE")
	}
	return format, nil
}

// FillFloat return fill float
func FillFloat(num float64, format string) (string, error) {
	if strings.Contains(format, "D") && strings.Contains(format, ".") ||
		strings.Count(format, "D") > 1 || strings.Count(format, ".") > 1 {
		return "", errors.New("multiple decimal points")
	}
	format = strings.ReplaceAll(format, "D", ".")
	i := strings.Index(format, ".")
	count := 0
	for ; i >= 0 && i < len(format); i++ {
		if format[i] == '0' || format[i] == '9' {
			count++
		}
	}
	tmp := strconv.FormatFloat(math.Abs(num), 'f', -1, 64)
	if tmp[len(tmp)-1] == '5' && len(tmp)-strings.Index(tmp, ".")-2 == count {
		if num >= 0 {
			num += 1 / math.Pow10(count+1)
		} else {
			num -= 1 / math.Pow10(count+1)
		}
	}
	strNum := fmt.Sprintf("%.[2]*[1]f", math.Abs(num), count)
	strNumSlice := strings.Split(strNum, ".")
	formatSlice := strings.Split(format, ".")
	formatInt := []byte(formatSlice[0])
	numLen := len(strNum)
	if len(strNumSlice) == 1 && len(formatSlice) == 1 {
		if strings.Count(formatSlice[0], "0")+
			strings.Count(formatSlice[0], "9") >= numLen {
			for i := len(formatInt) - 1; i >= 0; i-- {
				if numLen > 0 && (formatInt[i] == '0' || formatInt[i] == '9') {
					formatInt[i] = strNum[numLen-1]
					numLen--
				}
				if numLen == 0 {
					numLen--
					continue
				}
				if numLen < 0 {
					if formatInt[i] == '9' {
						formatInt[i] = ' '
					}
				}
			}
		}
		str := string(formatInt)
		if strings.Contains(str, "FM") {
			str = fillFM(str)
		} else {
			for i := strings.Index(formatSlice[0], "0"); i >= 0 && i < len(formatInt); i++ {
				if formatInt[i] == ' ' {
					formatInt[i] = '0'
				}
			}
			str = string(formatInt)
		}
		return strings.ReplaceAll(str, "FM", ""), nil
	}
	formatDecimal := []byte(formatSlice[1])
	tmpStrNum := strconv.FormatFloat(math.Abs(num), 'f', -1, 64)
	j := strings.Index(tmpStrNum, ".") + 1
	for i := 0; i < len(formatDecimal); i++ {
		if j <= len(tmpStrNum)-1 && (formatDecimal[i] == '0' || formatDecimal[i] == '9') {
			formatDecimal[i] = strNum[j]
			j++
		}
		if j == len(tmpStrNum) {
			j++
			continue
		}
		if j > len(tmpStrNum) {
			if formatDecimal[i] == '9' {
				formatDecimal[i] = ' '
			}
		}
	}
	j = strings.Index(strNum, ".") - 1
	for i := len(formatInt) - 1; i >= 0; i-- {

		if j >= 0 && (formatInt[i] == '0' || formatInt[i] == '9') {
			if j == 0 && strNum[j] == '0' {
				j = -2
			} else {
				formatInt[i] = strNum[j]
				j--
			}
		}
		if j == -1 {
			j--
			continue
		}
		if j < -1 {
			if formatInt[i] == '9' {
				formatInt[i] = ' '
			}
		}
	}
	str := string(formatInt) + "." + string(formatDecimal)
	if strings.Contains(str, "FM") {
		str = fillFM(str)
	} else {
		for i := strings.Index(formatSlice[0], "0"); i >= 0 && i < len(formatInt); i++ {
			if formatInt[i] == ' ' {
				formatInt[i] = '0'
			}
		}
		str1 := string(formatInt)
		for i := len(formatDecimal) - 1; i >= 0; i-- {
			if formatDecimal[i] == ' ' {
				formatDecimal[i] = '0'
			}
		}
		str = str1 + "." + string(formatDecimal)
	}
	return strings.ReplaceAll(str, "FM", ""), nil
}

// ReplaceFloatChar return replace char
func ReplaceFloatChar(num float64, format string) (string, error) {
	if strings.Contains(format, "PL") {
		if num >= 0 {
			format = strings.ReplaceAll(format, "PL", "+")
		}
		format = strings.ReplaceAll(format, "PL", " ")
	}

	format = strings.ReplaceAll(format, "L", "￥")
	format = strings.ReplaceAll(format, "G", ",")
	if strings.Contains(format, ".") && strings.Contains(format, "D") ||
		strings.Count(format, "D") > 1 {
		return "", errors.New("multiple decimal points")
	}
	if !strings.Contains(format, ".") && strings.Contains(format, "D") {
		format = strings.ReplaceAll(format, "D", ".")
	}
	if strings.Contains(format, "th") || strings.Contains(format, "TH") {
		if num < 0 || strings.Contains(format, ".") || strings.Contains(format, "D") {
			s := strings.ReplaceAll(strings.ReplaceAll(format, "th", ""), "TH", "")
			return s, nil
		}
		tmp := strconv.FormatFloat(math.Abs(num), 'f', -1, 64)
		if tmp[len(tmp)-1] == '5' && len(tmp)-strings.Index(tmp, ".")-2 == 0 {
			if num >= 0 {
				num += 1 / math.Pow10(1)
			} else {
				num -= 1 / math.Pow10(1)
			}
		}
		intNum, _ := strconv.Atoi(fmt.Sprintf("%.[2]*[1]f", math.Abs(num), 0))
		remainder := intNum % 10
		switch remainder {
		case 1:
			return strings.ReplaceAll(strings.ReplaceAll(format, "th", "st"), "TH", "ST"), nil
		case 2:
			return strings.ReplaceAll(strings.ReplaceAll(format, "th", "nd"), "TH", "ND"), nil
		case 3:
			return strings.ReplaceAll(strings.ReplaceAll(format, "th", "rd"), "TH", "RD"), nil
		default:
			return strings.ReplaceAll(strings.ReplaceAll(format, "th", "th"), "TH", "TH"), nil
		}
	}
	return format, nil
}

func convert(num int) string {
	if num > 3999 || num <= 0 {
		return "###############"
	}
	var numArr = []int{1, 4, 5, 9, 10, 40, 50, 90, 100, 400, 500, 900, 1000}
	var romArr = []string{"I", "IV", "V", "IX", "X", "XL", "L", "XC", "C", "CD", "D", "CM", "M"}
	var romStr = ""
	if num > 0 {
		var index = len(numArr) - 1
		for index >= 0 {
			if num >= numArr[index] {
				num -= numArr[index]
				romStr += romArr[index]
			} else {
				index--
			}
		}
	}
	return romStr
}

func prodcSpace(num int, str string) string {
	var i int
	var s string
	for i = 0; i < num; i++ {
		s = s + str
	}
	return s
}

// RN return roman num
func RN(num int, format string) string {
	str := convert(num)
	space := prodcSpace(15-len(str), " ")
	format = strings.ReplaceAll(format, "PL", " ")
	format = strings.ReplaceAll(format, "L", "￥")
	format = strings.ReplaceAll(format, "MI", " ")
	format = strings.ReplaceAll(format, "SG", "")
	format = strings.ReplaceAll(format, "PR", " ")
	format = strings.ReplaceAll(format, "D", "")
	format = strings.ReplaceAll(format, "th", "")
	format = strings.ReplaceAll(format, "TH", "")
	format = strings.ReplaceAll(format, "S", "")
	format = strings.ReplaceAll(format, ",", " ")
	format = strings.ReplaceAll(format, ".", "")
	return strings.ReplaceAll(format, "RN", space+str)
}
