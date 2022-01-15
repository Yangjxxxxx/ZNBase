package tree

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/coltypes"
)

const (
	notPlaceholder = 0
	regRule1       = "\\%.*?s"
	regRule2       = "\\%.*?\\$"
	regRule3       = "\\*.*?\\$"
)

// Word string record struct
type Word struct {
	Name          string
	IsPlaceholder bool
	Position      int //标记占位符对应的参数位置
}

// produce Space symbol
func productSpace(num int, str string) string {
	var i int
	var s string
	for i = 0; i < num; i++ {
		s = s + str
	}
	return s
}

func jugCorrect(s string, index int) error { //判断是否是支持类型，如果不是报错
	for i := index; i < len(s); i++ {
		if ('a' <= s[i] && s[i] <= 'z') || ('A' <= s[i] && s[i] <= 'Z') {
			if i == index+1 {
				if s[i] == 'L' || s[i] == 's' || s[i] == 'I' || s[i] == '%' {
					return nil
				}
			}
			if s[i] == 's' {
				return nil
			}
			return fmt.Errorf("unsupported type at '%s'", s[i:i+1]) //不支持的类型

		}
		if s[i] == '%' && i > index {
			return errors.New(" No valid parameters after '%'")
		}
	}
	return errors.New(" No valid parameters after '%'") //% 后没有有效的参数
}

// CutString separates special character from general character
func CutString(s string) ([]Word, error) {
	var position int //占位符对应的格式化参数位置
	var words []Word //格式化字符串处理后存储切片
	var tempWord Word
	wordStr := s[:]
	sFlag := false //%%时候标记符，只保留一个%
	fFlag := false //识别到一个%时标记
	head := 0
	for k, v := range wordStr {
		if sFlag {
			sFlag = false
			continue
		}
		if v == '%' {
			fFlag = true
			if k < len(s)-1 {
				if wordStr[k+1] == '%' { //%%符号处理
					temp := wordStr[head : k+1]
					tempWord.Name = temp
					tempWord.IsPlaceholder = false
					tempWord.Position = notPlaceholder
					words = append(words, tempWord)
					sFlag = true
					fFlag = false
					head = k + 2
					continue
				}
			}
			err := jugCorrect(s, k)
			if err != nil {
				return nil, err
			}
			if head != k { //普通字符串处理
				temp := wordStr[head:k]
				head = k
				tempWord.Name = temp
				tempWord.IsPlaceholder = false
				tempWord.Position = notPlaceholder
				head = k
				words = append(words, tempWord)
				continue
			}
		}
		if fFlag && (v == 's' || v == 'S' || v == 'I' || v == 'L') { //占位符处理
			temp := wordStr[head : k+1]
			head = k + 1
			tempWord.Name = temp
			tempWord.IsPlaceholder = true
			position++
			if strings.Contains(temp, "*") { //特殊占位符，位置偏移
				if strings.Contains(temp, "$*") {
				} else {
					position++
				}
			}
			tempWord.Position = position
			words = append(words, tempWord)
			fFlag = false
			continue
		}

	}
	if head != len(wordStr) {
		temp := wordStr[head:]
		tempWord.Name = temp
		tempWord.IsPlaceholder = false
		tempWord.Position = notPlaceholder
		words = append(words, tempWord)
	}
	return words, nil
}

// turnNum RegEx turn to string that turn to Int
func turnNum(str string, p string) (int, error) { //将字符串中的数字转换为相应类型
	reg := regexp.MustCompile(p)
	resultStr := reg.FindAllString(str, -1)
	ss := resultStr[0]
	s := ss[1 : len(ss)-1]
	i, err := strconv.ParseInt(s, 0, 32)
	if err != nil {
		return 0, err
	}
	return int(i), nil
}

// if number
func isNum(arg interface{}) bool { //判断是否是int型
	switch arg.(type) {
	case *DInt:
		return true
	case int:
		return true
	default:
		return false
	}

}

func isFloat(arg interface{}) bool { //判断是否是float型
	switch arg.(type) {
	case *DDecimal:
		return true
	case float64:
		return true
	default:
		return false
	}
}

//ChangeWords use to change 'ps' of word
func ChangeWords(words []Word, index int, spacing int) { //改变占位符对应参数的位置
	for i := 0; i < len(words); i++ {
		if (words[i].Position > index) && words[i].IsPlaceholder {
			words[i].Position += spacing
		}
	}

}
func isNumber(args Datums, num int) (string, bool) { //如果格式化参数是int或float，把int或float转为string
	if isFloat(args[num]) {
		return args[num].String(), true
	}
	if isNum(args[num]) {
		return strconv.Itoa(int(MustBeDInt(args[num]))), true
	}
	return "", false
}
func isNumberWithLen(args Datums, argNum int) (string, int, bool) { //如果格式化参数是int或float，把int或float转为string，以及长度
	if isFloat(args[argNum]) {
		temStr := args[argNum].String()
		length := len(temStr)
		return temStr, length, true
	}
	if isNum(args[argNum]) {
		temStr := strconv.Itoa(int(MustBeDInt(args[argNum])))
		length := len(temStr)
		return temStr, length, true
	}
	return "", 0, false
}

// JudgeType judging belong to which type
func JudgeType(ctx *EvalContext, words []Word, v Word, args Datums) (string, error) {
	isValidIndex := func(index int) error { //判断args[index]是否合法
		if index < 0 || index > (len(args)-1) {
			return errors.New("the appoint position isn't arg")
		}
		return nil
	}
	isValidIndexNum := func(index int) error { //判断args[index]是否合法/且为数字
		if err := isValidIndex(index); err != nil {
			return err
		}
		if !isNum(args[index]) {
			return errors.New("the appoint position isn't number")
		}
		return nil
	}
	ch := v.Name[:]

	switch ch[len(ch)-1] {
	case 's', 'S':
		if ch[1] == 's' || ch[1] == 'S' { //%s直接返回
			if (len(args) - 1) < v.Position { //参数与占位符不对应
				return "", errors.New("the number of parameter and format string don't match") //报参数错误
			}
			if temStr, ok := isNumber(args, v.Position); ok {
				return temStr, nil
			}

			return string(MustBeDString(args[v.Position])), nil
		}
		if '0' < ch[1] && ch[1] <= '9' { // 处理类似%1s类型
			if (len(args) - 1) < v.Position {
				return "", errors.New("the number of parameter and format string don't match") //报参数错误
			}
			if strings.Contains(ch, "%") && strings.Contains(ch, "s") { //拿到%1s中的1
				if num, err := turnNum(ch, regRule1); err == nil {
					if num >= 0 {
						if temStr, strLen, ok := isNumberWithLen(args, v.Position); ok {
							num -= strLen
							return productSpace(num, " ") + temStr, nil
						}
						num -= len(string(MustBeDString(args[v.Position])))
						return productSpace(num, " ") + string(MustBeDString(args[v.Position])), nil
					}
				}
			}
			// /处理%1$*2$s类型
			if strings.Contains(ch, "%") && strings.Contains(ch, "*") && strings.Contains(ch, "$") {
				if strings.Count(ch, "*") > 1 { //违法输入报错
					return "", fmt.Errorf("error at '%s'", "*")
				}
				if strings.Count(ch, "$") > 2 { //违法输入报错
					return "", fmt.Errorf("error at '%s'", "$")
				}
				if count, err := turnNum(ch, regRule2); err == nil { // //把占位符中数字取出,指定参数的位置
					if err := isValidIndex(count); err != nil {
						return "", err
					}
					if argNum, err := turnNum(ch, regRule3); err == nil { // space参数位置
						if err := isValidIndexNum(argNum); err != nil {
							return "", err
						}
						num, err := PerformCast(ctx, args[argNum], coltypes.Int64)
						if err != nil {
							return "", errors.New("space arg error")
						}
						resArgNum := int(MustBeDInt(num))
						if resArgNum < 0 { //左边对齐
							resArgNum = -resArgNum
							if temStr, strLen, ok := isNumberWithLen(args, count); ok {
								resArgNum -= strLen
								return temStr + productSpace(resArgNum, " "), nil
							}

							resArgNum -= len(string(MustBeDString(args[count])))
							ChangeWords(words, v.Position, count-v.Position)
							return string(MustBeDString(args[count])) + productSpace(resArgNum, " "), nil
						}
						if resArgNum > 0 { //右对齐
							if temStr, strLen, ok := isNumberWithLen(args, count); ok {
								resArgNum -= strLen
								return productSpace(resArgNum, " ") + temStr, nil
							}

							resArgNum -= len(string(MustBeDString(args[count])))
							ChangeWords(words, v.Position, count-v.Position)
							return productSpace(resArgNum, " ") + string(MustBeDString(args[count])), nil
						}
					}
				}
			}
			// 处理%3$类型
			if strings.Contains(ch, "%") && strings.Contains(ch, "$") {
				if strings.Count(ch, "$") > 1 {
					return "", fmt.Errorf("error at '%s'", "$")
				}
				if count, err := turnNum(ch, regRule2); err == nil {
					if err := isValidIndex(count); err != nil {
						return "", err
					}
					if temStr, ok := isNumber(args, count); ok { // int or float change into string
						return temStr, nil
					}
					return string(MustBeDString(args[count])), nil
				}
			}
		}
		if ch[1] == '-' { // 处理%-1s类型
			if (len(args) - 1) < v.Position {
				return "", errors.New("the number of parameter and format string don't match") //报参数错误
			}
			if '0' <= ch[2] && ch[2] <= '9' {
				if strings.Contains(ch, "%") && strings.Contains(ch, "s") {
					if num, err := turnNum(ch, regRule1); err == nil { //取出数字
						if num <= 0 {
							num = -num
							if temStr, strLen, ok := isNumberWithLen(args, v.Position); ok {
								num -= strLen
								return temStr + productSpace(num, " "), nil
							}
							num -= len(string(MustBeDString(args[v.Position])))
							return string(MustBeDString(args[v.Position])) + productSpace(num, " "), nil
						}
					}
				}
			}
		}
		if ch[1] == '*' { // /处理%*..s类情况
			// /处理%*2$s情况
			if (len(args) - 1) < v.Position {
				return "", errors.New("the number of parameter and format string don't match") //报参数错误
			}
			if strings.Count(ch, "*") > 1 { //违法输入处理
				return "", fmt.Errorf("error at '%s'", "*")
			}
			if strings.Count(ch, "$") > 1 { //违法输入处理
				return "", fmt.Errorf("error at '%s'", "$")
			}
			if '0' < ch[2] && ch[2] <= '9' {
				if strings.Contains(ch, "*") && strings.Contains(ch, "$") {
					if argNum, err := turnNum(ch, regRule3); err == nil { //取出字符串中数字
						if err := isValidIndexNum(argNum); err != nil {
							return "", err
						}
						num, err := PerformCast(ctx, args[argNum], coltypes.Int8)
						if err != nil {
							return "", errors.New("space arg error")
						}
						resArgNum := int(MustBeDInt(num))
						if (len(args) - 1) < argNum+1 {
							return "", errors.New("the number of parameter and format string don't match") //报参数错误
						}
						if resArgNum < 0 {
							resArgNum = -resArgNum
							if temStr, strLen, ok := isNumberWithLen(args, argNum+1); ok { //取出对应位置后一个参数
								resArgNum -= strLen
								return temStr + productSpace(resArgNum, " "), nil
							}
							resArgNum -= len(string(MustBeDString(args[argNum+1])))
							ChangeWords(words, v.Position, argNum+1-v.Position)
							return string(MustBeDString(args[argNum+1])) + productSpace(resArgNum, " "), nil
						}
						if resArgNum > 0 {
							if temStr, strLen, ok := isNumberWithLen(args, argNum+1); ok {
								resArgNum -= strLen
								return productSpace(resArgNum, " ") + temStr, nil
							}
							resArgNum -= len(string(MustBeDString(args[argNum+1])))
							ChangeWords(words, v.Position, argNum+1-v.Position)
							return productSpace(resArgNum, " ") + string(MustBeDString(args[argNum+1])), nil
						}
					}
				} else {
					return "", errors.New(" this should have '$' ")
				}
			}
			//处理%*s类型-默认前一个参数为宽度情况
			if len(ch) > 3 { //违法输入
				return "", errors.New("unrecognized conversion type identifier")
			}
			if err := isValidIndex(v.Position - 1); err != nil {
				return "", err
			}
			if !isNum(args[v.Position-1]) {
				return "", errors.New("the default parameters Previous it  isn't number")
			}
			if strings.Contains(ch, "$") {
				return "", fmt.Errorf("error at '%s'", "$")
			}
			num, err := PerformCast(ctx, args[v.Position-1], coltypes.Int64)
			if err != nil {
				return "", errors.New("space arg error")
			}
			resArgNum := int(MustBeDInt(num))
			if resArgNum > 0 {
				if temStr, strLen, ok := isNumberWithLen(args, v.Position); ok {
					resArgNum -= strLen
					return productSpace(resArgNum, " ") + temStr, nil
				}
				resArgNum -= len(string(MustBeDString(args[v.Position])))
				return productSpace(resArgNum, " ") + string(MustBeDString(args[v.Position])), nil

			}
			if resArgNum < 0 {
				resArgNum = -resArgNum
				if temStr, strLen, ok := isNumberWithLen(args, v.Position); ok {
					resArgNum -= strLen
					return temStr + productSpace(resArgNum, " "), nil
				}
				resArgNum -= len(string(MustBeDString(args[v.Position])))
				return string(MustBeDString(args[v.Position])) + productSpace(resArgNum, " "), nil
			}

		}
		if ch[1] == '-' { // 处理左对齐%-*s  默认参数情况
			if (len(args) - 1) < v.Position {
				return "", errors.New("the number of parameter and format string don't match") //报参数错误
			}
			if strings.Count(ch, "*") > 1 {
				return "", fmt.Errorf("error at '%s'", "*")
			}
			if ch[2] == '*' {
				num, err := PerformCast(ctx, args[v.Position-1], coltypes.Int8)
				if err != nil {
					return "", errors.New("space arg error")
				}
				resArgNum := int(MustBeDInt(num))
				if resArgNum < 0 {
					resArgNum = -resArgNum
					if temStr, strLen, ok := isNumberWithLen(args, v.Position); ok {
						resArgNum -= strLen
						return temStr + productSpace(resArgNum, " "), nil
					}

					resArgNum -= len(string(MustBeDString(args[v.Position])))
					return string(MustBeDString(args[v.Position])) + productSpace(resArgNum, " "), nil
				}
				if resArgNum > 0 {

					if temStr, strLen, ok := isNumberWithLen(args, v.Position); ok {
						resArgNum -= strLen
						return temStr + productSpace(resArgNum, " "), nil
					}
					resArgNum -= len(string(MustBeDString(args[v.Position])))
					return string(MustBeDString(args[v.Position])) + productSpace(resArgNum, " "), nil
				}
			}
		}
		return "", errors.New("input character and special character are mismatched")
	case 'I':
		if (len(args) - 1) < v.Position { //%I类型处理
			return "", errors.New("the number of parameter and format string don't match") //报参数错误
		}
		argStr := string(MustBeDString(args[v.Position])) //空字符加“ ”
		if argStr == "" {
			argStr = "\"" + argStr + "\""
			return argStr, nil
		}
		for _, v := range argStr {
			if 'A' <= v && v <= 'Z' { //带有大写字母加“ ”
				argStr = "\"" + argStr + "\""
				return argStr, nil
			}
		}
		return string(MustBeDString(args[v.Position])), nil
	case 'L':
		if (len(args) - 1) < v.Position { //处理%L情况
			return "", errors.New("the number of parameter and format string don't match") //报参数错误
		}
		argStr := string(MustBeDString(args[v.Position]))

		if strings.Contains(argStr, "'") { //特殊处理
			argStr = strings.ReplaceAll(argStr, "'", "''")
		}
		if strings.Contains(argStr, "\\") {
			argStr = strings.ReplaceAll(argStr, "\\", "\\\\")
			argStr = "E" + "'" + argStr + "'"
			return argStr, nil
		}
		argStr = "'" + argStr + "'"
		return argStr, nil
	default:
		return "null", errors.New("input format error")
	}

}
