package sqlutil

import "strings"

const (
	// SingleQuotation means convert a string into sql statement within single quotation
	SingleQuotation = 0

	// DoubleQuotation means convert a string into sql statement within double quotation
	DoubleQuotation = 1
)

// AddExtraMark 为字符串增加单/双引号
//	如：
//		v"iew ==> "v""iew"
//		k'iew ==> 'k''iew'
//      AAA   ==>  "AAA"
func AddExtraMark(name string, signal uint8) string {
	var build strings.Builder
	var sig uint8
	var sigStr string
	// 字符串两端的双引号只在包含特殊符号时加
	// 单引号什么情况都加
	var addQuotation = false
	// add quotation for uppercase
	var isLower = true
	start := 0

	switch signal {
	case SingleQuotation:
		sig = '\''
		sigStr = "'"
		addQuotation = true
	case DoubleQuotation:
		sig = '"'
		sigStr = "\""
	default:
	}
	for j := 0; j < len(name); j++ {
		if name[j] >= 'A' && name[j] <= 'Z' {
			isLower = false
		}
		if containSPMark(name[j]) {
			addQuotation = true
		}
		if name[j] == sig {
			build.WriteString(name[start : j+1])
			build.WriteString(sigStr)
			start = j + 1
		}
		if j == len(name)-1 {
			build.WriteString(name[start : j+1])
		}
	}
	name = build.String()
	if addQuotation || !isLower {
		name = sigStr + name + sigStr
	}
	return name
}

func containSPMark(ch byte) bool {
	switch ch {
	case '$':
		return true
	case '"':
		return true
	case '\'':
		return true
	case '.':
		return true
	case '!':
		return true
	case '=': // !=
		return true
	case '~': // !~
		return true
	case '*': // !~*
		return true
	case '?':
		return true
	case '|': // ?|
		return true
	case '&': // ?&
		return true
	case '<':
		return true
	case '>': // <>
		return true
	case '@': // <@
		return true
	case ':':
		return true
	case '/':
		return true
	case '-':
		return true
	case '#':
		return true
	default:
		return false
	}
}

// SolveRownumInCreateView 解决create view的时候as select rownum 二次解析错误，为rownum加回""
func SolveRownumInCreateView(query string) string {
	var result string
	ss := strings.Fields(query)
	for i, substr := range ss {
		if substr == "rownum" {
			temp := append(ss[:i], "\"rownum\"")
			temp = append(temp, ss[i+1:]...)
			ss = temp
		}
		if substr == "rownum," {
			temp := append(ss[:i], "\"rownum\",")
			temp = append(temp, ss[i+1:]...)
			ss = temp
		}
		if substr == "(rownum" {
			temp := append(ss[:i], "(\"rownum\"")
			temp = append(temp, ss[i+1:]...)
			ss = temp
		}
		if substr == "rownum)" {
			temp := append(ss[:i], "\"rownum\")")
			temp = append(temp, ss[i+1:]...)
			ss = temp
		}
	}
	result = strings.Join(ss, " ")
	return result
}
