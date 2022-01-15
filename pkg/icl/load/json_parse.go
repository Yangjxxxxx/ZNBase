package load

import (
	"encoding/json"
	"strconv"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

//ParseJSONRecord json to map
func ParseJSONRecord(jsonStr string) (map[string]interface{}, error) {
	tempRecordMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &tempRecordMap)
	if err != nil {
		return nil, err
	}
	return tempRecordMap, nil
}

//SortByFieldOrder 根据表字段返回对应的values
func SortByFieldOrder(
	records map[string]interface{}, cols []sqlbase.ColumnDescriptor,
) ([]string, error) {
	record := make([]string, len(cols))
	for i := 0; i < len(cols); i++ {
		col := cols[i]
		str, err := parseInterfaceToString(records[col.Name])
		if err != nil {
			return nil, err
		}
		record[i] = str
	}
	return record, nil
}

//JSONToRecords 将json 字符串转换为表列对应的[]string
func JSONToRecords(jsonStr string, cols []sqlbase.ColumnDescriptor) ([]string, error) {
	tempRecord, err := ParseJSONRecord(jsonStr)
	if err != nil {
		return nil, err
	}
	return SortByFieldOrder(tempRecord, cols)
}

//JSONToRecords 将json 字符串转换为表列对应的[]string
func parseInterfaceToString(jsonStr interface{}) (string, error) {
	if jsonStr == nil {
		return "", nil
	}
	switch jsonStr.(type) {
	case int:
		return strconv.Itoa(jsonStr.(int)), nil
	case int64:
		return strconv.FormatInt(jsonStr.(int64), 10), nil
	case byte:
		return string(jsonStr.(byte)), nil
	case string:
		return jsonStr.(string), nil
	case bool:
		return strconv.FormatBool(jsonStr.(bool)), nil
	case float32:
		return strconv.FormatFloat(float64(jsonStr.(float32)), 'E', -1, 64), nil
	case float64:
		return strconv.FormatFloat(jsonStr.(float64), 'E', -1, 64), nil
	default:
		return "", errors.Errorf("unsupported type conversion:%v to string", jsonStr)
	}
}
