package tree

import "strconv"

// MakeDate Create date from year, month, day field.
func MakeDate(y int, m int, d int) (*DDate, error) {
	var getResult string
	var ctx ParseTimeContext
	yStr := strconv.Itoa(y)
	mStr := strconv.Itoa(m)
	dStr := strconv.Itoa(d)
	getResult = yStr + "-" + mStr + "-" + dStr

	getResult1, err := ParseDDate(ctx, getResult)
	if err != nil {
		return nil, err
	}
	return getResult1, nil
}
