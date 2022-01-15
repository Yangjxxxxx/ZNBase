package util

// Min returns the smaller of a or b.
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
