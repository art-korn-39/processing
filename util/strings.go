package util

// С 0 (вкл.) до last (искл.)
func SubString(s string, first int, last int) string {

	runes := []rune(s)
	length := len(runes)

	if length <= last {
		last = length
	}

	if last == 0 {
		last = length
	}

	return string(runes[first:last])

}
