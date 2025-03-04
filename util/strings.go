package util

import "golang.org/x/text/encoding/charmap"

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

func IsString1251(s string) string {

	if s == "" {
		return ""
	}

	encoder := charmap.Windows1251.NewEncoder()
	_, err := encoder.String(s)

	if err != nil {
		return ""
	}

	return s

}
