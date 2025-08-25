package util

import (
	"strings"
	"time"
)

// Input formats
// "2024-05-22T12:45:41+0300"
// "2024-04-17 21:00:35 +0000 UTC"
// "2025/03/04 21:40:00"
func GetDateFromString(s string) time.Time {

	index := strings.Index(s, "+")
	if index != -1 {
		s = SubString(s, 0, index)
		s = strings.TrimSpace(s)
	}
	s = strings.ReplaceAll(s, "T", " ")
	s = strings.ReplaceAll(s, "/", "-")

	v, _ := time.Parse(time.DateTime, s)

	return v

}

// Input formats
// "10/1/2024 00:05"
// "9/26/24 9:53 pm"
func GetDateFromString2(s string) time.Time {

	var layout string
	if strings.Contains(s, "m") {
		s = strings.ToUpper(s)
		layout = "1/2/06 3:04 PM"
	} else {
		layout = "1/2/2006 15:04"
	}

	v, _ := time.Parse(layout, s)

	return v

}

func TruncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
