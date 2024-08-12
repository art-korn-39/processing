package util

import (
	"strings"
	"time"
)

// Input formats
// "2024-05-22T12:45:41+0300"
// "2024-04-17 21:00:35 +0000 UTC"
func GetDateFromString(s string) time.Time {

	index := strings.Index(s, "+")
	if index != -1 {
		s = SubString(s, 0, index)
		s = strings.TrimSpace(s)
	}
	s = strings.ReplaceAll(s, "T", " ")

	v, _ := time.Parse(time.DateTime, s)

	return v

}

func TruncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
