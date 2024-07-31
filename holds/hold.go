package holds

import (
	"app/currency"
	"sort"
	"time"
)

var Data []Hold

type Hold struct {
	Schema    string
	Currency  currency.Currency
	MA_id     int
	MA_name   string
	DateStart time.Time
	Percent   float64
	Days      int
}

func Sort() {
	sort.Slice(
		Data,
		func(i int, j int) bool {
			return Data[i].DateStart.After(Data[j].DateStart)
		},
	)
}
