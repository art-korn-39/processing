package holds

import (
	"app/currency"
	"sort"
	"time"
)

var data []Hold

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
		data,
		func(i int, j int) bool {
			return data[i].DateStart.After(data[j].DateStart)
		},
	)
}

func FindHoldForOperation(balance_currency currency.Currency, Transaction_completed_at time.Time) (*Hold, bool) {

	for _, h := range data {

		if h.Currency == balance_currency && h.DateStart.Before(Transaction_completed_at) {
			return &h, true
		}

	}

	return nil, false
}
