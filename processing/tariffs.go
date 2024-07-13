package processing

import (
	"app/util"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Tariff struct {
	Balance_name          string //`xlsx:"1"`
	Merchant              string //`xlsx:"5"`
	Merchant_account_name string //`xlsx:"6"`
	Merchant_account_id   int    //`xlsx:"7"`
	Balance_code          string //`xlsx:"8"`
	Provider              string //`xlsx:"9"`

	Schema         string //`xlsx:"18"`
	IsCrypto       bool
	Convertation   string //`xlsx:"19"`
	Operation_type string //`xlsx:"22"`

	PP_days    int     //`xlsx:"32"`
	PP_percent float64 //`xlsx:"33"`

	Subdivision1C string //`xlsx:"36"`
	Provider1C    string //`xlsx:"37"`
	RatedAccount  string //`xlsx:"38"`
	Balance_id    int    //`xlsx:"39"`
	Balance_type  string //`xlsx:"40"`
	id            int    //`xlsx:"46"`

	DateStartPS time.Time //`xlsx:"12"`
	CurrencyBM  Currency  //`xlsx:"15"`
	CurrencyBP  Currency  //`xlsx:"16"`
	DateStart   time.Time //`xlsx:"17"`
	RangeMIN    float64   //`xlsx:"20"`
	RangeMAX    float64   //`xlsx:"21"`
	Percent     float64   //`xlsx:"23"`
	Fix         float64   //`xlsx:"24"`
	Min         float64   //`xlsx:"25"`
	Max         float64   //`xlsx:"26"`

	Formula string
}

func (t *Tariff) SetFormula() {

	s := []string{}

	if t.Percent != 0 {
		s = append(s, fmt.Sprint("PCT ", util.BaseRound(t.Percent*100)))
	}
	if t.Fix != 0 {
		s = append(s, fmt.Sprint("FIX ", util.BaseRound(t.Fix)))
	}
	if t.Min != 0 {
		s = append(s, fmt.Sprint("MIN ", t.Min))
	}
	if t.Max != 0 {
		s = append(s, fmt.Sprint("MAX ", t.Max))
	}

	if t.RangeMIN != 0 || util.BaseRound(t.RangeMAX) != RANGE_MAX {
		s = append(s, fmt.Sprint("RA ", t.RangeMIN, " - ", t.RangeMAX))
	}

	t.Formula = strings.Join(s, " ")

}

func SortTariffs() {
	sort.Slice(
		storage.Tariffs,
		func(i int, j int) bool {
			return storage.Tariffs[i].DateStart.After(storage.Tariffs[j].DateStart)
		},
	)
}
