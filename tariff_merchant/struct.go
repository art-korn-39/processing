package tariff_merchant

import (
	"app/currency"
	"app/util"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const RANGE_MAX = float64(1000000000000)

type Operation interface {
	Get_Operation_created_at() time.Time
	Get_Transaction_completed_at() time.Time
	Get_IsPerevodix() bool
	Get_Merchant_account_id() int
	Get_Operation_type() string
	Get_Crypto_network() string
	Get_Channel_currency() currency.Currency
	Get_Channel_amount() float64
	Get_IsDragonPay() bool
	Get_ClassicTariffDragonPay() bool
	Get_DragonPayProvider1c() string
	Get_Payment_type() string
}

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
	NetworkType    string
	Payment_type   string

	RR_days    int     //`xlsx:"32"`
	RR_percent float64 //`xlsx:"33"`

	Subdivision1C string //`xlsx:"36"`
	Provider1C    string //`xlsx:"37"`
	RatedAccount  string //`xlsx:"38"`
	Balance_id    int    //`xlsx:"39"`
	Balance_type  string //`xlsx:"40"`
	Id            int    //`xlsx:"46"`

	DateStartPS time.Time         //`xlsx:"12"`
	CurrencyBM  currency.Currency //`xlsx:"15"`
	CurrencyBP  currency.Currency //`xlsx:"16"`

	DateStart time.Time //`xlsx:"17"`
	RangeMIN  float64   //`xlsx:"20"`
	RangeMAX  float64   //`xlsx:"21"`
	Percent   float64   //`xlsx:"23"`
	Fix       float64   //`xlsx:"24"`
	Min       float64   //`xlsx:"25"`
	Max       float64   //`xlsx:"26"`

	CurrencyCommission      string
	AmountInChannelCurrency bool

	DK_is_zero bool
	DK_percent float64
	DK_fix     float64
	DK_min     float64
	DK_max     float64

	Formula    string
	DK_formula string
	Range      string

	IsTest bool
}

func (t *Tariff) StartingFill() {

	if t.Schema == "Crypto" {
		t.IsCrypto = true
	}

	if t.CurrencyCommission == "балансовая" {
		t.AmountInChannelCurrency = true
	}

	t.RangeMAX = util.TR(t.RangeMAX == 0, RANGE_MAX, t.RangeMAX).(float64) // ставим в конце, чтобы формуле не мешал

	// FORMULA
	s := make([]string, 0, 5)

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

	range_min := strconv.FormatFloat(t.RangeMIN, 'f', -1, 64)
	range_max := strconv.FormatFloat(t.RangeMAX, 'f', -1, 64)

	if util.BaseRound(t.RangeMIN) != 0 || util.BaseRound(t.RangeMAX) != RANGE_MAX {
		s = append(s, fmt.Sprintf("R.MIN %s R.MAX %s", range_min, range_max))
	}

	t.Formula = strings.Join(s, " ")

	// RANGE
	if util.BaseRound(t.RangeMAX) != RANGE_MAX {
		t.Range = fmt.Sprintf("%s - max", range_min)
	} else {
		t.Range = fmt.Sprintf("%s - %s", range_min, range_max)
	}

	// ДК
	if t.DK_percent+t.DK_fix+t.DK_min+t.DK_max > 0 {
		t.DK_formula = fmt.Sprintf("%s%%, +%s, min %s, max %s", fmt.Sprint(util.BaseRound(t.DK_percent*100)), fmt.Sprint(t.DK_fix), fmt.Sprint(t.DK_min), fmt.Sprint(t.DK_max))
	} else {
		t.DK_is_zero = true
		t.DK_formula = "без компенсации"
	}

	t.IsTest = t.Schema == "Тест"

}
