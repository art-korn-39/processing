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
	Get_Channel_currency() currency.Currency
	Get_Provider_currency() currency.Currency
	GetBool(string) bool
	GetTime(string) time.Time
	GetInt(string) int
	GetFloat(string) float64
	GetString(string) string
}

type Tariff struct {
	Balance_name          string `db:"balance_name"`
	Merchant_id           int    `db:"merchant_id"`
	Merchant              string `db:"merchant_name"`
	Merchant_account_name string `db:"merchant_account_name"`
	Merchant_account_id   int    `db:"merchant_account_id"`
	Balance_code          string `db:"balance_code"`
	Provider_name         string `db:"provider_name"`
	Company               string `db:"company"`

	Schema         string `db:"schema"`
	IsCrypto       bool
	Convertation   string `db:"convertation"`
	Operation_type string `db:"operation_type"`
	NetworkType    string `db:"network_type"`
	Payment_type   string `db:"payment_type"`

	RR_days    int     `db:"rr_days"`
	RR_percent float64 `db:"rr_percent"`

	Subdivision1C string `db:"subdivision1c"`
	Provider1C    string `db:"provider1c"`
	RatedAccount  string `db:"ratedaccount"`
	Balance_id    int    `db:"balance_id"`
	Balance_type  string `db:"balance_type"`
	Id            int    `db:"id"`

	DateStartPS time.Time         `db:"date_start_ps"`
	CurrencyBM  currency.Currency // не используется

	Balance_currency_str string `db:"balance_currency"`
	Balance_currency     currency.Currency

	DateStartMA  time.Time `db:"date_start_ma"`
	DateFinishMA time.Time `db:"date_finish_ma"`

	DateStart time.Time `db:"date_start"`
	RangeMIN  float64   `db:"range_min"`
	RangeMAX  float64   `db:"range_max"`
	Percent   float64   `db:"percent"`
	Fix       float64   `db:"fix"`
	Min       float64   `db:"min"`
	Max       float64   `db:"max"`

	CurrencyCommission      string `db:"currency_commission"`
	AmountInChannelCurrency bool

	DK_is_zero bool
	DK_percent float64 `db:"dk_percent"`
	DK_fix     float64 `db:"dk_fix"`
	DK_min     float64 `db:"dk_min"`
	DK_max     float64 `db:"dk_max"`

	Formula    string
	DK_formula string
	Range      string

	IsTest bool
	IsFile bool
}

func (t *Tariff) StartingFill() {

	t.Balance_currency = currency.New(t.Balance_currency_str)

	if t.Schema == "Crypto" {
		t.IsCrypto = true
	}

	if t.CurrencyCommission == "балансовая" {
		t.AmountInChannelCurrency = true
	}

	t.Percent = t.Percent / 100
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
