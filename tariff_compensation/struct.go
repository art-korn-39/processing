package tariff_compensation

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
	GetBool(string) bool
	GetTime(string) time.Time
	GetInt(string) int
	GetFloat(string) float64
	GetString(string) string
}

type Tariff struct {
	Guid string `db:"guid"`
	Code string `db:"code"`
	Name string `db:"name"`

	ComissionType string `db:"comission_type"`
	Tariff_type   string `db:"tariff_type"`

	DateStart  time.Time `db:"date_start"`
	DateFinish time.Time `db:"date_finish"`

	Affiliate_name string `db:"affiliate_name"`
	Affiliate_guid string `db:"affiliate_guid"`
	Merchant_id    int    `db:"merchant_id"`
	Provider_id    int    `db:"provider_id"`
	Currency_str   string `db:"currency"`
	Payment_type   string `db:"payment_type"`
	Traffic_type   string `db:"traffic_type"`

	Provider_1c_guid string `db:"provider_1c_guid"`
	Provider_1c_name string `db:"provider_1c_name"`
	Operation_group  string `db:"opeation_group"`

	Provider_balance_guid string `db:"provider_balance_guid"`
	Provider_balance_name string `db:"provider_balance_name"`

	Merchant_account_name string `db:"merchant_account_name"`
	Merchant_account_id   int    `db:"merchant_account_id"`

	Percent      float64 `db:"percent"`
	Fix          float64 `db:"fix"`
	Min          float64 `db:"min"`
	Max          float64 `db:"max"`
	RangeMIN     float64 `db:"range_min"`
	RangeMAX     float64 `db:"range_max"`
	Turnover_max float64 `db:"turnover_max"`
	Turnover_min float64 `db:"turnover_min"`

	Currency currency.Currency

	Is_referal bool
	DK_is_zero bool
	Formula    string
	Range      string
}

func (t *Tariff) StartingFill() {

	if t.Provider_balance_guid == "00000000-0000-0000-0000-000000000000" {
		t.Provider_balance_guid = ""
	}

	t.Is_referal = t.Tariff_type == "referal"

	t.Currency = currency.New(t.Currency_str)

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

}
