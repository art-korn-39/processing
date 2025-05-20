package tariff_provider

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
	Get_Balance_currency() currency.Currency
	GetTime(string) time.Time
	GetInt(string) int
	GetFloat(string) float64
	GetString(string) string
	GetBool(string) bool
}

type Tariff struct {
	// ID_revise             string
	// Provider              string
	// Provider_name         string
	// Organization          string
	GUID                       string    `db:"guid"`
	Provider_balance_guid      string    `db:"provider_balance_guid"`
	Provider_balance_name      string    `db:"provider_balance_name"`
	DateStart                  time.Time `db:"date_start"`
	Merchant_name              string    `db:"merchant_name"`
	Merchant_account_name      string    `db:"merchant_account_name"`
	Merchant_account_id        int       `db:"merchant_account_id"`
	Merchant_legal_entity      int       `db:"merchant_legal_entity"`
	Payment_method             string    `db:"payment_method"`
	Payment_method_type        string    `db:"payment_method_type"`
	Region                     string    `db:"region"`
	ChannelCurrency            currency.Currency
	ChannelCurrency_str        string `db:"channel_currency"`
	Project                    string `db:"project_name"`
	Business_type              string `db:"business_type"`
	Operation_group            string `db:"operation_group"`
	Traffic_type               string `db:"traffic_type"`
	Account_bank_name          string `db:"account_bank_name"`
	Use_transaction_created_at bool   `db:"use_transaction_created_at"`
	Search_string_ma           string `db:"search_string_ma"`
	Endpoint_id                string `db:"endpoint_id"`

	Range_turnouver_min float64 `db:"tariff_range_turnouver_min"`
	Range_turnouver_max float64 `db:"tariff_range_turnouver_max"`
	Range_amount_min    float64 `db:"tariff_range_amount_min"`
	Range_amount_max    float64 `db:"tariff_range_amount_max"`
	Percent             float64 `db:"percent"`
	Fix                 float64 `db:"fix"`
	Min                 float64 `db:"min"`
	Max                 float64 `db:"max"`

	CountUsefulFields int
	Formula           string
	Range             string
}

func (t *Tariff) StartingFill() {

	if t.Merchant_legal_entity == -1 {
		t.Merchant_legal_entity = 0
	}

	t.ChannelCurrency = currency.New(t.ChannelCurrency_str)

	t.Percent = t.Percent / 100

	t.Range_amount_max = util.TR(t.Range_amount_max == 0, RANGE_MAX, t.Range_amount_max).(float64) // ставим в конце, чтобы формуле не мешал

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

	range_min := strconv.FormatFloat(t.Range_amount_min, 'f', -1, 64)
	range_max := strconv.FormatFloat(t.Range_amount_max, 'f', -1, 64)

	if util.BaseRound(t.Range_amount_min) != 0 || util.BaseRound(t.Range_amount_max) != RANGE_MAX {
		s = append(s, fmt.Sprintf("R.MIN %s R.MAX %s", range_min, range_max))
	}

	t.Formula = strings.Join(s, " ")

	// RANGE
	if util.BaseRound(t.Range_amount_max) != RANGE_MAX {
		t.Range = fmt.Sprintf("%s - max", range_min)
	} else {
		t.Range = fmt.Sprintf("%s - %s", range_min, range_max)
	}

	t.SetCountUsefulFields()

}

func (t *Tariff) SetCountUsefulFields() {
	if t.Merchant_name != "" {
		t.CountUsefulFields++
	}
	// if t.Merchant_account_name != "" {
	// 	t.CountUsefulFields++
	// }
	if t.Merchant_account_id != 0 {
		t.CountUsefulFields++
	}
	if t.Merchant_legal_entity != 0 {
		t.CountUsefulFields++
	}
	if t.Payment_method != "" {
		t.CountUsefulFields++
	}
	if t.Payment_method_type != "" {
		t.CountUsefulFields++
	}
	if t.Region != "" {
		t.CountUsefulFields++
	}
	if t.ChannelCurrency.Name != "" {
		t.CountUsefulFields++
	}
	if t.Project != "" {
		t.CountUsefulFields++
	}
	if t.Business_type != "" {
		t.CountUsefulFields++
	}
	if t.Traffic_type != "" {
		t.CountUsefulFields++
	}
	if t.Account_bank_name != "" {
		t.CountUsefulFields++
	}
	if t.Endpoint_id != "" {
		t.CountUsefulFields++
	}
}
