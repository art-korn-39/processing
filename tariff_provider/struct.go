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
	Get_Transaction_completed_at() time.Time
	Get_Merchant_name() string
	Get_Merchant_account_name() string
	Get_Legal_entity() int
	Get_Operation_group() string
	Get_Payment_method() string
	Get_Payment_type() string // это payment_method_type в файле
	Get_Region() string
	Get_Project() string
	Get_Business_type() string
	Get_Channel_currency() currency.Currency
	Get_Channel_amount() float64
	Get_Traffic_type() string
	Get_Account_bank_name() string
}

type Tariff struct {
	ID_revise             string
	Provider              string
	Provider_name         string
	Organization          string
	DateStart             time.Time
	Merchant_name         string
	Merchant_account_name string
	Merchant_legal_entity int
	Payment_method        string
	Payment_method_type   string
	Region                string
	ChannelCurrency       currency.Currency
	Project               string
	Business_type         string
	Operation_group       string
	Traffic_type          string
	Account_bank_name     string

	Range_turnouver_min float64
	Range_turnouver_max float64
	Range_amount_min    float64
	Range_amount_max    float64
	Percent             float64
	Fix                 float64
	Min                 float64
	Max                 float64

	CountUsefulFields int
	Formula           string
	Range             string
}

func (t *Tariff) StartingFill() {

	if t.Merchant_legal_entity == -1 {
		t.Merchant_legal_entity = 0
	}

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
	// if t.Operation_type != "" {
	// 	t.CountUsefulFields++
	// }
	if t.Merchant_name != "" {
		t.CountUsefulFields++
	}
	if t.Merchant_account_name != "" {
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
}
