package processing_provider

import (
	"app/countries"
	"app/currency"
	"app/dragonpay"
	"app/logs"
	"app/merchants"
	"app/provider_balances"
	"app/provider_registry"
	"app/tariff_provider"
	"app/teams_tradex"
	"app/util"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Operation struct {
	mu sync.Mutex

	Operation_id   int `db:"operation_id"`
	Transaction_id int `db:"transaction_id"`

	Document_date time.Time
	Document_id   int

	Transaction_completed_at time.Time
	Operation_created_at     time.Time `db:"operation_created_at"`
	Transaction_created_at   time.Time

	Merchant_id         int    `db:"merchant_id"`
	Merchant_account_id int    `db:"merchant_account_id"`
	Company_id          int    `db:"company_id"`
	Provider_id         int    `db:"provider_id"`
	Tariff_condition_id int    `db:"tariff_id"`
	Provider_payment_id string `db:"provider_payment_id"`
	Endpoint_id         string `db:"endpoint_id"`
	Contract_id         int    `db:"contract_id"`

	//Provider_base_name    string
	Provider_name         string `db:"provider_name"`
	Merchant_name         string `db:"merchant_name"`
	Merchant_account_name string `db:"merchant_account_name"`
	Account_bank_name     string `db:"account_bank_name"`
	Business_type         string `db:"business_type"`
	Country_code2         string `db:"country"`
	Region                string //`db:"region"`
	RRN                   string
	Payment_id            string
	Balance_type          string

	Project_name      string `db:"project_name"`
	Project_id        int    `db:"project_id"`
	Project_url       string
	Payment_type      string `db:"payment_type"`
	Payment_type_id   int    `db:"payment_type_id"`
	Payment_method_id int    `db:"payment_method_id"`
	Payment_method    string `db:"payment_method"`

	Operation_type    string `db:"operation_type"`
	Operation_type_id int    `db:"operation_type_id"`
	Operation_group   string
	Count_operations  int `db:"count_operations"`

	Provider_amount       float64 `db:"provider_amount"`
	Provider_currency_str string  `db:"provider_currency"`
	Provider_currency     currency.Currency

	Channel_amount       float64 `db:"channel_amount"`
	Channel_currency_str string  `db:"channel_currency"`
	Channel_currency     currency.Currency

	Surcharge_amount       float64 `db:"surcharge_amount"`
	Surcharge_currency_str string  `db:"surcharge_currency"`
	Surcharge_currency     currency.Currency

	Currency_str string `db:"currency"`
	Currency     currency.Currency

	Balance_amount   float64           // сумма в валюте баланса
	Balance_currency currency.Currency // валюта баланса

	BR_balance_currency       float64
	Extra_BR_balance_currency float64
	Operation_actual_amount   float64

	Verification string
	IsDragonPay  bool
	IsKessPay    bool
	IsPerevodix  bool
	IsTradex     bool

	//Crypto_network string
	//Provider1c     string

	CompensationBR float64

	ProviderOperation  *provider_registry.Operation
	Tariff             *tariff_provider.Tariff
	Extra_tariff       *tariff_provider.Tariff
	DragonpayOperation *dragonpay.Operation
	//Hold              *holds.Hold
	Country         countries.Country
	ProviderBalance *provider_balances.Balance
	Merchant        *merchants.Merchant

	Legal_entity_id int `db:"legal_entity_id"`
}

func (o *Operation) StartingFill() {

	if o.Transaction_completed_at.IsZero() {
		o.Transaction_completed_at = o.Operation_created_at
	}

	o.Document_date = util.TruncateToDay(o.Transaction_completed_at)

	o.IsDragonPay = strings.Contains(strings.ToLower(o.Provider_name), "dragonpay")
	o.IsKessPay = strings.Contains(strings.ToLower(o.Provider_name), "kesspay")
	o.IsPerevodix = strings.Contains(strings.ToLower(o.Provider_name), "perevodix")

	o.Provider_currency = currency.New(o.Provider_currency_str)
	o.Channel_currency = currency.New(o.Channel_currency_str)
	o.Surcharge_currency = currency.New(o.Surcharge_currency_str)
	o.Currency = currency.New(o.Currency_str)
	//o.Msc_currency = currency.New(o.Msc_currency_str)
	//o.Fee_currency = currency.New(o.Fee_currency_str)

	o.Provider_amount = util.TR(o.Provider_currency.Exponent, o.Provider_amount, o.Provider_amount/100).(float64)
	o.Channel_amount = util.TR(o.Channel_currency.Exponent, o.Channel_amount, o.Channel_amount/100).(float64)
	o.Surcharge_amount = util.TR(o.Surcharge_currency.Exponent, o.Surcharge_amount, o.Surcharge_amount/100).(float64)
	o.Operation_actual_amount = util.TR(o.Channel_currency.Exponent, o.Operation_actual_amount, o.Operation_actual_amount/100).(float64)
	//o.Msc_amount = util.TR(o.Msc_currency.Exponent, o.Msc_amount, o.Msc_amount/100).(float64)
	//o.Actual_amount = util.TR(o.Channel_currency.Exponent, o.Actual_amount, o.Actual_amount/100).(float64)
	//o.Fee_amount = util.TR(o.Fee_currency.Exponent, o.Fee_amount, o.Fee_amount/100).(float64)

	if o.Operation_type == "" {
		if o.Operation_type_id == 3 {
			o.Operation_type = "sale"
		} else if o.Operation_type_id == 2 {
			o.Operation_type = "capture"
		} else if o.Operation_type_id == 6 {
			o.Operation_type = "recurring"
		} else if o.Operation_type_id == 5 {
			o.Operation_type = "refund"
		} else if o.Operation_type_id == 11 {
			o.Operation_type = "payout"
		}
	}

	if o.Operation_group == "" && o.Operation_type != "" {
		if o.Operation_type == "payout" {
			o.Operation_group = "OUT"
		} else if o.Operation_type == "refund" {
			o.Operation_group = "REFUND"
		} else {
			o.Operation_group = "IN"
		}
	}

	if o.Operation_group == "OUT" {
		o.Balance_type = "OUT"
	} else {
		o.Balance_type = "IN"
	}

}

func (o *Operation) SetCountry() {
	o.Country = countries.GetCountry(o.Country_code2, o.Currency.Name)
}

func (o *Operation) SetPaymentType() {
	//o.Payment_type = dragonpay.GetProvider1C()
}

// func (o *Operation) SetBalanceCurrency() {

// 	if o.ProviderOperation != nil {
// 		o.Balance_currency = o.ProviderOperation.Provider_currency
// 	} else if o.ProviderBalance != nil && o.ProviderBalance.Convertation == "Курс наш (в колбэках)" {
// 		o.Balance_currency = o.ProviderBalance.Balance_currency
// 	} else {
// 		o.Balance_currency = o.Channel_currency
// 	}

// }

const (
	CNV_NO_CONVERT int = 1
	CNV_REESTR     int = 2
	CNV_CALLBACK   int = 3
)

func (o *Operation) SetBalanceAmount() {

	if o.ProviderOperation != nil && o.ProviderBalance != nil {

		if o.ProviderBalance.Convertation_id == CNV_REESTR || o.Channel_currency != o.Balance_currency {
			o.Balance_amount = o.ProviderOperation.Amount
		} else {
			o.Balance_amount = o.Channel_amount
		}

	} else if o.ProviderBalance != nil && o.ProviderBalance.Convertation_id == CNV_CALLBACK {

		if o.Provider_amount != 0 {
			o.Balance_amount = o.Provider_amount
		} else {
			o.Balance_amount = o.Channel_amount
		}

	} else {

		o.Balance_amount = o.Channel_amount

	}

}

func (o *Operation) SetBRAmount() {

	if o.IsPerevodix {
		detailed, ok := storage.Detailed[strconv.Itoa(o.Operation_id)]
		if ok {
			o.BR_balance_currency = detailed.BR_balance_currency
		}
		return
	}

	t := o.Tariff

	if t == nil {
		return
	}

	// BR
	commission := o.Balance_amount*t.Percent + t.Fix

	if t.Min != 0 && commission < t.Min {
		commission = t.Min
	} else if t.Max != 0 && commission > t.Max {
		commission = t.Max
	}

	// ОКРУГЛЕНИЕ
	if o.Channel_currency.Exponent {
		o.BR_balance_currency = util.Round(commission, 0)
	} else {
		o.BR_balance_currency = util.Round(commission, 4)
	}

}

func (o *Operation) SetExtraBRAmount() {

	t := o.Extra_tariff

	if t == nil {
		return
	}

	// BR
	commission := o.Balance_amount*t.Percent + t.Fix
	if o.IsKessPay {
		commission = (o.Balance_amount-o.BR_balance_currency)*t.Percent + t.Fix
	}

	if t.Min != 0 && commission < t.Min {
		commission = t.Min
	} else if t.Max != 0 && commission > t.Max {
		commission = t.Max
	}

	// ОКРУГЛЕНИЕ
	if o.Channel_currency.Exponent {
		o.Extra_BR_balance_currency = util.Round(commission, 0)
	} else {
		o.Extra_BR_balance_currency = util.Round(commission, 4)
	}

}

func (o *Operation) SetVerification() {

	if o.ProviderBalance == nil {
		o.Verification = VRF_NO_BALANCE
	} else if o.ProviderBalance.Convertation_id == CNV_REESTR && o.ProviderOperation == nil {
		o.Verification = VRF_NO_IN_REG
	} else if o.Tariff == nil {
		o.Verification = VRF_NO_TARIFF
	} else {
		o.Verification = VRF_OK
	}

}

const (
	VRF_OK         = "ОК"
	VRF_NO_BALANCE = "Не найден баланс"
	VRF_NO_TARIFF  = "Не найден тариф"
	VRF_NO_IN_REG  = "Нет в реестре"
)

func (op *Operation) Get_Channel_currency() currency.Currency {
	return op.Channel_currency
}

func (op *Operation) Get_Balance_currency() currency.Currency {
	return op.Balance_currency
}

func (op *Operation) GetBool(name string) bool {
	var result bool
	switch name {
	case "IsDragonPay":
		result = op.IsDragonPay
	default:
		logs.Add(logs.ERROR, "неизвестное поле bool: ", name)
	}
	return result
}
func (op *Operation) GetTime(name string) time.Time {
	var result time.Time
	switch name {
	case "Operation_created_at":
		result = op.Operation_created_at
	case "Transaction_completed_at":
		result = op.Transaction_completed_at
	case "Transaction_created_at":
		result = op.Transaction_created_at
	default:
		logs.Add(logs.ERROR, "неизвестное поле time: ", name)
	}
	return result
}
func (op *Operation) GetInt(name string) int {
	var result int
	switch name {
	case "Merchant_account_id":
		result = op.Merchant_account_id
	case "Provider_id":
		result = op.Provider_id
	case "Legal_entity_id":
		result = op.Legal_entity_id
	default:
		logs.Add(logs.ERROR, "неизвестное поле int: ", name)
	}
	return result
}

func (op *Operation) GetFloat(name string) float64 {
	var result float64
	switch name {
	case "Channel_amount":
		result = op.Channel_amount
	default:
		logs.Add(logs.ERROR, "неизвестное поле float: ", name)
	}
	return result
}

func (op *Operation) GetString(name string) string {
	var result string
	switch name {
	case "Balance_guid":
		if op.IsTradex {
			if op.ProviderOperation != nil {
				team := op.ProviderOperation.Team
				team_ref, ok := teams_tradex.GetTeamByName(team)
				if ok {
					return team_ref.Provider_balance_guid
				}
			}
			return ""
		} else if op.ProviderBalance != nil {
			result = op.ProviderBalance.GUID
		}
	case "Extra_balance_guid":
		if op.ProviderBalance != nil {
			result = op.ProviderBalance.Extra_balance_guid
		}
	case "Endpoint_id":
		if op.Endpoint_id != "" {
			result = op.Endpoint_id
		} else if op.DragonpayOperation != nil {
			result = op.DragonpayOperation.Endpoint_id
		} else {
			result = ""
		}
	case "Balance_type":
		result = op.Balance_type
	case "Merchant_name":
		result = op.Merchant_name
	case "Merchant_account_name":
		result = op.Merchant_account_name
	case "Operation_group":
		result = op.Operation_group
	case "Payment_method":
		result = op.Payment_method
	case "Payment_type":
		result = op.Payment_type
	case "Region":
		result = op.Country.Region
	case "Project_name":
		result = op.Project_name
	case "Business_type":
		result = op.Business_type
	case "Traffic_type":
		result = ""
	case "Account_bank_name":
		result = op.Account_bank_name
	case "Team":
		if op.ProviderOperation != nil {
			result = op.ProviderOperation.Team
		}
	default:
		logs.Add(logs.ERROR, "неизвестное поле string: ", name)
	}
	return result
}
