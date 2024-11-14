package processing_provider

import (
	"app/countries"
	"app/currency"
	"app/holds"
	"app/logs"
	"app/provider"
	"app/tariff_provider"
	"app/util"
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

	Merchant_id         int    `db:"merchant_id"`
	Merchant_account_id int    `db:"merchant_account_id"`
	Company_id          int    `db:"company_id"`
	Provider_id         int    `db:"provider_id"`
	Tariff_condition_id int    `db:"tariff_id"`
	Provider_payment_id string `db:"provider_payment_id"`
	Endpoint_id         string `db:"endpoint_id"`
	//Balance_id          int    `db:"balance_id"`
	Contract_id int `db:"contract_id"`

	Provider_base_name    string
	Provider_name         string `db:"provider_name"`
	Merchant_name         string `db:"merchant_name"`
	Merchant_account_name string `db:"merchant_account_name"`
	Account_bank_name     string `db:"account_bank_name"`
	Business_type         string `db:"business_type"`
	Country_code2         string `db:"country"`
	Region                string //`db:"region"`

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

	Currency_str string `db:"currency"`
	Currency     currency.Currency

	Balance_amount   float64           // сумма в валюте баланса
	Balance_currency currency.Currency // валюта баланса

	BR_balance_currency float64

	Verification   string
	IsDragonPay    bool
	IsPerevodix    bool
	Crypto_network string
	Provider1c     string

	CompensationBR float64

	ProviderOperation *provider.Operation
	Tariff            *tariff_provider.Tariff
	Hold              *holds.Hold
	Country           countries.Country

	Legal_entity_id int `db:"legal_entity_id"`
}

func (o *Operation) StartingFill() {

	if o.Transaction_completed_at.IsZero() {
		o.Transaction_completed_at = o.Operation_created_at
	}

	o.Document_date = util.TruncateToDay(o.Transaction_completed_at)

	o.IsDragonPay = strings.Contains(strings.ToLower(o.Provider_name), "dragonpay")
	o.IsPerevodix = o.Merchant_id == 73162

	o.Provider_currency = currency.New(o.Provider_currency_str)
	o.Channel_currency = currency.New(o.Channel_currency_str)
	//o.Msc_currency = currency.New(o.Msc_currency_str)
	//o.Fee_currency = currency.New(o.Fee_currency_str)

	o.Provider_amount = util.TR(o.Provider_currency.Exponent, o.Provider_amount, o.Provider_amount/100).(float64)
	o.Channel_amount = util.TR(o.Channel_currency.Exponent, o.Channel_amount, o.Channel_amount/100).(float64)
	//o.Msc_amount = util.TR(o.Msc_currency.Exponent, o.Msc_amount, o.Msc_amount/100).(float64)
	//o.Actual_amount = util.TR(o.Channel_currency.Exponent, o.Actual_amount, o.Actual_amount/100).(float64)
	//o.Fee_amount = util.TR(o.Fee_currency.Exponent, o.Fee_amount, o.Fee_amount/100).(float64)

	o.Currency = currency.New(o.Currency_str)

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
		if o.Operation_type == "refund" {
			o.Operation_group = "REFUND"
		} else if o.Operation_type == "payout" {
			o.Operation_group = "OUT"
		} else {
			o.Operation_group = "IN"
		}
	}

	// o.Tariff_rate_fix = util.TR(o.Channel_currency.Exponent, o.Tariff_rate_fix, o.Tariff_rate_fix/100).(float64)
	// o.Tariff_rate_min = util.TR(o.Channel_currency.Exponent, o.Tariff_rate_min, o.Tariff_rate_min/100).(float64)
	// o.Tariff_rate_max = util.TR(o.Channel_currency.Exponent, o.Tariff_rate_max, o.Tariff_rate_max/100).(float64)

	// o.Tariff_bof = &tariff_merchant.Tariff{
	// 	Percent: o.Tariff_rate_percent,
	// 	Fix:     o.Tariff_rate_fix,
	// 	Min:     o.Tariff_rate_min,
	// 	Max:     o.Tariff_rate_max,
	// }
	// o.Tariff_bof.StartingFill()

}

func (o *Operation) SetCountry() {
	o.Country = countries.GetCountry(o.Country_code2, o.Currency.Name)
}

func (o *Operation) SetBalanceAmount() {

	//t := o.Tariff
	o.Balance_currency = o.Channel_currency //временно!!!

	//rate := float64(1)
	balance_amount := float64(0)

	// // у KGX может быть RUB-RUB, но комсу надо брать из операции провайдера
	// if o.Channel_currency == o.Balance_currency && t.Convertation != "KGX" {
	// 	balance_amount = o.Channel_amount
	// } else if t.Convertation == "Без конверта" {
	// 	balance_amount = o.Channel_amount
	// } else if t.Convertation == "Частичные выплаты" {
	// 	balance_amount = o.Channel_amount
	// } else if t.Convertation == "Колбек" {
	// 	balance_amount = o.Provider_amount
	// } else if t.Convertation == "Реестр" || t.Convertation == "KGX" {

	// 	// Поиск в мапе операций провайдера по ID
	// 	ProviderOperation, ok := provider.Registry.Get(o.Operation_id, o.Document_date, o.Channel_amount)
	// 	o.ProviderOperation = ProviderOperation
	// 	if ok {
	// 		balance_amount = ProviderOperation.Amount
	// 		rate = ProviderOperation.Rate

	// 		if t.Convertation == "KGX" {
	// 			o.Provider_name = ProviderOperation.Balance //!!!
	// 			o.Balance_currency = ProviderOperation.Provider_currency
	// 		}

	// 	} else {
	// 		// если не нашли операцию провайдера по ID, то подбираем курс и считаем через него
	// 		rate = FindRateForOperation(o)
	// 		if rate != 0 {
	// 			balance_amount = o.Channel_amount / rate
	// 		}
	// 	}

	// } else { // крипта скорее всего
	balance_amount = o.Channel_amount
	//}

	//o.Rate = util.TR(rate == 0, float64(1), rate).(float64)
	o.Balance_amount = balance_amount

}

func (o *Operation) SetSRAmount() {

	t := o.Tariff

	if t == nil {
		return
	}

	// BR
	commission := o.Channel_amount*t.Percent + t.Fix

	if t.Min != 0 && commission < t.Min {
		commission = t.Min
	} else if t.Max != 0 && commission > t.Max {
		commission = t.Max
	}

	// ОКРУГЛЕНИЕ
	if o.Channel_currency.Exponent {
		o.BR_balance_currency = util.Round(commission, 0)
	} else {
		o.BR_balance_currency = util.Round(commission, 2)
	}

}

func (o *Operation) SetCheckFee() {

	// if o.Fee_currency == o.Balance_currency {
	// 	o.CheckFee = util.BaseRound(o.Fee_amount - o.SR_balance_currency)
	// } else {
	// 	o.CheckFee = util.BaseRound(o.Fee_amount - o.SR_channel_currency)
	// }

}

func (o *Operation) SetVerification() {

	// var Converation string
	// var CurrencyBP currency.Currency

	// if o.Tariff != nil {
	// 	//Converation = o.Tariff.Convertation
	// 	//CurrencyBP = o.Tariff.CurrencyBP
	// 	if o.Tariff_bof != nil {
	// 		s1 := (o.Tariff.Percent + o.Tariff.Fix + o.Tariff.Min + o.Tariff.Max) //* 100
	// 		s2 := o.Tariff_bof.Percent + o.Tariff_bof.Fix + o.Tariff_bof.Min + o.Tariff_bof.Max
	// 		o.CheckRates = util.BaseRound(s1 - s2)
	// 	}
	// }

	// если реестр и валюты одинаковые, то вылетает "требует уточ. курса"
	// if o.Tariff == nil {
	// 	o.Verification = VRF_NO_TARIFF
	// } else if Converation == "Реестр" || Converation == "KGX" {
	// 	if o.Balance_amount == 0 {
	// 		o.Verification = VRF_NO_IN_REG // нет курса и операции провайдера
	// 	} else if o.ProviderOperation == nil {
	// 		o.Verification = VRF_CHECK_RATE // курс есть, операции еще нет в реестре факт
	// 	} else if o.Channel_amount != o.ProviderOperation.Channel_amount {
	// 		o.Verification = VRF_DIFF_CHAN_AMOUNT // channel amount разный в БОФ и реестре пров.
	// 	} else if o.CheckFee != 0 {
	// 		o.Verification = VRF_VALID_REG_FEE // есть в реестре факт, но БОФ криво посчитал
	// 	} else {
	// 		o.Verification = VRF_VALID_REG // всё ок
	// 	}
	//} else if o.CheckFee == 0 {
	//	o.Verification = VRF_OK
	// } else if o.Channel_currency != CurrencyBP && Converation != "Колбек" {
	// 	o.Verification = VRF_CHECK_CURRENCY
	// 	// } else if o.CheckRates != 0 {
	// 	// 	o.Verification = VRF_CHECK_TARIFF
	// 	//} else if Converation == "Частичные выплаты" && o.Channel_amount != o.Actual_amount {
	// 	//	o.Verification = VRF_PARTIAL_PAYMENTS
	// } else if o.IsDragonPay {
	// 	if o.Endpoint_id == "" {
	// 		o.Verification = VRF_ENDPOINT_DRAGONPAY
	// 	} else {
	// 		o.Verification = VRF_DRAGON_PAY
	// 	}
	// } else {
	// 	o.Verification = VRF_CHECK_BILLING
	// }

	// if o.Tariff != nil && o.IsPerevodix && o.Tariff.Convertation == "KGX" {

	// 	if kgx.GetDataLen() == 0 {
	// 		o.Verification_KGX = VRF_NO_DATA_PEREVODIX_KGX
	// 	} else if !kgx.LineContains(o.Provider_name, o.Operation_type, o.Payment_type, o.Balance_currency) {
	// 		o.Verification_KGX = VRF_NO_MAPPING_KGX_LIST
	// 	} else if o.Provider1c == "" {
	// 		o.Verification_KGX = VRF_NO_FILLED_KGX_LIST
	// 	} else {
	// 		o.Verification_KGX = VRF_OK
	// 	}

	// } else {
	// 	o.Verification_KGX = VRF_OK
	// }

}

const (
	VRF_OK                 = "ОК"
	VRF_NO_TARIFF          = "Не найден тариф"
	VRF_NO_IN_REG          = "Нет в реестре"
	VRF_CHECK_RATE         = "Требует уточнения курса"
	VRF_CHECK_CURRENCY     = "Валюта учёта отлична от валюты в Биллинге"
	VRF_CHECK_TARIFF       = "Несоответствие тарифа"
	VRF_DRAGON_PAY         = "Исключение ДрагонПей"
	VRF_CHECK_BILLING      = "Провень начисления биллинга"
	VRF_PARTIAL_PAYMENTS   = "Частичные выплаты"
	VRF_ENDPOINT_DRAGONPAY = "Endpoint_id пусто обратитесь к сверке/в саппорт"
)

func (op *Operation) Get_Channel_currency() currency.Currency {
	return op.Channel_currency
}

func (op *Operation) GetBool(name string) bool {
	var result bool
	switch name {
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
	default:
		logs.Add(logs.ERROR, "неизвестное поле time: ", name)
	}
	return result
}
func (op *Operation) GetInt(name string) int {
	var result int
	switch name {
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
	default:
		logs.Add(logs.ERROR, "неизвестное поле string: ", name)
	}
	return result
}
