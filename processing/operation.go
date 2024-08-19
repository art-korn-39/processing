package processing

import (
	"app/currency"
	"app/holds"
	"app/provider"
	"app/util"
	"strings"
	"sync"
	"time"
)

type Operation struct {
	mu sync.Mutex

	Operation_id   int `db:"operation_id"`
	Transaction_id int `db:"transaction_id"`
	Document_date  time.Time

	Transaction_completed_at time.Time
	Operation_created_at     time.Time `db:"operation_created_at"`

	Merchant_id         int    `db:"merchant_id"`
	Merchant_account_id int    `db:"merchant_account_id"`
	Balance_id          int    `db:"balance_id"`
	Company_id          int    `db:"company_id"`
	Contract_id         int    `db:"contract_id"`
	Provider_id         int    `db:"provider_id"`
	Tariff_condition_id int    `db:"tariff_id"`
	Provider_payment_id string `db:"provider_payment_id"`

	Provider_name         string `db:"provider_name"`
	Merchant_name         string `db:"merchant_name"`
	Merchant_account_name string `db:"merchant_account_name"`
	Account_bank_name     string `db:"account_bank_name"`
	Business_type         string `db:"business_type"`
	Country               string `db:"country"`
	Region                string `db:"region"`

	Project_name      string `db:"project_name"`
	Project_id        int    `db:"project_id"`
	Payment_type      string `db:"payment_type"`
	Payment_type_id   int    `db:"payment_type_id"`
	Payment_method_id int    `db:"payment_method_id"`

	Operation_type    string `db:"operation_type"`
	Operation_type_id int    `db:"operation_type_id"`
	Operation_group   string
	Count_operations  int `db:"count_operations"`

	Provider_amount       float64 `db:"provider_amount"`
	Provider_currency_str string  `db:"provider_currency"`
	Provider_currency     currency.Currency

	Msc_amount       float64 `db:"msc_amount"`
	Msc_currency_str string  `db:"msc_currency"`
	Msc_currency     currency.Currency

	Channel_amount       float64 `db:"channel_amount"`
	Channel_currency_str string  `db:"channel_currency"`
	Channel_currency     currency.Currency

	Fee_amount       float64 `db:"fee_amount"`
	Fee_currency_str string  `db:"fee_currency"`
	Fee_currency     currency.Currency

	Balance_amount   float64           // сумма в валюте баланса
	Balance_currency currency.Currency // валюта баланса

	Rate                 float64
	SR_channel_currency  float64
	SR_balance_currency  float64
	CheckFee, CheckRates float64
	Verification         string
	IsDragonPay          bool
	IsPerevodix          bool
	Crypto_network       string

	RR_amount float64
	RR_date   time.Time

	CompensationRC float64
	CompensationBC float64

	hold_amount float64
	hold_date   time.Time

	ProviderOperation *provider.Operation
	Tariff_bof        *Tariff
	Tariff            *Tariff
	Hold              *holds.Hold

	Tariff_rate_fix     float64 `db:"billing__tariff_rate_fix"`
	Tariff_rate_percent float64 `db:"billing__tariff_rate_percent"`
	Tariff_rate_min     float64 `db:"billing__tariff_rate_min"`
	Tariff_rate_max     float64 `db:"billing__tariff_rate_max"`
}

func (o *Operation) StartingFill() {

	if o.Transaction_completed_at.IsZero() {
		o.Transaction_completed_at = o.Operation_created_at
	}

	o.Document_date = util.TruncateToDay(o.Transaction_completed_at)

	o.IsDragonPay = strings.Contains(o.Provider_name, "Dragonpay")
	o.IsPerevodix = strings.Contains(o.Provider_name, "Perevodix") || o.Provider_name == "SbpQRViaIntervaleE46AltIT"

	o.Provider_currency = currency.New(o.Provider_currency_str)
	o.Msc_currency = currency.New(o.Msc_currency_str)
	o.Channel_currency = currency.New(o.Channel_currency_str)
	o.Fee_currency = currency.New(o.Fee_currency_str)

	o.Provider_amount = util.TR(o.Provider_currency.Exponent, o.Provider_amount, o.Provider_amount/100).(float64)
	o.Msc_amount = util.TR(o.Msc_currency.Exponent, o.Msc_amount, o.Msc_amount/100).(float64)
	o.Channel_amount = util.TR(o.Channel_currency.Exponent, o.Channel_amount, o.Channel_amount/100).(float64)
	o.Fee_amount = util.TR(o.Fee_currency.Exponent, o.Fee_amount, o.Fee_amount/100).(float64)

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

	o.Tariff_rate_fix = util.TR(o.Channel_currency.Exponent, o.Tariff_rate_fix, o.Tariff_rate_fix/100).(float64)
	o.Tariff_rate_min = util.TR(o.Channel_currency.Exponent, o.Tariff_rate_min, o.Tariff_rate_min/100).(float64)
	o.Tariff_rate_max = util.TR(o.Channel_currency.Exponent, o.Tariff_rate_max, o.Tariff_rate_max/100).(float64)

	o.Tariff_bof = &Tariff{
		Percent: o.Tariff_rate_percent,
		Fix:     o.Tariff_rate_fix,
		Min:     o.Tariff_rate_min,
		Max:     o.Tariff_rate_max,
	}
	o.Tariff_bof.StartingFill()

}

func (o *Operation) SetBalanceAmount() {

	t := o.Tariff
	o.Balance_currency = t.CurrencyBP

	rate := float64(1)
	balance_amount := float64(0)

	// у KGX может быть RUB-RUB, но комсу надо брать из операции провайдера
	if o.Channel_currency == o.Balance_currency && t.Schema != "KGX" {
		balance_amount = o.Channel_amount
	} else if t.Convertation == "Без конверта" {
		balance_amount = o.Channel_amount
	} else if t.Convertation == "Колбек" {
		balance_amount = o.Provider_amount
	} else if t.Convertation == "Реестр" || t.Schema == "KGX" {

		// Поиск в мапе операций провайдера по ID
		ProviderOperation, ok := provider.Registry.Get(o.Operation_id, o.Document_date, o.Channel_amount)
		o.ProviderOperation = ProviderOperation
		if ok {
			balance_amount = ProviderOperation.Amount
			rate = ProviderOperation.Rate

			if t.Schema == "KGX" {
				o.Provider_name = ProviderOperation.Balance //!!!
				o.Balance_currency = ProviderOperation.Provider_currency
			}

		} else {
			// если не нашли операцию провайдера по ID, то подбираем курс и считаем через него
			rate = FindRateForOperation(o)
			if rate != 0 {
				balance_amount = o.Channel_amount / rate
			}
		}

	} else { // крипта скорее всего
		balance_amount = o.Channel_amount
	}

	o.Rate = rate
	o.Balance_amount = balance_amount

}

func (o *Operation) SetSRAmount() {

	t := o.Tariff

	if t == nil {
		return
	}

	// SR В ВАЛЮТЕ БАЛАНСА
	commission := o.Balance_amount*t.Percent + t.Fix

	if t.Min != 0 && commission < t.Min {
		commission = t.Min
	} else if t.Max != 0 && commission > t.Min {
		commission = t.Max
	}

	// SR В ВАЛЮТЕ КАНАЛА
	if t.Convertation == "Реестр" {
		o.SR_channel_currency = commission * o.Rate
	} else {
		o.SR_channel_currency = commission
	}

	// для KGX подставляем BR в SR_balance_currency
	if t.Schema == "KGX" && o.ProviderOperation != nil {
		commission = o.ProviderOperation.BR_amount
	}

	// ОКРУГЛЕНИЕ
	if o.Balance_currency.Exponent {
		o.Balance_amount = util.Round(o.Balance_amount, 0)
		o.SR_balance_currency = util.Round(commission, 0)
	} else {
		o.Balance_amount = util.Round(o.Balance_amount, 2)
		o.SR_balance_currency = util.Round(commission, 2)
	}

	if o.Channel_currency.Exponent {
		o.SR_channel_currency = util.Round(o.SR_channel_currency, 0)
	} else {
		o.SR_channel_currency = util.Round(o.SR_channel_currency, 2)
	}

}

func (o *Operation) SetCheckFee() {

	if o.Fee_currency == o.Balance_currency {
		o.CheckFee = util.BaseRound(o.Fee_amount - o.SR_balance_currency)
	} else {
		o.CheckFee = util.BaseRound(o.Fee_amount - o.SR_channel_currency)
	}

}

func (o *Operation) SetVerification() {

	var Converation, Schema string
	var CurrencyBP currency.Currency

	if o.Tariff != nil {
		Converation = o.Tariff.Convertation
		Schema = o.Tariff.Schema
		CurrencyBP = o.Tariff.CurrencyBP
		if o.Tariff_bof != nil {
			s1 := (o.Tariff.Percent + o.Tariff.Fix + o.Tariff.Min + o.Tariff.Max) //* 100
			s2 := o.Tariff_bof.Percent + o.Tariff_bof.Fix + o.Tariff_bof.Min + o.Tariff_bof.Max
			o.CheckRates = util.BaseRound(s1 - s2)
		}
	}

	if o.Tariff == nil {
		o.Verification = VRF_NO_TARIFF
	} else if Converation == "Реестр" || Schema == "KGX" {
		if o.Balance_amount == 0 {
			o.Verification = VRF_NO_IN_REG // нет курса и операции провайдера
		} else if o.ProviderOperation == nil {
			o.Verification = VRF_CHECK_RATE // курс есть, операции еще нет в реестре факт
		} else if o.Channel_amount != o.ProviderOperation.Channel_amount {
			o.Verification = VRF_DIFF_CHAN_AMOUNT // channel amount разный в БОФ и реестре пров.
		} else if o.CheckFee != 0 {
			o.Verification = VRF_VALID_REG_FEE // есть в реестре факт, но БОФ криво посчитал
		} else {
			o.Verification = VRF_VALID_REG // всё ок
		}
	} else if o.CheckFee == 0 {
		o.Verification = VRF_OK
	} else if o.Channel_currency != CurrencyBP {
		o.Verification = VRF_CHECK_CURRENCY
	} else if o.CheckRates != 0 {
		o.Verification = VRF_CHECK_TARIFF
	} else if o.IsDragonPay {
		o.Verification = VRF_DRAGON_PAY
	} else {
		o.Verification = VRF_CHECK_BILLING
	}

}

const (
	VRF_OK               = "ОК"
	VRF_VALID_REG        = "Валидирован по реестру"
	VRF_VALID_REG_FEE    = "Валидирован по реестру (см. CheckFee)"
	VRF_NO_TARIFF        = "Не найден тариф"
	VRF_NO_IN_REG        = "Нет в реестре"
	VRF_CHECK_RATE       = "Требует уточнения курса"
	VRF_DIFF_CHAN_AMOUNT = "Real amount отличается от реестра"
	VRF_CHECK_CURRENCY   = "Валюта учёта отлична от валюты в Биллинге"
	VRF_CHECK_TARIFF     = "Несоответствие тарифа"
	VRF_DRAGON_PAY       = "Исключение ДрагонПей"
	VRF_CHECK_BILLING    = "Провень начисления биллинга"
)

func (o *Operation) SetRR() {

	if o.Tariff == nil {
		return
	}

	if o.Operation_type != "sale" {
		return
	}

	o.RR_date = o.Document_date.AddDate(0, 0, o.Tariff.RR_days)
	o.RR_amount = o.Balance_amount * o.Tariff.RR_percent / 100

}

func (o *Operation) SetHold() {

	if o.Hold == nil {
		return
	}

	if o.Operation_type != "sale" {
		return
	}

	o.hold_date = o.Document_date.AddDate(0, 0, o.Hold.Days)
	o.hold_amount = o.Balance_amount * o.Hold.Percent

}

func (o *Operation) SetDK() {

	t := o.Tariff

	if t.DK_is_zero {
		return
	}

	// BALANCE CURRENCY
	commissionBC := o.Balance_amount*o.Tariff.DK_percent + t.DK_fix

	if t.DK_min != 0 && commissionBC < t.DK_min {
		commissionBC = t.DK_min
	} else if t.DK_max != 0 && commissionBC > t.DK_max {
		commissionBC = t.DK_max
	}

	if o.Balance_currency.Exponent {
		commissionBC = util.Round(commissionBC, 0)
	} else {
		commissionBC = util.Round(commissionBC, 2)
	}

	o.CompensationBC = commissionBC - o.SR_balance_currency

	// CHANNEL CURRENCY
	commissionRC := o.Channel_amount*o.Tariff.DK_percent + t.DK_fix
	commission_with_rate := commissionRC / util.TR(o.Rate == 0, 1, o.Rate).(float64)

	if t.DK_min != 0 && commission_with_rate < t.DK_min {
		commissionRC = t.DK_min * o.Rate
	} else if t.DK_max != 0 && commission_with_rate > t.DK_max {
		commissionRC = t.DK_max * o.Rate
	}

	if o.Channel_currency.Exponent {
		commissionRC = util.Round(commissionRC, 0)
	} else {
		commissionRC = util.Round(commissionRC, 2)
	}

	o.CompensationRC = commissionRC - o.SR_channel_currency

}
