package processing

import (
	"app/util"
	"time"
)

type Operation struct {
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
	Provider_currency     Currency

	Msc_amount       float64 `db:"msc_amount"`
	Msc_currency_str string  `db:"msc_currency"`
	Msc_currency     Currency

	Channel_amount       float64 `db:"channel_amount"`
	Channel_currency_str string  `db:"channel_currency"`
	Channel_currency     Currency

	Fee_amount       float64 `db:"fee_amount"`
	Fee_currency_str string  `db:"fee_currency"`
	Fee_currency     Currency

	Balance_amount   float64  // сумма в валюте баланса
	Balance_currency Currency // валюта баланса

	Rate                float64
	PP_amount           float64
	SR_channel_currency float64
	SR_balance_currency float64
	CheckFee            float64
	Verification        string
	IsDragonPay         bool

	Crypto_network    string
	ProviderOperation *ProviderOperation
	Tariff_bof        *Tariff
	Tariff            *Tariff
}

func (o *Operation) StartingFill() {

	if o.Transaction_completed_at.IsZero() {
		o.Transaction_completed_at = o.Operation_created_at
	}

	o.Document_date = util.TruncateToDay(o.Transaction_completed_at)

	o.Provider_currency = NewCurrency(o.Provider_currency_str)
	o.Msc_currency = NewCurrency(o.Msc_currency_str)
	o.Channel_currency = NewCurrency(o.Channel_currency_str)
	o.Fee_currency = NewCurrency(o.Fee_currency_str)

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

}

func (o *Operation) SetBalanceAmount() {

	t := o.Tariff
	o.Balance_currency = t.CurrencyBP

	rate := float64(1)
	balance_amount := float64(0)

	if o.Channel_currency == o.Balance_currency {
		balance_amount = o.Channel_amount
	} else if t.Convertation == "Без конверта" {
		balance_amount = o.Channel_amount
	} else if t.Convertation == "Колбек" {
		balance_amount = o.Provider_amount
	} else if t.Convertation == "Реестр" {

		// Поиск в мапе операций провайдера по ID
		ProviderOperation, ok := storage.Provider_operations[o.Operation_id]
		o.ProviderOperation = &ProviderOperation

		balance_amount = ProviderOperation.Amount
		rate = ProviderOperation.Rate

		// если не нашли операцию провайдера по ID, то подбираем курс и считаем через него
		if !ok {
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

	var CheckRates float64
	var Converation string
	var CurrencyBP Currency
	if o.Tariff != nil {
		Converation = o.Tariff.Convertation
		CurrencyBP = o.Tariff.CurrencyBP
		if o.Tariff_bof != nil {
			s1 := (o.Tariff.Percent + o.Tariff.Fix + o.Tariff.Min + o.Tariff.Max) * 100
			s2 := o.Tariff_bof.Percent + o.Tariff_bof.Fix + o.Tariff_bof.Min + o.Tariff_bof.Max
			CheckRates = util.BaseRound(s1 - s2)
		}
	}

	var Provider_registry_amount float64
	if o.ProviderOperation != nil {
		Provider_registry_amount = o.ProviderOperation.Amount
	}

	if o.CheckFee == 0 {
		if Converation == "Реестр" {
			o.Verification = "Валидирован по реестру"
		} else {
			o.Verification = "ОК"
		}
	} else if o.Tariff == nil {
		o.Verification = "Не найден тариф"
	} else if Converation == "Реестр" {
		if o.Balance_amount == 0 { // нет курса и операции провайдера
			o.Verification = "Нет в реестре"
		} else if Provider_registry_amount == 0 { // еще не появился в реестре факт
			o.Verification = "Требует уточнения курса"
		} else { // есть в реестре факт, но БОФ криво посчитал
			o.Verification = "Валидирован по реестру (см. CheckFee)"
		}
	} else if o.Channel_currency != CurrencyBP {
		o.Verification = "Валюта учёта отлична от валюты в Биллинге"
	} else if CheckRates != 0 {
		o.Verification = "Несоответствие тарифа"
	} else if o.IsDragonPay {
		o.Verification = "Исключение ДрагонПей"
	} else {
		o.Verification = "Провень начисления биллинга"
	}

}
