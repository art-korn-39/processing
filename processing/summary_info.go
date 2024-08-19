package processing

import (
	"app/config"
	"app/currency"
	"app/logs"
	"fmt"
	"time"
)

type SumFileds struct {
	count_operations    int
	channel_amount      float64
	balance_amount      float64
	fee_amount          float64
	SR_channel_currency float64
	SR_balance_currency float64
	checkFee            float64
	checkRates          float64
	RR_amount           float64
	hold_amount         float64
	CompensationRC      float64
	CompensationBC      float64
}

func (sf *SumFileds) AddValues(o *Operation) {
	sf.count_operations = sf.count_operations + o.Count_operations
	sf.channel_amount = sf.channel_amount + o.Channel_amount
	sf.balance_amount = sf.balance_amount + o.Balance_amount
	sf.fee_amount = sf.fee_amount + o.Fee_amount
	sf.SR_channel_currency = sf.SR_channel_currency + o.SR_channel_currency
	sf.SR_balance_currency = sf.SR_balance_currency + o.SR_balance_currency
	sf.checkFee = sf.checkFee + o.CheckFee
	sf.checkRates = sf.checkRates + o.CheckRates
	sf.RR_amount = sf.RR_amount + o.RR_amount
	sf.hold_amount = sf.hold_amount + o.hold_amount
	sf.CompensationBC = sf.CompensationBC + o.CompensationBC
	sf.CompensationRC = sf.CompensationRC + o.CompensationRC
}

type KeyFields_SummaryInfo struct {
	document_date time.Time
	balance_id    int

	verification          string
	operation_type        string
	country               string
	payment_type          string
	merchant_name         string
	project_name          string
	merchant_account_name string
	balance_name          string
	merchant_account_id   int
	tariff_condition_id   int

	channel_currency currency.Currency
	balance_currency currency.Currency

	tariff     Tariff
	tariff_bof Tariff

	// balance_name  string
	// subdivision1C string
	// provider1C    string
	// ratedAccount  string

	// currencyBP                currency.Currency
	// percent                   float64
	// fix                       float64
	// min                       float64
	// max                       float64
	// range_min                 float64
	// range_max                 float64
	// date_start                time.Time
	// tariff_condition_id       int
	contract_id    int //???
	RR_date        time.Time
	hold_date      time.Time
	Crypto_network string
	// Formula, FormulaDK, Range string
}

func NewKeyFields_SummaryInfo(o *Operation) (KF KeyFields_SummaryInfo) {
	KF = KeyFields_SummaryInfo{
		document_date:         o.Document_date,
		balance_id:            o.Balance_id,
		verification:          o.Verification,
		operation_type:        o.Operation_type,
		country:               o.Country,
		payment_type:          o.Payment_type,
		merchant_name:         o.Merchant_name,
		project_name:          o.Project_name,
		merchant_account_name: o.Merchant_account_name,
		merchant_account_id:   o.Merchant_account_id,
		tariff_condition_id:   o.Tariff_condition_id,
		channel_currency:      o.Channel_currency,
		balance_currency:      o.Balance_currency,
		Crypto_network:        o.Crypto_network,
		RR_date:               o.RR_date,
		hold_date:             o.hold_date,
	}

	if o.Tariff != nil {
		KF.tariff = *o.Tariff

		if o.Tariff.Schema == "KGX" {
			if o.ProviderOperation != nil {
				KF.balance_name = o.Provider_name // тут уже лежит баланс из реестра провайдера
			}
		} else {
			KF.balance_name = o.Tariff.Balance_name
		}
	}

	if o.Tariff_bof != nil {
		KF.tariff_bof = *o.Tariff_bof
	}

	// if o.Tariff != nil {
	// 	KF.balance_name = o.Tariff.Balance_name
	// 	KF.subdivision1C = o.Tariff.Subdivision1C
	// 	KF.ratedAccount = o.Tariff.RatedAccount
	// 	KF.provider1C = o.Tariff.Provider1C
	// 	KF.currencyBP = o.Tariff.CurrencyBP
	// 	KF.percent = o.Tariff.Percent
	// 	KF.fix = o.Tariff.Fix
	// 	KF.min = o.Tariff.Min
	// 	KF.max = o.Tariff.Max
	// 	KF.range_min = o.Tariff.RangeMIN
	// 	KF.range_max = o.Tariff.RangeMAX
	// 	KF.date_start = o.Tariff.DateStart
	// 	KF.tariff_condition_id = o.Tariff.id
	// 	KF.Formula = o.Tariff.Formula
	// 	KF.FormulaDK = o.Tariff.DK_formula
	// 	KF.Range = o.Tariff.Range
	// }

	return

}

func GroupRegistryToSummaryInfo() (group_data map[KeyFields_SummaryInfo]SumFileds) {

	if !config.Get().SummaryInfo.Usage {
		return
	}

	start_time := time.Now()

	group_data = map[KeyFields_SummaryInfo]SumFileds{}
	for _, operation := range storage.Registry {
		kf := NewKeyFields_SummaryInfo(operation) // получили структуру с полями группировки
		sf := group_data[kf]                      // получили текущие агрегатные данные по ним
		sf.AddValues(operation)                   // увеличили агрегатные данные на значения тек. операции
		group_data[kf] = sf                       // положили обратно в мапу
	}

	for k, v := range group_data {
		v.checkRates = v.checkRates / float64(v.count_operations)
		group_data[k] = v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в данные Excel: %v", time.Since(start_time)))

	return

}
