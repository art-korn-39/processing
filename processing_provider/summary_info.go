package processing_provider

import (
	"app/config"
	"app/currency"
	"app/logs"
	"fmt"
	"time"
)

type SumFileds struct {
	count_operations    int
	balance_amount      float64
	BR_balance_currency float64
	CompensationBR      float64
	checkFee            float64
	checkRates          float64
}

func (sf *SumFileds) AddValues(o *Operation) {
	sf.count_operations = sf.count_operations + o.Count_operations
	sf.balance_amount = sf.balance_amount + o.Balance_amount
	sf.BR_balance_currency = sf.BR_balance_currency + o.BR_balance_currency
	sf.CompensationBR = sf.CompensationBR + o.CompensationBR
	sf.checkFee = sf.checkFee + o.CheckFee
	sf.checkRates = sf.checkRates + o.CheckRates
}

// func (sf *SumFileds) AddValuesFromSF(sf2 SumFileds) {
// 	sf.count_operations = sf.count_operations + sf2.count_operations
// 	sf.channel_amount = sf.channel_amount + sf2.channel_amount
// 	sf.balance_amount = sf.balance_amount + sf2.balance_amount
// 	sf.fee_amount = sf.fee_amount + sf2.fee_amount
// 	sf.SR_channel_currency = sf.SR_channel_currency + sf2.SR_channel_currency
// 	sf.SR_balance_currency = sf.SR_balance_currency + sf2.SR_balance_currency
// 	sf.checkFee = sf.checkFee + sf2.checkFee
// 	sf.checkRates = sf.checkRates + sf2.checkRates
// 	sf.RR_amount = sf.RR_amount + sf2.RR_amount
// 	sf.hold_amount = sf.hold_amount + sf2.hold_amount
// 	sf.CompensationBC = sf.CompensationBC + sf2.CompensationBC
// 	sf.CompensationRC = sf.CompensationRC + sf2.CompensationRC
// 	sf.BalanceRefund_turnover = sf.BalanceRefund_turnover + sf2.BalanceRefund_turnover
// }

type KeyFields_SummaryInfo struct {
	document_date    time.Time
	balance_id       int
	balance_name     string
	provider         string
	JL               string
	provider_name    string
	verification     string
	operation_type   string
	country          string
	payment_type     string
	merchant_name    string
	channel_currency currency.Currency
	balance_currency currency.Currency
}

func NewKeyFields_SummaryInfo(o *Operation) (KF KeyFields_SummaryInfo) {
	KF = KeyFields_SummaryInfo{
		document_date: o.Document_date,
		balance_id:    o.Balance_id,
		//balance_name:          o.balance_name,
		provider:         o.Provider_base_name,
		provider_name:    o.Provider_name,
		verification:     o.Verification,
		operation_type:   o.Operation_type,
		country:          o.Country,
		payment_type:     o.Payment_type,
		merchant_name:    o.Merchant_name,
		channel_currency: o.Channel_currency,
		balance_currency: o.Balance_currency,
	}

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
		//v.SetBalanceRefund(k.tariff.Convertation, k.tariff.Percent)
		group_data[k] = v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в данные Excel: %v", time.Since(start_time)))

	return

}
