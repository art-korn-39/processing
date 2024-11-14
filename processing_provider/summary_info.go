package processing_provider

import (
	"app/config"
	"app/currency"
	"app/logs"
	"app/tariff_provider"
	"fmt"
	"time"
)

type SumFileds struct {
	count_operations    int
	balance_amount      float64
	BR_balance_currency float64
	CompensationBR      float64
}

func (sf *SumFileds) AddValues(o *Operation) {
	sf.count_operations = sf.count_operations + o.Count_operations
	sf.balance_amount = sf.balance_amount + o.Balance_amount
	sf.BR_balance_currency = sf.BR_balance_currency + o.BR_balance_currency
	sf.CompensationBR = sf.CompensationBR + o.CompensationBR
}

type KeyFields_SummaryInfo struct {
	document_date         time.Time
	provider              string
	provider_name         string
	verification          string
	operation_type        string
	country               string
	payment_type          string
	merchant_account_name string
	merchant_name         string
	region                string
	account_bank_name     string
	channel_currency      currency.Currency
	balance_currency      currency.Currency
	tariff                tariff_provider.Tariff
}

func NewKeyFields_SummaryInfo(o *Operation) (KF KeyFields_SummaryInfo) {
	KF = KeyFields_SummaryInfo{
		document_date:         o.Document_date,
		provider:              o.Provider_base_name,
		provider_name:         o.Provider_name,
		verification:          o.Verification,
		operation_type:        o.Operation_type,
		country:               o.Country_code2,
		payment_type:          o.Payment_type,
		merchant_name:         o.Merchant_name,
		merchant_account_name: o.Merchant_account_name,
		region:                o.Country.Region,
		account_bank_name:     o.Account_bank_name,
		channel_currency:      o.Channel_currency,
		balance_currency:      o.Balance_currency,
	}

	if KF.country == "" {
		KF.country = o.Country.Code2
	}

	if o.Tariff != nil {
		KF.tariff = *o.Tariff
		KF.provider = o.Tariff.Provider
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
		group_data[k] = v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в данные Excel: %v", time.Since(start_time)))

	return

}
