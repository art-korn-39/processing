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
	compensationBR      float64
	channel_amount      float64
	surcharge_amount    float64
}

func (sf *SumFileds) AddValues(o *Operation) {
	sf.count_operations = sf.count_operations + o.Count_operations
	sf.balance_amount = sf.balance_amount + o.Balance_amount
	sf.BR_balance_currency = sf.BR_balance_currency + o.BR_balance_currency
	sf.compensationBR = sf.compensationBR + o.CompensationBR
	sf.channel_amount = sf.channel_amount + o.Channel_amount
	sf.surcharge_amount = sf.surcharge_amount + o.Surcharge_amount
}

type KeyFields_SummaryInfo struct {
	balance      string
	organization string
	id_revise    string

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
	contractor_provider   string
	contractor_merchant   string
}

func NewKeyFields_SummaryInfo(o *Operation) (KF KeyFields_SummaryInfo) {
	KF = KeyFields_SummaryInfo{
		document_date:         o.Document_date,
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
		//KF.provider = o.Tariff.Provider
	}

	if o.ProviderBalance != nil {
		KF.id_revise = o.ProviderBalance.Balance_code
		KF.balance = o.ProviderBalance.Name
		KF.organization = o.ProviderBalance.Legal_entity
		KF.contractor_provider = o.ProviderBalance.Contractor
		//KF.balance_currency = o.ProviderBalance.Balance_currency
	}

	if o.Merchant != nil {
		KF.contractor_merchant = o.Merchant.Contractor_name
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
