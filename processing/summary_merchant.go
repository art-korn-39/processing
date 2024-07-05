package processing

import (
	"app/config"
	"app/logs"
	"fmt"
	"time"
)

type SummaryRowMerchant struct {
	Document_date       time.Time `db:"document_date"`
	Convertation        string    `db:"convertation"`
	Operation_type      string    `db:"operation_type"`
	Operation_group     string    `db:"operation_group"`
	Merchant_id         int       `db:"merchant_id"`
	Merchant_account_id int       `db:"merchant_account_id"`
	Balance_id          int       `db:"balance_id"`
	Provider_id         int       `db:"provider_id"`
	Country             string    `db:"country"`
	Region              string    `db:"region"`
	Tariff_date_start   time.Time `db:"tariff_date_start"`
	Tariff_id           int       `db:"tariff_id"`
	Formula             string    `db:"formula"`
	Crypto_network      string    `db:"payment_type"`

	Count_operations int `db:"count_operations"`

	Channel_currency_str string  `db:"channel_currency"`
	Channel_amount       float64 `db:"channel_amount"`
	SR_channel_currency  float64 `db:"sr_channel_currency"`

	Balance_currency_str string  `db:"balance_currency"`
	Balance_amount       float64 `db:"balance_amount"`
	SR_balance_currency  float64 `db:"sr_balance_currency"`

	//Provider_payment_id string    `db:"provider_payment_id"`
	//Contract_date_start   time.Time `db:"tariff_date_start"`
}

func (row *SummaryRowMerchant) AddValues(o Operation) {

	row.Count_operations = row.Count_operations + o.Count_operations
	row.Channel_amount = row.Channel_amount + o.Channel_amount
	row.Balance_amount = row.Balance_amount + o.Balance_amount
	row.SR_channel_currency = row.SR_channel_currency + o.SR_channel_currency
	row.SR_balance_currency = row.SR_balance_currency + o.SR_balance_currency
}

func GroupRegistryToSummaryMerchant() (data []SummaryRowMerchant) {

	NewKey := func(o Operation) (k SummaryRowMerchant) {
		k = SummaryRowMerchant{}

		k.Document_date = o.Document_date
		k.Operation_type = o.Operation_type
		k.Operation_group = o.Operation_group
		k.Merchant_id = o.Merchant_id
		k.Merchant_account_id = o.Merchant_account_id
		k.Balance_id = o.Balance_id
		k.Provider_id = o.Provider_id
		k.Country = o.Country
		k.Region = o.Region
		k.Crypto_network = o.Crypto_network
		k.Channel_currency_str = o.Channel_currency.Name
		k.Balance_currency_str = o.Balance_currency.Name

		if o.Tariff != nil {
			k.Convertation = o.Tariff.Convertation
			k.Tariff_date_start = o.Tariff.DateStart
			k.Tariff_id = o.Tariff.id
			k.Formula = o.Tariff.Formula
		}
		return
	}

	if !config.Get().Summary.Usage {
		return
	}

	start_time := time.Now()

	group_data := map[SummaryRowMerchant]SummaryRowMerchant{}
	for _, operation := range storage.Registry {
		key := NewKey(*operation) // получили структуру с полями группировки
		row := group_data[key]    // получили текущие агрегатные данные по ним
		row.AddValues(*operation) // увеличили агрегатные данные на значения тек. операции
		group_data[key] = row     // положили обратно в мапу
	}

	data = make([]SummaryRowMerchant, 0, len(group_data))
	for k, v := range group_data {

		k.Count_operations = v.Count_operations
		k.Channel_amount = v.Channel_amount
		k.Balance_amount = v.Balance_amount
		k.SR_channel_currency = v.SR_channel_currency
		k.SR_balance_currency = v.SR_balance_currency

		data = append(data, k)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в итоговые данные: %v [%d строк]", time.Since(start_time), len(data)))

	return

}
