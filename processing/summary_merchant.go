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
	Project_id          int       `db:"project_id"`
	Tariff_date_start   time.Time `db:"tariff_date_start"`
	Tariff_id           int       `db:"tariff_id"`
	Formula             string    `db:"formula"`
	Payment_type_id     int       `db:"payment_type_id"`
	Payment_type        string    `db:"payment_type"`
	Payment_method_id   int       `db:"payment_method_id"`

	Business_type     string `db:"business_type"`
	Account_bank_name string `db:"account_bank_name"`

	Channel_currency_str string `db:"channel_currency"`
	Balance_currency_str string `db:"balance_currency"`

	Count_operations    int     `db:"count_operations"`
	Channel_amount      float64 `db:"channel_amount"`
	SR_channel_currency float64 `db:"sr_channel_currency"`
	Balance_amount      float64 `db:"balance_amount"`
	SR_balance_currency float64 `db:"sr_balance_currency"`

	Rate           float64 `db:"rate"`
	Rated_account  string  `db:"rated_account"`
	Provider_1c    string  `db:"provider_1c"`
	Subdivision_1c string  `db:"subdivision_1c"`

	RR_amount float64   `db:"rr_amount"`
	RR_date   time.Time `db:"rr_date"`
}

func (row *SummaryRowMerchant) AddValues(o Operation) {

	row.Rate = row.Rate + o.Rate
	row.Count_operations = row.Count_operations + o.Count_operations
	row.Channel_amount = row.Channel_amount + o.Channel_amount
	row.Balance_amount = row.Balance_amount + o.Balance_amount
	row.SR_channel_currency = row.SR_channel_currency + o.SR_channel_currency
	row.SR_balance_currency = row.SR_balance_currency + o.SR_balance_currency

	if o.Tariff != nil && o.Operation_group == "IN" {
		row.RR_amount = row.RR_amount + o.Tariff.RR_percent/100*o.Balance_amount
	}
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
		k.Project_id = o.Project_id
		k.Payment_type = o.Payment_type
		k.Payment_type_id = o.Payment_type_id
		k.Payment_method_id = o.Payment_method_id
		k.Account_bank_name = o.Account_bank_name
		k.Business_type = o.Business_type
		k.Channel_currency_str = o.Channel_currency.Name
		k.Balance_currency_str = o.Balance_currency.Name

		if o.Tariff != nil {
			k.Convertation = o.Tariff.Convertation
			k.Tariff_date_start = o.Tariff.DateStart
			k.Tariff_id = o.Tariff.id
			k.Formula = o.Tariff.Formula
			k.Provider_1c = o.Tariff.Provider1C
			k.Subdivision_1c = o.Tariff.Subdivision1C
			k.Rated_account = o.Tariff.RatedAccount

			if o.Operation_group == "IN" {
				k.RR_date = o.Document_date.AddDate(0, 0, o.Tariff.RR_days)
			}
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

		k.Rate = v.Rate / float64(v.Count_operations)
		k.Count_operations = v.Count_operations
		k.Channel_amount = v.Channel_amount
		k.Balance_amount = v.Balance_amount
		k.SR_channel_currency = v.SR_channel_currency
		k.SR_balance_currency = v.SR_balance_currency
		k.RR_amount = v.RR_amount

		data = append(data, k)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в итоговые данные: %v [%d строк]", time.Since(start_time), len(data)))

	return

}
