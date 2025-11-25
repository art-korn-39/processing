package processing_provider

import (
	"app/config"
	"app/logs"
	"fmt"
	"strconv"
	"time"
)

//key = ДатаДокумента + Мерчант + ТипКонвертации

type SummaryRowProvider struct {
	Document_id     int       `db:"document_id"`
	Document_date   time.Time `db:"document_date"`
	Convertation_id int       `db:"convertation_id"`
	Convertation    string    `db:"convertation"`

	Operation_type      string `db:"operation_type"`
	Operation_group     string `db:"operation_group"`
	Merchant_id         int    `db:"merchant_id"`
	Merchant_account_id int    `db:"merchant_account_id"`

	Provider_id       int       `db:"provider_id"`
	Country           string    `db:"country"`
	Region            string    `db:"region"`
	Project_id        int       `db:"project_id"`
	Tariff_date_start time.Time `db:"tariff_date_start"`
	Tariff_guid       string    `db:"tariff_guid"`
	Formula           string    `db:"formula"`
	Payment_type_id   int       `db:"payment_type_id"`
	Payment_type      string    `db:"payment_type"`
	Business_type     string    `db:"business_type"`
	Provider_1c       string    `db:"provider_1c"`

	//Schema              string    `db:"schema"`
	//Payment_method_id int       `db:"payment_method_id"`
	//Balance_id          int       `db:"balance_id"`
	//Account_bank_name string `db:"account_bank_name"`

	Channel_currency_str string `db:"channel_currency"`
	Balance_currency_str string `db:"balance_currency"`

	Count_operations    int     `db:"count_operations"`
	Channel_amount      float64 `db:"channel_amount"`
	BR_channel_currency float64 `db:"br_channel_currency"`

	Balance_amount            float64 `db:"balance_amount"`
	BR_balance_currency       float64 `db:"br_balance_currency"`
	Extra_BR_balance_currency float64 `db:"extra_br_balance_currency"`

	Rate float64 `db:"rate"`

	RR_amount float64   `db:"rr_amount"`
	RR_date   time.Time `db:"rr_date"`
}

func (row *SummaryRowProvider) AddValues(o *Operation) {

	row.Count_operations = row.Count_operations + o.Count_operations
	row.Channel_amount = row.Channel_amount + o.Channel_amount
	row.Balance_amount = row.Balance_amount + o.Balance_amount
	row.BR_balance_currency = row.BR_balance_currency + o.BR_balance_currency
	row.Extra_BR_balance_currency = row.Extra_BR_balance_currency + o.Extra_BR_balance_currency

	//row.BR_channel_currency = row.BR_channel_currency + o.BR_channel_currency
	//row.RR_amount = row.RR_amount + o.RR_amount

}

func (row *SummaryRowProvider) SetRate() {

	if row.Balance_amount == 0 || row.Channel_amount == 0 {
		return
	}

	if row.Balance_currency_str == "EUR" {
		row.Rate = row.Balance_amount / row.Channel_amount
	} else {
		row.Rate = row.Channel_amount / row.Balance_amount
	}

}

// func (row *SummaryRowProvider) SetConvertationID() {
// 	if row.Convertation == "Реестр" { //|| row.Schema == "KGX" {
// 		row.Convertation_id = 2
// 	} else if row.Convertation == "Без конверта" { //|| row.Schema == "Crypto" {
// 		row.Convertation_id = 1
// 	}
// }

func (row *SummaryRowProvider) SetID() {

	//id merch len = 5
	//days len = 5
	//conv len = 1

	date_01_01_2024 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	duration := row.Document_date.Sub(date_01_01_2024)

	days := int(duration.Hours() / 24)
	days_str := fmt.Sprintf("%05d", days)

	merch_str := fmt.Sprintf("%05d", row.Provider_id)

	id_str := fmt.Sprint(10, days_str, merch_str, row.Convertation_id)
	row.Document_id, _ = strconv.Atoi(id_str)

}

func GroupRegistryToSummaryProvider() (data []SummaryRowProvider) {

	NewKey := func(o *Operation) (k SummaryRowProvider) {
		k = SummaryRowProvider{}

		k.Document_date = o.Document_date
		k.Operation_type = o.Operation_type
		k.Operation_group = o.Operation_group
		k.Merchant_id = o.Merchant_id
		k.Merchant_account_id = o.Merchant_account_id
		k.Provider_id = o.Provider_id
		k.Country = o.Country.Code2
		k.Region = o.Region
		k.Project_id = o.Project_id
		k.Payment_type = o.Payment_type
		k.Payment_type_id = o.Payment_type_id
		k.Business_type = o.Business_type
		k.Channel_currency_str = o.Channel_currency.Name
		k.Balance_currency_str = o.Balance_currency.Name
		k.Provider_1c = o.Provider1c

		//k.Balance_id = o.Balance_id
		//k.Payment_method_id = o.Payment_method_id
		//k.Account_bank_name = o.Account_bank_name
		//k.RR_date = o.RR_date

		if o.Tariff != nil {
			k.Tariff_date_start = o.Tariff.DateStart
			k.Tariff_guid = o.Tariff.GUID
			k.Formula = o.Tariff.Formula
		}

		if o.ProviderBalance != nil {
			k.Convertation = o.ProviderBalance.Convertation
			k.Convertation_id = o.ProviderBalance.Convertation_id
		}

		k.SetID()
		return
	}

	if !config.Get().Summary.Usage {
		return
	}

	start_time := time.Now()

	group_data := map[SummaryRowProvider]SummaryRowProvider{}
	for _, operation := range storage.Registry {
		key := NewKey(operation) // получили структуру с полями группировки
		row := group_data[key]   // получили текущие агрегатные данные по ним
		row.AddValues(operation) // увеличили агрегатные данные на значения тек. операции
		group_data[key] = row    // положили обратно в мапу
		operation.Document_id = key.Document_id
	}

	data = make([]SummaryRowProvider, 0, len(group_data))
	for k, v := range group_data {

		k.Rate = v.Balance_amount / v.Channel_amount
		k.Count_operations = v.Count_operations
		k.Channel_amount = v.Channel_amount
		k.Balance_amount = v.Balance_amount
		k.BR_channel_currency = v.BR_channel_currency
		k.BR_balance_currency = v.BR_balance_currency
		k.RR_amount = v.RR_amount
		k.Extra_BR_balance_currency = v.Extra_BR_balance_currency

		k.SetRate()

		data = append(data, k)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в итоговые данные: %v [%d строк]", time.Since(start_time), len(data)))

	return

}
