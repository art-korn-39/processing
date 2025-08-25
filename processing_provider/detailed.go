package processing_provider

import (
	"app/currency"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type Detailed_row struct {
	Operation_id              int       `db:"operation_id"`
	Provider_payment_id       string    `db:"provider_payment_id"`
	Transaction_id            int       `db:"transaction_id"`
	RRN                       string    `db:"rrn"`
	Payment_id                string    `db:"payment_id"`
	Provider_name             string    `db:"provider_name"`
	Provider_id               int       `db:"provider_id"`
	Merchant_account_name     string    `db:"merchant_account_name"`
	Merchant_name             string    `db:"merchant_name"`
	Project_id                int       `db:"project_id"`
	Operation_type            string    `db:"operation_type"`
	Payment_type              string    `db:"payment_type"`
	Country                   string    `db:"country"`
	Transaction_created_at    time.Time `db:"transaction_created_at"`
	Transaction_completed_at  time.Time `db:"transaction_completed_at"`
	Channel_amount            float64   `db:"channel_amount"`
	Channel_currency_str      string    `db:"channel_currency"`
	Provider_amount           float64   `db:"provider_amount"`
	Provider_currency_str     string    `db:"provider_currency"`
	Operation_actual_amount   float64   `db:"operation_actual_amount"`
	Surcharge_amount          float64   `db:"surcharge_amount"`
	Surcharge_currency_str    string    `db:"surcharge_currency"`
	Endpoint_id               string    `db:"endpoint_id"`
	Account_bank_name         string    `db:"account_bank_name"`
	Operation_created_at      time.Time `db:"operation_created_at"`
	Balance_amount            float64   `db:"balance_amount"`
	BR_balance_currency       float64   `db:"br_balance_currency"`
	Extra_BR_balance_currency float64   `db:"extra_br_balance_currency"`
	Balance_currency_str      string    `db:"balance_currency"`
	Rate                      float64   `db:"rate"`
	CompensationBR            float64   `db:"compensation_br"`
	Verification              string    `db:"verification"`
	Tariff_date_start         time.Time `db:"tariff_date_start"`
	Act_percent               float64   `db:"act_percent"`
	Act_fix                   float64   `db:"act_fix"`
	Act_min                   float64   `db:"act_min"`
	Act_max                   float64   `db:"act_max"`
	Range_min                 float64   `db:"range_min"`
	Range_max                 float64   `db:"range_max"`
	Region                    string    `db:"region"`
	Document_id               int       `db:"document_id"`
	Provider_dragonpay        string    `db:"provider_dragonpay"`

	Balance_currency currency.Currency
	Provider_BR      float64

	IsTestId   int    `db:"is_test_id"`
	IsTestType string `db:"is_test_type"`
}

func NewDetailedRow(o *Operation) (d Detailed_row) {

	d = Detailed_row{}

	d.Operation_id = o.Operation_id
	d.Provider_payment_id = o.Provider_payment_id
	d.Transaction_id = o.Transaction_id
	d.RRN = o.RRN
	d.Payment_id = o.Payment_id
	d.Provider_name = o.Provider_name
	d.Provider_id = o.Provider_id
	d.Merchant_name = o.Merchant_name
	d.Merchant_account_name = o.Merchant_account_name
	d.Project_id = o.Project_id
	d.Operation_type = o.Operation_type
	d.Payment_type = o.Payment_type
	if o.Country_code2 != "" {
		d.Country = o.Country_code2
	} else {
		d.Country = o.Country.Code2
	}
	d.Transaction_created_at = o.Transaction_created_at
	d.Transaction_completed_at = o.Transaction_completed_at
	d.Channel_amount = o.Channel_amount
	d.Channel_currency_str = o.Channel_currency.Name
	d.Provider_amount = o.Provider_amount
	d.Provider_currency_str = o.Provider_currency.Name
	d.Operation_actual_amount = o.Operation_actual_amount
	d.Surcharge_amount = o.Surcharge_amount
	d.Surcharge_currency_str = o.Surcharge_currency.Name
	if o.DragonpayOperation != nil {
		d.Endpoint_id = o.DragonpayOperation.Endpoint_id
	} else {
		d.Endpoint_id = o.Endpoint_id
	}
	d.Account_bank_name = o.Account_bank_name
	d.Operation_created_at = o.Operation_created_at
	d.Balance_amount = o.Balance_amount
	d.BR_balance_currency = o.BR_balance_currency
	d.Extra_BR_balance_currency = o.Extra_BR_balance_currency
	if o.Channel_currency.Name == o.Balance_currency.Name {
		d.Rate = 1
	} else if o.ProviderOperation != nil {
		d.Rate = o.ProviderOperation.Rate
	}
	d.Balance_currency_str = o.Balance_currency.Name
	d.Balance_currency = o.Balance_currency
	d.CompensationBR = o.CompensationBR
	d.Verification = o.Verification
	d.Region = o.Country.Region

	if o.DragonpayOperation != nil {
		d.Provider_dragonpay = o.DragonpayOperation.Provider1c
	}

	d.IsTestId = o.IsTestId
	d.IsTestType = o.IsTestType

	if o.Tariff != nil {
		t := o.Tariff
		d.Tariff_date_start = t.DateStart
		d.Act_percent = t.Percent
		d.Act_fix = t.Fix
		d.Act_min = t.Min
		d.Act_max = t.Max
		d.Range_min = t.Range_amount_min
		d.Range_max = t.Range_amount_max
	}

	if o.ProviderOperation != nil {
		d.Provider_BR = o.ProviderOperation.BR_amount
	}

	// d.Document_id = o.Document_id
	// d.Merchant_id = o.Merchant_id
	// d.Merchant_account_id = o.Merchant_account_id
	// d.Company_id = o.Company_id
	// d.Project_name = o.Project_name
	// d.Payment_method = o.Payment_method
	// d.Business_type = o.Business_type
	// d.Project_url = o.Project_url
	// d.Crypto_network = o.Crypto_network

	return d
}

func Read_Detailed(db *sqlx.DB, registry_done chan querrys.Args) {

	if db == nil {
		return
	}

	// MERCHANT_NAME + DATE
	Args := <-registry_done

	start_time := time.Now()

	stat := `select payment_id,br_balance_currency 
				from detailed_provider 
				where transaction_completed_at between $1 and $2`

	var result []*Detailed_row
	err := db.Select(&result, stat, Args.DateFrom, Args.DateTo)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	storage.Detailed = map[string]*Detailed_row{}
	for _, v := range result {
		storage.Detailed[v.Payment_id] = v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение detailed_provider из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Detailed))))

}
