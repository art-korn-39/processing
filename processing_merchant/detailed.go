package processing_merchant

import "time"

type Detailed_row struct {
	Operation_id             int       `db:"operation_id"`
	Transaction_completed_at time.Time `db:"transaction_completed_at"`
	Document_id              int       `db:"document_id"`

	Merchant_id         int    `db:"merchant_id"`
	Merchant_account_id int    `db:"merchant_account_id"`
	Balance_id          int    `db:"balance_id"`
	Company_id          int    `db:"company_id"`
	Contract_id         int    `db:"contract_id"`
	Project_id          int    `db:"project_id"`
	Provider_id         int    `db:"provider_id"`
	Provider_payment_id string `db:"provider_payment_id"`
	Payment_id          string `db:"payment_id"`

	Provider_name         string `db:"provider_name"`
	Merchant_name         string `db:"merchant_name"`
	Merchant_account_name string `db:"merchant_account_name"`
	Account_bank_name     string `db:"account_bank_name"`
	Project_name          string `db:"project_name"`
	Payment_type          string `db:"payment_type"`
	Country               string `db:"country"`
	Region                string `db:"region"`

	Operation_type string `db:"operation_type"`

	Provider_amount       float64 `db:"provider_amount"`
	Provider_currency_str string  `db:"provider_currency"`

	Msc_amount       float64 `db:"msc_amount"`
	Msc_currency_str string  `db:"msc_currency"`

	Channel_amount       float64 `db:"channel_amount"`
	Channel_currency_str string  `db:"channel_currency"`

	Fee_amount       float64 `db:"fee_amount"`
	Fee_currency_str string  `db:"fee_currency"`

	Balance_amount       float64 `db:"balance_amount"`
	Balance_currency_str string  `db:"balance_currency"`

	Rate                     float64 `db:"rate"`
	SR_channel_currency      float64 `db:"sr_channel_currency"`
	SR_balance_currency      float64 `db:"sr_balance_currency"`
	CheckFee                 float64 `db:"check_fee"`
	Provider_registry_amount float64 `db:"provider_registry_amount"`

	Verification   string `db:"verification"`
	Crypto_network string `db:"crypto_network"`
	Convertation   string `db:"convertation"`

	Provider1C    string `db:"provider_1c"`
	Subdivision1C string `db:"subdivision_1c"`
	RatedAccount  string `db:"rated_account"`

	Tariff_condition_id int       `db:"tariff_id"`
	Tariff_date_start   time.Time `db:"tariff_date_start"`
	Act_percent         float64   `db:"act_percent"`
	Act_fix             float64   `db:"act_fix"`
	Act_min             float64   `db:"act_min"`
	Act_max             float64   `db:"act_max"`
	Range_min           float64   `db:"range_min"`
	Range_max           float64   `db:"range_max"`

	Tariff_rate_percent float64 `db:"tariff_rate_percent"`
	Tariff_rate_fix     float64 `db:"tariff_rate_fix"`
	Tariff_rate_min     float64 `db:"tariff_rate_min"`
	Tariff_rate_max     float64 `db:"tariff_rate_max"`
}

func NewDetailedRow(o *Operation) (d Detailed_row) {

	d = Detailed_row{}

	d.Operation_id = o.Operation_id
	d.Transaction_completed_at = o.Transaction_completed_at
	d.Document_id = o.Document_id
	d.Merchant_id = o.Merchant_id
	d.Merchant_account_id = o.Merchant_account_id
	d.Balance_id = o.Balance_id
	d.Company_id = o.Company_id
	d.Contract_id = o.Contract_id
	d.Project_id = o.Project_id
	d.Provider_payment_id = o.Provider_payment_id
	d.Provider_name = o.Provider_name
	d.Merchant_name = o.Merchant_name
	d.Merchant_account_name = o.Merchant_account_name
	d.Account_bank_name = o.Account_bank_name
	d.Project_name = o.Project_name
	d.Payment_type = o.Payment_type
	d.Operation_type = o.Operation_type
	d.Payment_id = o.Payment_id

	d.Region = o.Country.Region
	if o.Country_code2 != "" {
		d.Country = o.Country_code2
	} else {
		d.Country = o.Country.Code2
	}

	d.Provider_amount = o.Provider_amount
	d.Provider_currency_str = o.Provider_currency.Name
	d.Msc_amount = o.Msc_amount
	d.Msc_currency_str = o.Msc_currency.Name
	d.Channel_amount = o.Channel_amount
	d.Channel_currency_str = o.Channel_currency.Name
	d.Fee_amount = o.Fee_amount
	d.Fee_currency_str = o.Fee_currency.Name
	d.Balance_amount = o.Balance_amount
	d.Balance_currency_str = o.Balance_currency.Name
	d.SR_balance_currency = o.SR_balance_currency
	d.SR_channel_currency = o.SR_channel_currency

	d.CheckFee = o.CheckFee
	d.Verification = o.Verification
	d.Rate = o.Rate
	d.Crypto_network = o.Crypto_network
	d.Provider1C = o.Provider1c

	if o.Tariff != nil {
		t := o.Tariff
		d.Convertation = t.Convertation
		d.Provider1C = t.Provider1C
		d.RatedAccount = t.RatedAccount
		d.Subdivision1C = t.Subdivision1C
		//d.Tariff_condition_id = t.Id
		d.Tariff_date_start = t.DateStart
		d.Act_percent = t.Percent
		d.Act_fix = t.Fix
		d.Act_min = t.Min
		d.Act_max = t.Max
		d.Range_min = t.RangeMIN
		d.Range_max = t.RangeMAX
	}

	if o.Tariff_dragonpay_mid != nil {
		d.Tariff_condition_id = o.Tariff_dragonpay_mid.Id
	} else if o.Tariff != nil {
		d.Tariff_condition_id = o.Tariff.Id
	}

	if o.Tariff_bof != nil {
		t := o.Tariff_bof
		d.Tariff_rate_percent = t.Percent
		d.Tariff_rate_fix = t.Fix
		d.Tariff_rate_min = t.Min
		d.Tariff_rate_max = t.Max
	}

	if o.ProviderOperation != nil {
		d.Provider_registry_amount = o.ProviderOperation.Amount
	}

	return d
}
