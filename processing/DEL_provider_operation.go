package processing

// import (
// 	"app/currency"
// 	"time"
// )

// type ProviderOperation struct {
// 	Id                           int       `db:"operation_id"`
// 	Transaction_completed_at     time.Time `db:"transaction_completed_at"`
// 	Transaction_completed_at_day time.Time `db:"transaction_completed_at_day"`
// 	Operation_type               string    `db:"operation_type"`
// 	Country                      string    `db:"country"`
// 	Payment_type                 string    `db:"payment_method_type"`
// 	Merchant_name                string    `db:"merchant_name"`
// 	Rate                         float64   `db:"rate"`
// 	Amount                       float64   `db:"amount"`
// 	Channel_amount               float64   `db:"channel_amount"`

// 	Provider_name         string  `db:"provider_name"`
// 	Merchant_account_name string  `db:"merchant_account_name"`
// 	Provider_payment_id   string  `db:"provider_payment_id"`
// 	Project_url           string  `db:"project_url"`
// 	Operation_status      string  `db:"operation_status"`
// 	Account_number        string  `db:"account_number"`
// 	BR_amount             float64 `db:"br_amount"`

// 	Channel_currency_str  string `db:"channel_currency"`
// 	Provider_currency_str string `db:"provider_currency"`

// 	Channel_currency  currency.Currency
// 	Provider_currency currency.Currency
// }

// func SortRates() {
// 	sort.Slice(
// 		storage.Rates,
// 		func(i int, j int) bool {
// 			return storage.Rates[i].Transaction_completed_at.After(storage.Rates[j].Transaction_completed_at)
// 		},
// 	)
// }
