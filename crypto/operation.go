package crypto

import (
	"app/currency"
	"time"
)

type Operation struct {
	Id                   int       `db:"operation_id"`
	Network              string    `db:"network"`
	Created_at           time.Time `db:"created_at"`
	Created_at_day       time.Time `db:"created_at_day"`
	Operation_type       string    `db:"operation_type"`
	Payment_amount       float64   `db:"payment_amount"`
	Payment_currency_str string    `db:"payment_currency"`
	Crypto_amount        float64   `db:"crypto_amount"`
	Crypto_currency_str  string    `db:"crypto_currency"`

	Payment_currency currency.Currency
	Crypto_currency  currency.Currency
}
