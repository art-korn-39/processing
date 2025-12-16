package dragonpay

import (
	"app/currency"
	"time"
)

// 208 bytes
type Operation struct {
	Id           int       `db:"operation_id"`
	Provider1c   string    `db:"provider"`
	Create_date  time.Time `db:"create_date"`
	Settle_date  time.Time `db:"settle_date"`
	Refno        string    `db:"refno"`
	Currency_str string    `db:"currency"`
	Amount       float64   `db:"amount"`
	Endpoint_id  string    `db:"endpoint_id"`
	Fee_amount   float64   `db:"fee_amount"`
	Description  string    `db:"description"`
	Message      string    `db:"message"`

	Currency currency.Currency
}

type Accord struct {
	Endpoint_id     string `db:"endpoint_id"`
	Provider1c      string `db:"provider1c"`
	Payment_type    string `db:"payment_type"`
	Payment_type_id int    `db:"payment_type_id"`
}
