package origamix

import (
	"app/currency"
	"time"
)

type Operation struct {
	Operation_id          int       `db:"operation_id"`
	Payment_id            int       `db:"payment_id"`
	Merchant_id           int       `db:"merchant_id"`
	Merchant_account_name string    `db:"merchant_account_name"`
	Payment_method        string    `db:"payment_method"`
	Payment_type          string    `db:"payment_type"`
	Ps_id                 int       `db:"ps_id"`
	Ps_account            string    `db:"ps_account"`
	Ps_provider           string    `db:"ps_provider"`
	Amount_init           float64   `db:"amount_init"`
	Amount_processed      int       `db:"amount_processed"`
	Currency_str          string    `db:"currency"`
	Status                string    `db:"status"`
	Ps_code               string    `db:"ps_code"`
	Ps_message            string    `db:"ps_message"`
	Result_code           string    `db:"result_code"`
	Result_message        string    `db:"result_message"`
	Created_at            time.Time `db:"created_at"`
	Updated_at            time.Time `db:"updated_at"`

	Currency currency.Currency
}

func (o *Operation) StartingFill() {
	o.Currency = currency.New(o.Currency_str)
}
