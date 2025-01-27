package provider_balances

import (
	"app/currency"
	"time"
)

type Balance struct {
	Name                string    `db:"provider_balance"`
	GUID                string    `db:"guid"`
	Contractor          string    `db:"contractor"`
	Provider_name       string    `db:"provider_name"`
	Provider_id         int       `db:"provider_id"`
	Balance_code        string    `db:"balance_code"`
	Legal_entity        string    `db:"legal_entity"`
	Merchant_account    string    `db:"merchant_account"`
	Merchant_account_id int       `db:"merchant_account_id"`
	Date_start          time.Time `db:"date_start"`
	Date_finish         time.Time `db:"date_finish"`

	Convertation    string `db:"convertation"`
	Convertation_id int    `db:"convertation_id"`

	Key_record string `db:"key_record"`

	Balance_currency_str string `db:"balance_currency"`
	Balance_currency     currency.Currency
}
