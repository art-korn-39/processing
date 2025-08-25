package balances_tradex

type Balance struct {
	Guid              string `db:"guid"`
	Name              string `db:"name"`
	Country           string `db:"issuer_country"`
	Provider1c        string `db:"provider1c"`
	Payment_type_name string `db:"payment_type_name"`
	Payment_type_id   int    `db:"payment_type_id"`
	Balance_guid      string `db:"provider_balance_guid"`
	Balance_name      string `db:"provider_balance_name"`
	Balance_nickname  string `db:"provider_balance_nickname"`
	Balance_currency  string `db:"provider_balance_currency"`
}
