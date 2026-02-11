package teams_tradex

import "app/currency"

type Team struct {
	Guid                 string `db:"guid"`
	Name                 string `db:"name"`
	Id                   string `db:"id"`
	Balance_guid         string `db:"provider_balance_guid"`
	Balance_name         string `db:"provider_balance_name"`
	Balance_nickname     string `db:"provider_balance_nickname"`
	Channel_currency_str string `db:"channel_currency"`
	Channel_currency     currency.Currency
}
