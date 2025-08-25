package teams_tradex

type Team struct {
	Guid             string `db:"guid"`
	Name             string `db:"name"`
	Id               string `db:"id"`
	Balance_guid     string `db:"provider_balance_guid"`
	Balance_name     string `db:"provider_balance_name"`
	Balance_nickname string `db:"provider_balance_nickname"`
}
