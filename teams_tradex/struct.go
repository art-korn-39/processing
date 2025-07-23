package teams_tradex

type Team struct {
	Guid                      string `db:"guid"`
	Name                      string `db:"name"`
	Id                        string `db:"id"`
	Provider_balance_guid     string `db:"provider_balance_guid"`
	Provider_balance_name     string `db:"provider_balance_name"`
	Provider_balance_nickname string `db:"provider_balance_nickname"`
}
