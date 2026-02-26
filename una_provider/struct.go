package una_provider

import (
	"time"
)

type Operation interface {
	GetBool(string) bool
	GetTime(string) time.Time
	GetInt(string) int
	GetFloat(string) float64
	GetString(string) string
}

type Tariff struct {
	Contract_name   string    `db:"contract_name"`
	Contract_guid   string    `db:"contract_guid"`
	Bof_id          int       `db:"bof_id"`
	DateStart       time.Time `db:"date_start"`
	DateFinish      time.Time `db:"date_finish"`
	Contractor_name string    `db:"contractor_name"`
	Contractor_guid string    `db:"contractor_guid"`

	Provider_id  int    `db:"provider_id"`
	Balance_guid string `db:"balance_guid"`
	Balance_name string `db:"balance_name"`
	Amount_days  int    `db:"amount_days"`
}
