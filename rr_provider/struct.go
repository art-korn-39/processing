package rr_provider

import (
	"time"
)

// provider id merchant id balance guid ma id
// dates

type Operation interface {
	GetBool(string) bool
	GetTime(string) time.Time
	GetInt(string) int
	GetFloat(string) float64
	GetString(string) string
}

// 176 bytes
type Tariff struct {
	Contract_name         string    `db:"contract_name"`
	Contract_guid         string    `db:"contract_guid"`
	Provider_id           int       `db:"provider_id"`
	DateStart             time.Time `db:"date_start"`
	DateFinish            time.Time `db:"date_finish"`
	Balance_guid          string    `db:"balance_guid"`
	Balance_name          string    `db:"balance_name"`
	Merchant_account_name string    `db:"merchant_account_name"`
	Merchant_account_id   int       `db:"merchant_account_id"`
	Amount_days           int       `db:"amount_days"`
	Percent               float64   `db:"percent"`
	Limit                 float64   `db:"limit_amount"`
}
