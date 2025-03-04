package provider_registry

import (
	"app/currency"
	"time"
)

type bof_operation interface {
	Get_Channel_currency() currency.Currency
	Get_Tariff_balance_currency() currency.Currency
	GetBool(string) bool
	GetTime(string) time.Time
	GetInt(string) int
	GetFloat(string) float64
	GetString(string) string
}

func FindRateForOperation(o bof_operation) float64 {

	for _, r := range rates {

		if r.Transaction_completed_at.Before(o.GetTime("Transaction_completed_at")) &&
			r.Operation_type == o.GetString("Operation_type") &&
			r.Country == o.GetString("Country") &&
			r.Payment_type == o.GetString("Payment_type") &&
			r.Merchant_name == o.GetString("Merchant_name") &&
			r.Channel_currency == o.Get_Channel_currency() &&
			r.Provider_currency == o.Get_Tariff_balance_currency() {
			return r.Rate
		}
	}

	return 0

}
