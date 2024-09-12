package kgx

import "app/currency"

var data []KGX_line

type KGX_line struct {
	Balance          string
	Operation_type   string
	Balance_currency currency.Currency
	Payment_type     string
	Provider1c       string
}
