package processing

import (
	"slices"
	"strings"
)

var ExponentCurrencies = []string{"JPY", "KRW", "UGX", "VND", "CLP", "XAF", "RWF", "XOF"}

// type Storage struct {
// 	psql                *sqlx.DB
// 	clickhouse          *sqlx.DB
// 	registry            []*Operation
// 	tariffs             []Tariff
// 	crypto              map[int]string
// 	rates               []ProviderOperation
// 	provider_operations map[int]ProviderOperation
// }

type Currency struct {
	Name     string
	Exponent bool
}

func NewCurrency(name string) Currency {
	n := strings.ToUpper(name)
	c := Currency{Name: n, Exponent: slices.Contains(ExponentCurrencies, n)}
	return c
}
