package currency

import (
	"slices"
	"strings"
)

var exponent = []string{"JPY", "KRW", "UGX", "VND", "CLP", "XAF", "RWF", "XOF"}

type Currency struct {
	Name     string
	Exponent bool
}

func New(name string) Currency {

	n := strings.ToUpper(name)
	c := Currency{Name: n, Exponent: slices.Contains(exponent, n)}
	return c
}
