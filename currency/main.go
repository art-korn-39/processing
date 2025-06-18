package currency

import (
	"slices"
	"strings"
)

var exponent = []string{"JPY", "KRW", "UGX", "VND", "CLP", "XAF", "RWF", "XOF", "GNF", "PYG"}

type Currency struct {
	Name     string
	Exponent bool
}

func New(name string) Currency {

	n := strings.ToUpper(name)
	return Currency{Name: n, Exponent: slices.Contains(exponent, n)}

}
