package currency

import (
	"slices"
	"strings"
)

var exponent = []string{"JPY", "KRW", "UGX", "VND", "CLP", "XAF", "RWF", "XOF", "GNF", "PYG"}
var crypto = []string{"BTC", "ETH", "XRP", "LTC", "XMR", "ADA", "DOT", "BNB", "BCH"}

// 32 bytes
type Currency struct {
	Name     string
	Exponent bool // без деления на 100
	Crypto   bool
}

func (obj Currency) IsNil() bool {
	return obj == Currency{}
}

func (obj Currency) Exist() bool {
	return obj != Currency{}
}

func New(name string) Currency {

	n := strings.ToUpper(name)

	crypto := slices.Contains(crypto, n)

	return Currency{Name: n,
		Exponent: slices.Contains(exponent, n) || crypto,
		Crypto:   crypto,
	}

}

func (c *Currency) GetAccuracy(baseAccuracy int) int {

	if c.Crypto {
		return 8
	} else {
		return baseAccuracy
	}

}
