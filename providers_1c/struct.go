package providers_1c

type Provider1c struct {
	Guid                  string `db:"guid"`
	Name                  string `db:"name"`
	Payment_method_name   string `db:"payment_method_name"`
	Payment_method_id     int    `db:"payment_method_id"`
	Payment_type_name     string `db:"payment_type_name"`
	Payment_type_guid     string `db:"payment_type_guid"`
	Payment_type_id       int    `db:"payment_type_id"`
	Provider_name         string `db:"provider_name"`
	Provider_guid         string `db:"provider_guid"`
	Currency              string `db:"currency"`
	Merchant_id           int    `db:"merchant_id"`
	Provider_balance_guid string `db:"provider_balance_guid"`
}

type Registry map[string]*LinkedProvider1c

type LinkedProvider1c struct {
	Provider1c *Provider1c
	Next       *LinkedProvider1c
}

func (r Registry) Set(p Provider1c) {

	val, ok := r[p.Provider_guid]
	if ok {
		for {
			if val.Next == nil { // дошли до последнего
				val.Next = &LinkedProvider1c{
					Provider1c: &p,
					Next:       nil,
				}
				break
			}
			val = val.Next
		}
	} else {
		r[p.Provider_guid] = &LinkedProvider1c{
			Provider1c: &p,
			Next:       nil,
		}
	}

}

func GetProvider1c(contractor_guid, payment_type, currency, provider_balance_guid string, merchant_id int) (*Provider1c, bool) {

	var currency2 string
	if currency == "USDT" {
		currency2 = "USD"
	}

	val, ok := registry[contractor_guid]
	if ok {
		for {

			p := val.Provider1c

			if p.Payment_type_name == payment_type &&
				(p.Currency == currency || p.Currency == currency2) &&
				(p.Merchant_id == merchant_id || p.Merchant_id == 0) &&
				(p.Provider_balance_guid == provider_balance_guid || p.Provider_balance_guid == "") {
				return p, true
			}

			if val.Next == nil {
				break
			}
			val = val.Next
		}
	}

	return nil, false

}
