package provider_balances

import (
	"app/currency"
	"time"
)

type data map[int]*LinkedBalance

type Balance struct {
	Name                string    `db:"provider_balance"`
	Nickname            string    `db:"nickname"`
	GUID                string    `db:"guid"`
	Extra_balance_guid  string    `db:"extra_balance_guid"`
	Type                string    `db:"type"` //IN, OUT, IN-OUT
	Contractor          string    `db:"contractor"`
	Provider_name       string    `db:"provider_name"`
	Provider_id         int       `db:"provider_id"`
	Balance_code        string    `db:"balance_code"`
	Legal_entity        string    `db:"legal_entity"`
	Merchant_account    string    `db:"merchant_account"`
	Merchant_account_id int       `db:"merchant_account_id"`
	Date_start          time.Time `db:"date_start"`
	Date_finish         time.Time `db:"date_finish"`

	Convertation    string `db:"convertation"`
	Convertation_id int    `db:"convertation_id"`

	Key_record string `db:"key_record"`

	Balance_currency_str string `db:"balance_currency"`
	Balance_currency     currency.Currency
}

var (
	data_maid     data
	data_guid     map[string]*Balance
	data_nickname map[string]*Balance
)

type LinkedBalance struct {
	Balance *Balance
	Next    *LinkedBalance
}

type Operation interface {
	GetTime(string) time.Time
	GetInt(string) int
	GetString(string) string
}

func (r data) Set(b Balance) {

	val, ok := r[b.Merchant_account_id]
	if ok {
		for {
			if val.Next == nil { // дошли до последнего
				val.Next = &LinkedBalance{
					Balance: &b,
					Next:    nil,
				}
				break
			}
			val = val.Next
		}
	} else {
		r[b.Merchant_account_id] = &LinkedBalance{
			Balance: &b,
			Next:    nil,
		}
	}

}

func GetBalance(op Operation, balance_currency string) (*Balance, bool) {

	ma_id := op.GetInt("Merchant_account_id")
	provider_id := op.GetInt("Provider_id")
	balance_type := op.GetString("Balance_type")
	date := op.GetTime("Operation_created_at")

	val, ok := data_maid[ma_id]
	if ok {
		for {
			b := val.Balance
			if b.Provider_id == provider_id &&
				(b.Balance_currency.Name == balance_currency || balance_currency == "") &&
				(b.Type == balance_type || b.Type == "IN-OUT" || balance_type == "NULL") {

				if b.Date_start.Before(date) && (b.Date_finish.IsZero() || b.Date_finish.After(date)) {
					return b, true
				}
			}

			if val.Next == nil {
				break
			}
			val = val.Next
		}
	}

	return nil, false

}

func GetBalanceByProviderAndMA(ma_id, provider_id int) (*Balance, bool) {

	val, ok := data_maid[ma_id]
	if ok {
		for {
			b := val.Balance
			if b.Provider_id == provider_id {
				return b, true
			}

			if val.Next == nil {
				break
			}
			val = val.Next
		}
	}

	return nil, false

}

func GetbalanceByGUID(guid string) (*Balance, bool) {

	val, ok := data_guid[guid]
	return val, ok

}

func GetBalanceByNickname(nickname string) (*Balance, bool) {

	val, ok := data_nickname[nickname]
	return val, ok

}
