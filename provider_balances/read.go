package provider_balances

import (
	"app/currency"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	data_maid map[string]*Balance
)

func init() {
	data_maid = map[string]*Balance{}
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_provider_balances()

	slice_balances := []Balance{}

	err := db.Select(&slice_balances, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, balance := range slice_balances {

		balance.Balance_currency = currency.New(balance.Balance_currency_str)

		if balance.Type == "IN-OUT" {
			hash := fmt.Sprint(balance.Provider_id, balance.Merchant_account_id, balance.Balance_currency.Name, "IN")
			data_maid[hash] = &balance
			hash = fmt.Sprint(balance.Provider_id, balance.Merchant_account_id, balance.Balance_currency.Name, "OUT")
			data_maid[hash] = &balance
		} else {
			hash := fmt.Sprint(balance.Provider_id, balance.Merchant_account_id, balance.Balance_currency.Name, balance.Type)
			data_maid[hash] = &balance
		}

		hash2 := fmt.Sprint(balance.Provider_id, balance.Merchant_account_id)
		data_maid[hash2] = &balance

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение балансов провайдеров из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(slice_balances))))

}

func GetBalance(provider_id, ma_id int, balance_currency, balance_type string) (*Balance, bool) {
	hash := fmt.Sprint(provider_id, ma_id, balance_currency, balance_type)
	b, ok := data_maid[hash]
	return b, ok
}

func GetByProvierAndMA(provider_id, ma_id int) (*Balance, bool) {
	hash := fmt.Sprint(provider_id, ma_id)
	b, ok := data_maid[hash]
	return b, ok
}
