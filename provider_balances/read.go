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

func init() {
	data_maid = data{}
	data_guid = map[string]*Balance{}
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

		data_maid.Set(balance)

		data_guid[balance.GUID] = &balance

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение балансов провайдеров из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(slice_balances))))

}

// func GetBalance(provider_id, ma_id int, balance_currency, balance_type string) (*Balance, bool) {
// 	hash := fmt.Sprint(provider_id, ma_id, balance_currency, balance_type)
// 	b, ok := data_maid[hash]
// 	return b, ok
// }

// func GetByProvierAndMA(provider_id, ma_id int) (*Balance, bool) {
// 	hash := fmt.Sprint(provider_id, ma_id)
// 	b, ok := data_maid[hash]
// 	return b, ok
// }
