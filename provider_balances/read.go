package provider_balances

import (
	"app/currency"
	"app/logs"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	data_maid map[int]*Balance
)

func init() {
	data_maid = map[int]*Balance{}
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := `SELECT * FROM provider_balances`

	slice_balances := []Balance{}

	err := db.Select(&slice_balances, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, balance := range slice_balances {

		balance.Balance_currency = currency.New(balance.Balance_currency_str)

		data_maid[balance.Merchant_account_id] = &balance
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение балансов провайдеров из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(data_maid))))

}

func GetByMAID(id int) (*Balance, bool) {
	b, ok := data_maid[id]
	return b, ok
}
