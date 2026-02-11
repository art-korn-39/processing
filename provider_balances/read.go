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
	data_nickname = map[string]*Balance{}
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

		data_nickname[balance.Nickname] = &balance
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение балансов провайдеров: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(slice_balances))))

}
