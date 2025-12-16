package balances_tradex

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	data []*Balance
)

func init() {
	data = []*Balance{}
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_balances_tradex()

	//slice_balances := []Balance{}

	err := db.Select(&data, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	// for _, team := range data {

	// 	map_team_names[team.Name] = &team

	// }

	logs.Add(logs.INFO, fmt.Sprintf("Чтение balances tradex: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(data))))

}

func GetProvider1c(balance, provider_currency, country_code2, payment_type string) string {

	for _, v := range data {

		if v.Balance_nickname != "" && v.Balance_nickname != balance {
			continue
		}

		if v.Balance_currency != "" && v.Balance_currency != provider_currency {
			continue
		}

		if v.Country != "" && v.Country != country_code2 {
			continue
		}

		if v.Payment_type_name != "" && v.Payment_type_name != payment_type {
			continue
		}

		return v.Provider1c

	}

	return ""
}
