package providers_1c

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	registry      Registry
	map_providers map[string]*Provider1c
)

func init() {
	registry = make(map[string]*LinkedProvider1c, 100000)
	//map_providers = map[string]*Provider1c{}
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_providers_1c()

	slice_providers1c := []Provider1c{}

	err := db.Select(&slice_providers1c, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, provider1c := range slice_providers1c {

		// currency := provider1c.Currency
		// if currency == "USDT" {
		// 	currency = "USD"
		// }

		// key := fmt.Sprint(provider1c.Provider_guid, provider1c.Payment_type_name, currency, provider1c.Merchant_id)
		// map_providers[key] = &provider1c

		registry.Set(provider1c)

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение поставщиков 1С ФИН: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(slice_providers1c))))

}

func GetProvider1c_temp(contractor_guid, payment_type, currency string, merchant_id int) (*Provider1c, bool) {

	if currency == "USDT" {
		currency = "USD"
	}

	key1 := fmt.Sprint(contractor_guid, payment_type, currency, merchant_id)
	key2 := fmt.Sprint(contractor_guid, payment_type, currency, 0)

	val, ok := map_providers[key1]
	if ok {
		return val, true
	} else {
		val, ok = map_providers[key2]
		if ok {
			return val, true
		}
	}

	return nil, false

}
