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
	map_providers map[string]*Provider1c
)

func init() {
	map_providers = map[string]*Provider1c{}
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

		key := fmt.Sprint(provider1c.Provider_guid, provider1c.Payment_type_name)
		map_providers[key] = &provider1c

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение поставщиков 1С ФИН из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(slice_providers1c))))

}

func GetProvider1c(contractor_guid, payment_type string) (*Provider1c, bool) {

	key := fmt.Sprint(contractor_guid, payment_type)

	val, ok := map_providers[key]
	if ok {
		return val, true
	}

	return nil, false

}
