package tariff_merchant

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func Read_PSQL_Tariffs(db *sqlx.DB, registry_done <-chan querrys.Args) {

	if db == nil {
		return
	}

	// MERCHANT_NAME + DATE
	Args := <-registry_done

	start_time := time.Now()

	stat := querrys.Stat_Select_tariffs_merchant()

	err := db.Select(&data, stat, pq.Array(Args.Merchant_id))
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, tariff := range data {

		tariff.StartingFill()

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение тарифов мерчантов из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(data))))

}
