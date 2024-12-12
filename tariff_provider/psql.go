package tariff_provider

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func Read_PSQL_Tariffs(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_tariffs_provider()

	err := db.Select(&data, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, tariff := range data {

		tariff.StartingFill()

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение тарифов провайдера из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(data))))

}
