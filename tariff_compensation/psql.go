package tariff_compensation

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func Read_PSQL_Tariffs(db *sqlx.DB, processing_merchant bool) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_tariffs_compensations()

	err := db.Select(&data, stat, processing_merchant)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, tariff := range data {

		tariff.StartingFill()

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение прочих тарифов: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(data))))

}
