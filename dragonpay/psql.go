package dragonpay

import (
	"app/currency"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func PSQL_read_registry(db *sqlx.DB, handbookOnly bool) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_dragonpay_handbook()
	slice_rows := []Accord{}
	err := db.Select(&slice_rows, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, row := range slice_rows {
		handbook[row.Endpoint_id] = row
	}

	if handbookOnly {
		return
	}

	stat = querrys.Stat_Select_dragonpay()
	slice_operations := []Operation{}
	err = db.Select(&slice_operations, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, operation := range slice_operations {

		operation.Currency = currency.New(operation.Currency_str)

		registry[operation.Id] = operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций dragonpay из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(registry))))

}
