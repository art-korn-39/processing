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

func PSQL_read_registry(db *sqlx.DB, handbookOnly bool, registry_done chan querrys.Args) {

	if db == nil {
		return
	}

	var Args querrys.Args
	if registry_done != nil {
		Args = <-registry_done
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
	err = db.Select(&slice_operations, stat, Args.DateFrom, Args.DateTo)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, operation := range slice_operations {

		operation.Currency = currency.New(operation.Currency_str)

		registry[operation.Id] = &operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций dragonpay: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(registry))))

}

func PSQL_get_operation(db *sqlx.DB, id int) *Operation {

	if db == nil {
		return nil
	}

	stat := querrys.Stat_Select_dragonpay_operation()

	var operation Operation

	err := db.Get(&operation, stat)
	if err != nil {
		return nil
	}

	registry[operation.Id] = &operation

	return &operation

}
