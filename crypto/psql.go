package crypto

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func PSQL_read_registry(db *sqlx.DB, registry_done chan querrys.Args) {

	if db == nil {
		return
	}

	Args := <-registry_done

	start_time := time.Now()

	stat := querrys.Stat_Select_crypto()

	slice_operations := []Operation{}

	err := db.Select(&slice_operations, stat, Args.DateFrom, Args.DateTo)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, operation := range slice_operations {

		// operation.Crypto_currency = currency.New(operation.Crypto_currency_str)
		// operation.Payment_currency = currency.New(operation.Payment_currency_str)

		operation.StartingFill()
		Registry[operation.Id] = &operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение криптовалютных операций: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(Registry))))

}

func PSQL_get_operation(db *sqlx.DB, id int) *Operation {

	if db == nil {
		return nil
	}

	stat := querrys.Stat_Select_crypto_operation()

	var operation Operation

	err := db.Get(&operation, stat)
	if err != nil {
		return nil
	}

	Registry[operation.Id] = &operation

	return &operation

}
