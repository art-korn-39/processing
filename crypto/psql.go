package crypto

import (
	"app/currency"
	"app/logs"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func PSQL_read_registry(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := `SELECT * FROM crypto`

	slice_operations := []Operation{}

	err := db.Select(&slice_operations, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, operation := range slice_operations {

		operation.Crypto_currency = currency.New(operation.Crypto_currency_str)
		operation.Payment_currency = currency.New(operation.Payment_currency_str)

		Registry[operation.Id] = operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение криптовалютных операций из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Registry))))

}
