package processing_merchant

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type detailed_provider struct {
	Operation_id   int     `db:"operation_id"`
	Balance_amount float64 `db:"balance_amount"`
	Rate           float64 `db:"rate"`
	BR_amount      float64 `db:"br_balance_currency"`
}

var (
	data_detailed_provider map[int]*detailed_provider
)

func init() {
	data_detailed_provider = map[int]*detailed_provider{}
}

func PSQL_read_detailed_provider(db *sqlx.DB, registry_done chan querrys.Args) {

	if db == nil {
		return
	}

	Args := <-registry_done

	start_time := time.Now()

	stat := `select operation_id,balance_amount,rate,br_balance_currency 
			from detailed_provider
			where lower(merchant_name) = ANY($1) AND transaction_completed_at BETWEEN $2 AND $3`

	slice_detailed := []detailed_provider{}

	err := db.Select(&slice_detailed, stat, pq.Array(Args.Merhcant), Args.DateFrom, Args.DateTo)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, detailed := range slice_detailed {

		data_detailed_provider[detailed.Operation_id] = &detailed

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение detailed_provider операций из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(slice_detailed))))

}
