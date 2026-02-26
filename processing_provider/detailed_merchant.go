package processing_provider

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type detailed_merchant struct {
	Operation_id int `db:"operation_id"`
	IsTestId     int `db:"is_test_id"`
}

var (
	data_detailed_merchant map[int]*detailed_merchant
)

func init() {
	data_detailed_merchant = map[int]*detailed_merchant{}
}

func PSQL_read_detailed_merchant(db *sqlx.DB, registry_done chan querrys.Args) {

	if db == nil {
		return
	}

	// переписать только на provider_id = ANY($1), когда будет будет заполнено поле

	Args := <-registry_done

	start_time := time.Now()

	stat := `SELECT operation_id, is_test_id 
			FROM detailed
			WHERE (provider_id = ANY($1) OR provider_id = 0) 
			AND transaction_completed_at BETWEEN $2 AND $3`

	slice_detailed := []detailed_merchant{}

	err := db.Select(&slice_detailed, stat, pq.Array(Args.Provider_id), Args.DateFrom, Args.DateTo)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, detailed := range slice_detailed {

		data_detailed_merchant[detailed.Operation_id] = &detailed

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение detailed операций: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(slice_detailed))))

}
