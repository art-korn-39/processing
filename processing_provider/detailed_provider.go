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

type detailed_provider struct {
	Operation_id             int       `db:"operation_id"`
	Transaction_completed_at time.Time `db:"transaction_completed_at"`
	Operation_status         string    `db:"operation_status"`
	Channel_amount           float64   `db:"channel_amount"`
	Balance_amount           float64   `db:"balance_amount"`
	//Fee_amount               float64   `db:"fee_amount"`
	BR_balance_currency float64 `db:"br_balance_currency"`
	//BR_channel_currency      float64   `db:"br_channel_currency"`
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

	stat := `SELECT operation_id,operation_status,channel_amount,br_balance_currency,
			balance_amount,transaction_completed_at
			FROM detailed_provider
			WHERE (provider_id = ANY($1) OR provider_id = 0) 
			AND transaction_completed_at BETWEEN $2 AND $3
			AND is_final = true
			AND correction_type_id != 4`

	slice_detailed := []detailed_provider{}

	err := db.Select(&slice_detailed, stat, pq.Array(Args.Provider_id), Args.DateFrom, Args.DateTo)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, detailed := range slice_detailed {

		v, ok := data_detailed_provider[detailed.Operation_id]
		if ok {
			if detailed.Transaction_completed_at.After(v.Transaction_completed_at) {
				data_detailed_provider[detailed.Operation_id] = &detailed
			}
		} else {
			data_detailed_provider[detailed.Operation_id] = &detailed
		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение detailed_provider операций: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(slice_detailed))))

}
