package processing_provider

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

func init() {
	bof_clickhouse_data = make(map[int]Clickhouse_operation, 10000)
}

var (
	bof_clickhouse_data map[int]Clickhouse_operation
)

type Clickhouse_operation struct {
	Operation_id int `db:"operation_id"`
	Balance_id   int `db:"balance_id"`
}

func GetDataFromClickhouse(db *sqlx.DB, registry_done chan querrys.Args) {

	if config.Get().Registry.Storage == config.Clickhouse {
		return
	}

	var Args querrys.Args
	if registry_done != nil {
		Args = <-registry_done
	}

	if len(Args.ID) == 0 {
		return
	}

	start_time := time.Now()

	stat_base := `
	SELECT 
		IFNULL(operation__operation_id, 0) AS operation_id,
		IFNULL(billing__balance_id, 0) AS balance_id	
	FROM reports
	WHERE 
		operation__operation_id IN ('$1');`

	slice_id := make([]string, 0, len(Args.ID))
	for _, v := range Args.ID {
		slice_id = append(slice_id, strconv.Itoa(v))
	}

	batchSize := 10000
	if len(slice_id) > 100000 {
		_, err := db.Exec("SET max_query_size = 1048576")
		if err != nil {
			logs.Add(logs.ERROR, fmt.Sprintf("Failed to set max_query_size: %v", err))
			return
		}
		batchSize = 50000
	}

	result := make([]Clickhouse_operation, 0, len(slice_id))

	// Обрабатываем батчами
	for i := 0; i < len(slice_id); i += batchSize {
		end := i + batchSize
		if end > len(slice_id) {
			end = len(slice_id)
		}

		batch := slice_id[i:end]

		id_str := strings.Trim(strings.Join(batch, "','"), "[]")
		stat := strings.ReplaceAll(stat_base, "$1", id_str)

		batchResult := []Clickhouse_operation{}
		err := db.Select(&batchResult, stat)
		if err != nil {
			logs.Add(logs.ERROR, err)
			return
		}

		result = append(result, batchResult...)

	}

	for _, v := range result {
		bof_clickhouse_data[v.Operation_id] = v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение доп. операций из CH: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(bof_clickhouse_data))))

}
