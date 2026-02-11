package processing_provider

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"strconv"
	"strings"

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

	stat := `
	SELECT 
		IFNULL(operation__operation_id, 0) AS operation_id,
		IFNULL(billing__balance_id, 0) AS balance_id	
	FROM reports
	WHERE 
		operation__operation_id IN ('$1')`

	slice_id := make([]string, 0, len(Args.ID))
	for _, v := range Args.ID {
		slice_id = append(slice_id, strconv.Itoa(v))
	}

	id_str := strings.Trim(strings.Join(slice_id, "','"), "[]")
	stat = strings.ReplaceAll(stat, "$1", id_str)

	result := []Clickhouse_operation{}
	err := db.Select(&result, stat)
	if err != nil {
		logs.Add(logs.ERROR, err)
		return
	}

	for _, v := range result {
		bof_clickhouse_data[v.Operation_id] = v
	}

}
