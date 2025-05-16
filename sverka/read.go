package sverka

import (
	"app/logs"
	"app/processing_provider"
	"app/querrys"
	"app/util"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type detailed_struct struct {
	opid_map  map[string]*processing_provider.Detailed_row
	payid_map map[string]*processing_provider.Detailed_row
}

func PSQL_read_registry(db *sqlx.DB, provider []string, dateFrom, dateTo time.Time) detailed_struct {

	if db == nil {
		logs.Add(logs.FATAL, `база Postgres не подключена`)
	}

	if len(provider) == 0 {
		logs.Add(logs.FATAL, `пустой массив "provider_name" для чтения detailed provider`)
	}

	start_time := time.Now()

	args := []any{pq.Array(provider), dateFrom, dateTo}

	stat := querrys.Stat_Select_detailed_provider()

	table := []processing_provider.Detailed_row{}

	err := db.Select(&table, stat, args...)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	result := detailed_struct{
		opid_map:  map[string]*processing_provider.Detailed_row{},
		payid_map: map[string]*processing_provider.Detailed_row{},
	}

	for i := range table {
		row := &table[i]

		result.opid_map[strconv.Itoa(row.Operation_id)] = row
		result.payid_map[row.Provider_payment_id] = row
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение таблицы detailed provider из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(table))))

	return result

}
