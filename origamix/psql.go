package origamix

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

func PSQL_read_registry(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_bof_origamix()

	slice_operations := []Operation{}

	err := db.Select(&slice_operations, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, operation := range slice_operations {

		//operation.Crypto_currency = currency.New(operation.Crypto_currency_str)
		//operation.Payment_currency = currency.New(operation.Payment_currency_str)

		Registry[operation.Operation_id] = operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций origamix из Postgres: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(Registry))))

}

func insert_into_db(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	chan_operations := make(chan []Operation, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_bof_origamix()
	_, err := db.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for v := range chan_operations {

				_, err := db.NamedExec(stat, v)

				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД: ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Operation, 0, batch_len)
	for _, v := range Registry {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			chan_operations <- batch
			batch = make([]Operation, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		chan_operations <- batch
	}

	close(chan_operations)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка origamix в Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Registry))))

}
