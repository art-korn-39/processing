package crypto

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func insert_into_db(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	chan_operations := make(chan []Operation, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_crypto()
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

				tx, _ := db.Beginx()

				sliceID := make([]int, 0, len(v))
				for _, row := range v {
					sliceID = append(sliceID, row.Id)
				}

				_, err := tx.Exec("delete from crypto where operation_id = ANY($1);", pq.Array(sliceID))
				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint("ошибка при удалении: ", err))
					tx.Rollback()
					continue
				}

				_, err = tx.NamedExec(stat, v)
				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД: ", err))
					tx.Rollback()
				} else {
					tx.Commit()
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

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка crypto в Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Registry))))

}
