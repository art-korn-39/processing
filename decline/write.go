package decline

import (
	"app/config"
	"app/logs"
	"app/processing"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
)

func InsertIntoDB() {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []DeclineOperation, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := processing.Stat_Insert_decline()
	_, err := db.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for v := range channel {

				tx, _ := db.Beginx()

				sliceID := make([]int, 0, len(v))
				for _, row := range v {
					sliceID = append(sliceID, row.Operation_id)
				}

				_, err := tx.Exec("delete from decline where operation_id = ANY($1);", pq.Array(sliceID))
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
	batch := make([]DeclineOperation, 0, batch_len)
	for _, v := range decline_operations {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]DeclineOperation, 0, batch_len)
		}
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Загрузка операций decline в Postgres: %v", time.Since(start_time)))
}
