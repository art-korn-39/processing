package decline

import (
	"app/config"
	"app/file"
	"app/logs"
	"app/querrys"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func InsertIntoDB(db *sqlx.DB, decline_operations map[int]Operation, files []*file.FileInfo) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Operation, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_decline()
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
	batch := make([]Operation, 0, batch_len)
	for _, v := range decline_operations {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Operation, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	for _, f := range files {
		f.InsertIntoDB(db, 0)
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка decline в Postgres: %v (%d строк)", time.Since(start_time), len(decline_operations)))
}
