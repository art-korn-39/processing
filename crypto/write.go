package crypto

import (
	"app/config"
	"app/logs"
	"app/processing"
	"app/querrys"
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

	channel := make(chan []processing.CryptoOperation, 1000)

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
			for v := range channel {

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
	batch := make([]processing.CryptoOperation, 0, batch_len)
	for _, v := range crypto_operations {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]processing.CryptoOperation, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Загрузка криптовалютных операций в Postgres: %v", time.Since(start_time)))
	logs.Add(logs.REGL, fmt.Sprintf("Загрузка crypto в Postgres: %v (%d строк)", time.Since(start_time), len(crypto_operations)))

}
