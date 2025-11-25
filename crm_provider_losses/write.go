package crm_provider_losses

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

func operationsInsertIntoDB(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Operation, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_crm_provider_losses()
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

				_, err := db.NamedExec(stat, v)
				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (provider_losses): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Operation, 0, batch_len)
	for _, v := range operations {
		batch = append(batch, *v)
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

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка provider losses в Postgres: %v", time.Since(start_time)))
}
