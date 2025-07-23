package crm_chargeback

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

func chargebacksInsertIntoDB(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Chargeback, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_chargeback()
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
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (chargeback): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Chargeback, 0, batch_len)
	for _, v := range chargebacks {
		batch = append(batch, *v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Chargeback, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка chargeback в Postgres: %v", time.Since(start_time)))
}

func operationsInsertIntoDB(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Operation, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_chargeback_operations()
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
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (chargeback operations): ", err))
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

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка chargeback operations в Postgres: %v", time.Since(start_time)))
}
