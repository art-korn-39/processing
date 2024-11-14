package crm_dictionary

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

func paymentMethodInsertIntoDB(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Payment_method, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_payment_method()
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
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (payment_method): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Payment_method, 0, batch_len)
	for _, v := range payment_methods {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Payment_method, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка payment_method в Postgres: %v", time.Since(start_time)))
}

func paymentTypeInsertIntoDB(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Payment_type, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_payment_type()
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
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (payment_type): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Payment_type, 0, batch_len)
	for _, v := range payment_types {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Payment_type, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка payment_type в Postgres: %v", time.Since(start_time)))
}
