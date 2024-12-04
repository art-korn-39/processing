package conversion

import (
	"app/config"
	"app/file"
	"app/logs"
	"app/provider_registry"
	"app/querrys"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func WriteIntoDB(chan_operations chan provider_registry.Operation, chan_readed_files chan *file.FileInfo) {

	if db == nil {
		logs.Add(logs.FATAL, "no connection to postgres")
		return
	}

	var wg sync.WaitGroup

	//1М rows, чтобы читающие горутины на паузу не встали
	chan_batches := make(chan Batch, 10000) //1000

	batch_len := 1500 //1500 20 fileds

	statement := querrys.Stat_Insert_provider_registry()

	_, err := db.PrepareNamed(statement)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	var count_rows int64

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			//var deadlock bool
			for b := range chan_batches {

				// tx, err := db.Beginx()
				// if err != nil {
				// 	logs.Add(logs.ERROR, err)
				// 	continue
				// }

				v := b.Get()

				_, err = db.NamedExec(statement, v)

				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprintf("не удалось записать в БД: %v, date: %s, provider: %s, merchant: %s", err, v[0].Transaction_completed_at_day.Format(time.DateOnly), v[0].Provider_name, v[0].Merchant_name))
					//deadlock = true
					//tx.Rollback()

					_, err = db.NamedExec(statement, v)
					if err == nil {
						logs.Add(logs.ERROR, "повторная запись -> успешно")
					}

				} else {
					atomic.AddInt64(&count_rows, int64(len(v)))
					// 	err = tx.Commit()
					// 	if err != nil {
					// 		logs.Add(logs.ERROR, "ошибка при commit:"+err.Error())
					// 	}
					// 	if deadlock {
					// 		logs.Add(logs.ERROR, "commit done")
					// 	}
				}

			}
		}()
	}

	go func() {
		i := 1
		batch := Batch{}
		for v := range chan_operations {
			batch.Set(v)
			if i%batch_len == 0 {
				chan_batches <- batch
				batch = Batch{}
			}
			i++
		}
		if len(batch) > 0 {
			chan_batches <- batch
		}
		close(chan_batches)
	}()

	ctx, cancel := context.WithCancel(context.Background())

	go func(ctx context.Context) {
		ticker := time.NewTicker(time.Second * 30)
		defer ticker.Stop()
	loop:
		for {
			select {
			case <-ticker.C:
				logs.Add(logs.INFO, "Обновлено строк: ", count_rows)
			case <-ctx.Done():
				break loop
			}
		}

	}(ctx)

	wg.Wait()

	cancel()

	// Штатное завершение, сохраняем статусы всех файлов
	var count_files int
	for f := range chan_readed_files {
		f.InsertIntoDB(db, 0)
		count_files++
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Добавлено/обновлено: %d строк (%d файлов)", count_rows, count_files))

}
