package conversion

import (
	"app/file"
	"app/logs"
	"app/provider"
	"app/querrys"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func WriteIntoDB(chan_operations chan provider.Operation, chan_readed_files chan *file.FileInfo) {

	if db == nil {
		logs.Add(logs.FATAL, "no connection to postgres")
		return
	}

	var wg sync.WaitGroup

	//1М rows, чтобы читающие горутины на паузу не встали
	chan_batches := make(chan Batch2, 1000)

	batch_len := 1500 // 20 fileds

	statement := querrys.Stat_Insert_provider_registry()

	_, err := db.PrepareNamed(statement)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	var count_rows int64

	wg.Add(1)
	//for i := 1; i <= 1; i++ {
	go func() {
		defer wg.Done()
		for b := range chan_batches {

			//tx, _ := db.Beginx()

			// sliceID := make([]int, 0, len(v))
			// sliceRows := make([]processing.ProviderOperation, 0, len(v))
			// count_rows = count_rows + len(v)
			// for _, row := range v {
			// 	sliceID = append(sliceID, row.Id)
			// 	sliceRows = append(sliceRows, row)
			// }

			// _, err = tx.NamedQuery(`DELETE from provider_registry
			// 	WHERE operation_id = :operation_id
			// 		AND transaction_completed_at_day = :transaction_completed_at_day
			// 		AND channel_amount = :channel_amount;`, v)

			// if err != nil {
			// 	logs.Add(logs.ERROR, fmt.Sprint("ошибка при удалении: ", err))
			// 	tx.Rollback()
			// 	continue
			// }

			// query, args, err := sqlx.In(`DELETE FROM provider_registry WHERE operation_id = :operation_id
			// AND transaction_completed_at_day = :transaction_completed_at_day
			// AND channel_amount = :channel_amount`, v)

			// fmt.Println(query)
			// fmt.Println(args...)
			// fmt.Println(err)

			// log.Fatal()

			v := b.Get()

			_, err := db.NamedExec(statement, v)

			if err != nil {
				logs.Add(logs.ERROR, fmt.Sprintf("не удалось записать в БД: %v, date: %s, provider: %s, merchant: %s", err, v[0].Transaction_completed_at_day.Format(time.DateOnly), v[0].Provider_name, v[0].Merchant_name))
				//tx.Rollback()
			} else {
				atomic.AddInt64(&count_rows, int64(len(v)))
				//tx.Commit()
			}

			b = nil
			v = nil

		}
	}()
	//}

	go func() {
		// i := 1
		// batch := make([]processing.ProviderOperation, 0, 1000)
		// for v := range channel_operations {
		// 	batch = append(batch, v)
		// 	if i%batch_len == 0 {
		// 		channel_slices <- batch
		// 		batch = make([]processing.ProviderOperation, 0, 1000)
		// 	}
		// 	i++
		// }
		// if len(batch) != 0 {
		// 	channel_slices <- batch
		// }
		// close(channel_slices)

		i := 1
		batch := Batch2{}
		for v := range chan_operations {
			batch.Set(v)
			if i%batch_len == 0 {
				chan_batches <- batch
				batch = Batch2{}
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
		//f.mu.Lock()
		//if !f.done {
		f.InsertIntoDB(db, 0)
		//logs.Add(logs.MAIN, fmt.Sprint("Записан в postgres: ", filepath.Base(f.Filename)))
		//}
		//f.mu.Unlock()
		count_files++
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Добавлено/обновлено: %d строк (%d файлов)", count_rows, count_files))

}
