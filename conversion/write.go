package conversion

import (
	"app/logs"
	"app/processing"
	"app/querrys"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/lib/pq"
)

func WriteIntoDB(channel_operations chan processing.ProviderOperation, channel_files chan *FileInfo) {

	if db == nil {
		logs.Add(logs.FATAL, "no connection to postgres")
		return
	}

	//start_time := time.Now()

	var wg sync.WaitGroup
	//var mu sync.Mutex

	//1М rows, чтобы читающие горутины на паузу не встали
	channel_maps := make(chan map[int]processing.ProviderOperation, 1000)

	batch_len := 1000 // 17 fileds

	statement := querrys.Stat_Insert_provider_registry()

	_, err := db.PrepareNamed(statement)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	counter_rows := 0

	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range channel_maps {

			tx, _ := db.Beginx()

			//print := false
			sliceID := make([]int, 0, len(v))
			sliceRows := make([]processing.ProviderOperation, 0, len(v))
			for _, row := range v {
				sliceID = append(sliceID, row.Id)
				sliceRows = append(sliceRows, row)
				counter_rows++
				// if counter_rows%100000 == 0 {
				// 	print = true
				// }
			}

			_, err = tx.Exec("delete from provider_registry where operation_id = ANY($1);", pq.Array(sliceID))
			if err != nil {
				logs.Add(logs.ERROR, fmt.Sprint("ошибка при удалении: ", err))
				tx.Rollback()
				continue
			}

			_, err := tx.NamedExec(statement, sliceRows)

			if err != nil {
				logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД: ", err))
				tx.Rollback()
			} else {
				// mu.Lock()
				// if print {
				// 	logs.Add(logs.INFO, fmt.Sprint("Добавлено/обновлено в postgres: ", counter_rows, " строк"))
				// }
				// mu.Unlock()
				tx.Commit()
			}

		}
	}()

	i := 1
	batch := map[int]processing.ProviderOperation{}
	for v := range channel_operations {
		batch[v.Id] = v
		if i%batch_len == 0 {
			channel_maps <- batch
			batch = map[int]processing.ProviderOperation{}
		}
		i++
	}

	if len(batch) != 0 {
		channel_maps <- batch
	}

	close(channel_maps)

	wg.Wait()

	// Штатное завершение, сохраняем статусы всех файлов
	for f := range channel_files {
		if !f.Done {
			f.InsertIntoDB()
			logs.Add(logs.INFO, fmt.Sprint("Записан в postgres: ", filepath.Base(f.Filename)))
		}
	}

	logs.Add(logs.INFO, fmt.Sprint("Добавлено/обновлено: ", counter_rows, " строк"))
	//logs.Add(logs.INFO, fmt.Sprintf("Загрузка операций провайдера в Postgres: %v", time.Since(start_time)))

}
