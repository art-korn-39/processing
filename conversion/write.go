package conversion

import (
	"app/logs"
	"app/processing"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
)

func WriteIntoDB(channel_operations chan processing.ProviderOperation, channel_files chan *FileInfo) {

	start_time := time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex

	//1М rows, чтобы читающие горутины на паузу не встали
	channel_maps := make(chan map[int]processing.ProviderOperation, 1000)

	batch_len := 1000 // 17 fileds

	statement := processing.Stat_Insert_provider_registry()

	_, err := db.PrepareNamed(statement)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	counter_rows := 0

	wg.Add(1)
	for i := 1; i <= 1; i++ {
		go func() {
			defer wg.Done()
			for v := range channel_maps {

				tx, _ := db.Beginx()

				print := false
				sliceID := make([]int, 0, len(v))
				sliceRows := make([]processing.ProviderOperation, 0, len(v))
				for _, row := range v {
					sliceID = append(sliceID, row.Id)
					sliceRows = append(sliceRows, row)
					counter_rows++
					if counter_rows%100000 == 0 {
						print = true
					}
				}

				_, err = tx.Exec("delete from provider_registry where operation_id = ANY($1);", pq.Array(sliceID))
				if err != nil {
					tx.Rollback()
					continue
				}

				_, err := tx.NamedExec(statement, sliceRows)

				if err != nil {
					log.Println("не удалось записать в БД ", err)
					tx.Rollback()
				} else {
					mu.Lock()
					if print {
						log.Println("Загружено в БД: ", counter_rows, " rows")
					}
					mu.Unlock()
					tx.Commit()
				}

			}
		}()
	}

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
		f.InsertIntoDB()
	}

	logs.Add(logs.INFO, fmt.Sprintf("Загрузка операций провайдера в Postgres: %v", time.Since(start_time)))

}
