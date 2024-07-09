package conversion

import (
	"app/config"
	"app/logs"
	"app/processing"
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

func ReadFiles(files []*FileInfo) (ch_operations chan processing.ProviderOperation, ch_readed_files chan *FileInfo) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	ch_operations = make(chan processing.ProviderOperation, 1000000)
	ch_readed_files = make(chan *FileInfo, 5000) // с запасом, чтобы deadlock не поймать из-за переполнения
	ch_files := make(chan *FileInfo, 100)

	var count_readed int
	var count_skipped int

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer logs.Finish()
			defer wg.Done()

			for f := range ch_files {
				f.SetLastUpload()
				if f.LastUpload.After(f.Modified) {
					mu.Lock()
					count_skipped++
					mu.Unlock()
					continue
				}

				operations, err := processing.ReadRates(f.Filename)
				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint(filepath.Base(f.Filename), " : ", err))
					mu.Lock()
					count_skipped++ // atomic
					mu.Unlock()
					continue
				}

				for _, v := range operations {
					ch_operations <- v
				}

				f.LastUpload = time.Now()
				f.Rows = len(operations)

				ch_readed_files <- f

				// через 40 сек после чтение ставим новую временную метку
				go func(f *FileInfo) {
					ticker := time.NewTicker(40 * time.Second)
					<-ticker.C
					if !f.Done {
						f.InsertIntoDB()
						logs.Add(logs.INFO, fmt.Sprint("Записан в postgres: ", filepath.Base(f.Filename)))
					}
				}(f)
				//logs.Add(logs.INFO, fmt.Sprint("Прочитан файл: ", filepath.Base(f.Filename)))

				mu.Lock()
				count_readed++
				mu.Unlock()
			}
		}()
	}

	go func() {
		wg.Wait()
		close(ch_operations)
		close(ch_readed_files)
		logs.Add(logs.INFO, fmt.Sprint("Пропущено файлов: ", count_skipped))
	}()

	go func() {
		for _, f := range files {
			ch_files <- f
		}
		close(ch_files)
	}()

	return

}
