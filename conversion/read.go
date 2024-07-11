package conversion

import (
	"app/config"
	"app/logs"
	"app/processing"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func ReadFiles(files []*FileInfo) (ch_operations chan processing.ProviderOperation, ch_readed_files chan *FileInfo) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	ch_operations = make(chan processing.ProviderOperation, 1000000)
	ch_readed_files = make(chan *FileInfo, 5000) // с запасом, чтобы deadlock не поймать из-за переполнения
	ch_files := make(chan *FileInfo, 100)

	var count_readed int64
	var count_skipped int64

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
					atomic.AddInt64(&count_skipped, 1)
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
					f.mu.Lock()
					if !f.done {
						f.InsertIntoDB()
						logs.Add(logs.INFO, fmt.Sprint("Записан в postgres: ", filepath.Base(f.Filename)))
					}
					f.mu.Unlock()
				}(f)
				//logs.Add(logs.INFO, fmt.Sprint("Прочитан файл: ", filepath.Base(f.Filename)))

				atomic.AddInt64(&count_readed, 1)
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
