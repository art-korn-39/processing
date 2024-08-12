package conversion

import (
	"app/config"
	"app/file"
	"app/logs"
	"app/provider"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func ReadFiles(files []*file.FileInfo) (chan_operations chan provider.Operation, chan_readed_files chan *file.FileInfo) {

	var wg sync.WaitGroup

	chan_operations = make(chan provider.Operation, 1000000)
	chan_readed_files = make(chan *file.FileInfo, 5000) // с запасом, чтобы deadlock не поймать из-за переполнения
	chan_files := make(chan *file.FileInfo, 100)

	var count_readed int64
	var count_skipped int64

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer logs.Finish()
			defer wg.Done()

			for f := range chan_files {
				f.GetLastUpload(db)
				if f.LastUpload.After(f.Modified) {
					atomic.AddInt64(&count_skipped, 1)
					continue
				}

				operations, err := provider.ReadRates(f.Filename)
				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint(filepath.Base(f.Filename), " : ", err))
					atomic.AddInt64(&count_skipped, 1)
					continue
				}

				for _, v := range operations {
					chan_operations <- v
				}

				f.LastUpload = time.Now()
				f.Rows = len(operations)

				chan_readed_files <- f

				// через 40 сек после чтение ставим новую временную метку в БД
				go f.InsertIntoDB(db, 40*time.Second)

				atomic.AddInt64(&count_readed, 1)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(chan_operations)
		close(chan_readed_files)
		logs.Add(logs.INFO, fmt.Sprint("Пропущено файлов: ", count_skipped))
	}()

	go func() {
		for _, f := range files {
			chan_files <- f
		}
		close(chan_files)
	}()

	return

}
