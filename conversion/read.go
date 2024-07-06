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

	ch_operations = make(chan processing.ProviderOperation, 1000000)
	ch_readed_files = make(chan *FileInfo, 5000) // с запасом, чтобы deadlock не поймать из-за переполнения

	ch_files := make(chan *FileInfo, 100)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer logs.Finish()
			defer wg.Done()

			for f := range ch_files {
				f.SetLastUpload()
				if f.LastUpload.After(f.Modified) {
					logs.Add(logs.INFO, fmt.Sprint("Пропущен: ", filepath.Base(f.Filename)))
					continue
				}
				operations, err := processing.ReadRates(f.Filename)
				if err != nil {
					logs.Add(logs.ERROR, err)
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
					f.InsertIntoDB()
					logs.Add(logs.INFO, fmt.Sprint("Записан в БД: ", filepath.Base(f.Filename)))
				}(f)
				logs.Add(logs.INFO, fmt.Sprint("Прочитан: ", filepath.Base(f.Filename)))
			}
		}()
	}

	go func() {
		wg.Wait()
		close(ch_operations)
		close(ch_readed_files)
	}()

	go func() {
		for _, f := range files {
			ch_files <- f
		}
		close(ch_files)
	}()

	return

}
