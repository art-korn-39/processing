package crypto

import (
	"app/config"
	"app/logs"
	"app/processing"
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

func ReadFiles(filenames []string) {

	start_time := time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex

	channel_files := make(chan string, 1000)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for filename := range channel_files {

				if filepath.Base(filename) != "pay-in-out.csv" {
					continue
				}

				operations, err := processing.ReadFileCrypto(filename)
				if err != nil {
					logs.Add(logs.ERROR, err)
					continue
				}

				mu.Lock()
				for _, o := range operations {
					crypto_operations[o.Id] = o
				}
				mu.Unlock()

			}
		}()
	}

	for _, v := range filenames {
		channel_files <- v
	}

	close(channel_files)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение файлов: %v [%d строк]", time.Since(start_time), len(crypto_operations)))

}
