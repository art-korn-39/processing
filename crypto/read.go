package crypto

import (
	"app/config"
	"app/logs"
	"app/util"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func Read_CSV_files(folder string) {

	start_time := time.Now()

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	read_files(filenames)

	logs.Add(logs.INFO, fmt.Sprintf("Чтение криптовалютных операций: %v", time.Since(start_time)))

}

func read_files(filenames []string) {

	start_time := time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex

	var files_readed int64

	channel_files := make(chan string, 1000)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for filename := range channel_files {

				if strings.HasPrefix(filepath.Base(filename), "in-out-v2") {
					operations, err := ReadFile3(filename)
					if err != nil {
						//logs.Add(logs.ERROR, err)
						continue
					}

					mu.Lock()
					for _, o := range operations {
						Registry3[o.Transaction_id] = o
					}
					mu.Unlock()
				} else {
					operations, err := ReadFile(filename)
					if err != nil {
						//logs.Add(logs.ERROR, err)
						continue
					}

					mu.Lock()
					for _, o := range operations {
						Registry[o.Id] = o
					}
					mu.Unlock()
				}

				atomic.AddInt64(&files_readed, 1)
			}
		}()
	}

	for _, v := range filenames {
		channel_files <- v
	}

	close(channel_files)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение файлов: %v [%d прочитано]", time.Since(start_time), files_readed))

}
