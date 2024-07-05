package processing

import (
	"app/config"
	"app/logs"
	"app/util"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

func Read_Crypto() {

	if config.Get().Crypto.Storage == config.PSQL {
		util.Unused()
	} else {
		Read_CSV_Crypto()
	}

}

func Read_CSV_Crypto() {

	if config.Get().Crypto.Filename == "" {
		return
	}

	start_time := time.Now()

	folderPath := config.Get().Crypto.Filename

	storage.Crypto = make(map[int]string)

	ParseDir_crypto(folderPath)

	logs.Add(logs.INFO, fmt.Sprintf("Чтение криптовалютных операций: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Crypto))))

}

func ParseDir_crypto(folder string) {

	var wg sync.WaitGroup

	channel_files := make(chan string, 1000)

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	wg.Add(config.READ_GOROUTINES)
	for i := 1; i <= config.READ_GOROUTINES; i++ {
		go func() {
			defer wg.Done()
			for filename := range channel_files {

				if filepath.Base(filename) != "pay-in-out.csv" {
					continue
				}

				err := ReadCrypto(filename)
				if err != nil {
					logs.Add(logs.ERROR, err)
					continue
				}

			}
		}()
	}

	for _, v := range filenames {
		channel_files <- v
	}

	close(channel_files)

	wg.Wait()

}

func ReadCrypto(filename string) (err error) {

	file, err := os.Open(filename)
	if err != nil {
		logs.Add(logs.ERROR, fmt.Sprint("os.Open() ", err))
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		logs.Add(logs.ERROR, fmt.Sprint("reader.ReadAll() ", filename, ": ", err))
		return err
	}

	// мапа соответствий: имя колонки - индекс
	map_fileds := map[string]int{}
	for i, field := range records[0] {
		map_fileds[field] = i + 1
	}

	// проверяем наличие обязательных полей
	err = CheckRequiredFileds_Crypto(map_fileds)
	if err != nil {
		logs.Add(logs.ERROR, fmt.Sprint("CheckRequiredFileds_Crypto() ", filename, ": ", err))
		return err
	}

	for i, record := range records {

		if i == 0 {
			continue
		}

		operation_id, _ := strconv.Atoi(record[map_fileds["Operation id"]-1])
		network := record[map_fileds["Crypto network"]-1]

		mu.Lock()
		storage.Crypto[operation_id] = network
		mu.Unlock()

	}

	return nil
}

func CheckRequiredFileds_Crypto(map_fileds map[string]int) error {

	M := []string{"Operation id", "Crypto network"}

	for _, v := range M {

		_, ok := map_fileds[v]
		if !ok {
			return fmt.Errorf("отсуствует обязательное поле: %s", v)
		}

	}

	return nil

}
