package crypto

import (
	"app/config"
	"app/currency"
	"app/logs"
	"app/util"
	"app/validation"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
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

				operations, err := ReadFile(filename)
				if err != nil {
					//logs.Add(logs.ERROR, err)
					continue
				}

				atomic.AddInt64(&files_readed, 1)

				mu.Lock()
				for _, o := range operations {
					Registry[o.Id] = o
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

	logs.Add(logs.INFO, fmt.Sprintf("Чтение файлов: %v [%d прочитано]", time.Since(start_time), files_readed))

}

func ReadFile(filename string) (ops []Operation, err error) {

	file, err := os.Open(filename)
	if err != nil {
		//logs.Add(logs.ERROR, fmt.Sprint("os.Open() ", err))
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		//logs.Add(logs.ERROR, "reader.ReadAll() ", filename, ": ", err)
		return nil, err
	}

	map_fileds := validation.GetMapOfColumnNamesStrings(records[0])
	err = validation.CheckMapOfColumnNames(map_fileds, "crypto")
	if err != nil {
		return nil, err
	}

	ops = make([]Operation, 0, len(records))

	for i, record := range records {

		if i == 0 {
			continue
		}

		o := Operation{}
		o.Id, _ = strconv.Atoi(record[map_fileds["operation id"]-1])
		o.Network = record[map_fileds["crypto network"]-1]
		o.Created_at = util.GetDateFromString(record[map_fileds["created at"]-1])
		o.Created_at_day = o.Created_at
		o.Operation_type = record[map_fileds["operation type"]-1]
		o.Payment_amount, _ = strconv.ParseFloat(record[map_fileds["payment amount"]-1], 64)
		o.Payment_currency_str = record[map_fileds["payment currency"]-1]
		o.Crypto_amount, _ = strconv.ParseFloat(record[map_fileds["crypto amount"]-1], 64)
		o.Crypto_currency_str = record[map_fileds["crypto currency"]-1]

		o.Payment_currency = currency.New(record[map_fileds["payment currency"]-1])
		o.Crypto_currency = currency.New(record[map_fileds["crypto currency"]-1])

		ops = append(ops, o)

	}

	return ops, nil
}
