package processing

import (
	"app/config"
	"app/logs"
	"app/util"
	"app/validation"
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
		PSQL_ReadCrypto()
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

	ReadFilesCrypto(folderPath)

	logs.Add(logs.INFO, fmt.Sprintf("Чтение криптовалютных операций: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Crypto))))

}

func ReadFilesCrypto(folder string) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	channel_files := make(chan string, 1000)

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for filename := range channel_files {

				if filepath.Base(filename) != "pay-in-out.csv" {
					continue
				}

				operations, err := ReadFileCrypto(filename)
				if err != nil {
					logs.Add(logs.ERROR, "ReadFileCrypto() ", err)
					continue
				}

				mu.Lock()
				for _, o := range operations {
					storage.Crypto[o.Id] = o
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

}

func ReadFileCrypto(filename string) (ops []CryptoOperation, err error) {

	file, err := os.Open(filename)
	if err != nil {
		logs.Add(logs.ERROR, fmt.Sprint("os.Open() ", err))
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		logs.Add(logs.ERROR, "reader.ReadAll() ", filename, ": ", err)
		return nil, err
	}

	map_fileds := validation.GetMapOfColumnNamesStrings(records[0])
	err = validation.CheckMapOfColumnNames(map_fileds, "crypto")
	if err != nil {
		return nil, err
	}

	ops = make([]CryptoOperation, 0, len(records))

	for i, record := range records {

		if i == 0 {
			continue
		}

		o := CryptoOperation{}
		o.Id, _ = strconv.Atoi(record[map_fileds["operation id"]-1])
		o.Network = record[map_fileds["crypto network"]-1]
		o.Created_at = util.GetDateFromString(record[map_fileds["created at"]-1])
		o.Operation_type = record[map_fileds["operation type"]-1]
		o.Payment_amount, _ = strconv.ParseFloat(record[map_fileds["payment amount"]-1], 64)
		o.Payment_currency_str = record[map_fileds["payment currency"]-1]
		o.Crypto_amount, _ = strconv.ParseFloat(record[map_fileds["crypto amount"]-1], 64)
		o.Crypto_currency_str = record[map_fileds["crypto currency"]-1]

		o.Payment_currency = NewCurrency(record[map_fileds["payment currency"]-1])
		o.Crypto_currency = NewCurrency(record[map_fileds["crypto currency"]-1])

		ops = append(ops, o)

	}

	return ops, nil
}

func PSQL_ReadCrypto() {

	if storage.Postgres.DB == nil {
		return
	}

	start_time := time.Now()

	storage.Crypto = map[int]CryptoOperation{}

	stat := `SELECT * FROM crypto`

	slice_operations := []CryptoOperation{}

	err := storage.Postgres.Select(&slice_operations, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, operation := range slice_operations {

		operation.Crypto_currency = NewCurrency(operation.Crypto_currency_str)
		operation.Payment_currency = NewCurrency(operation.Payment_currency_str)

		storage.Crypto[operation.Id] = operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение криптовалютных операций из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Crypto))))

}
