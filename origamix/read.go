package origamix

import (
	"app/config"
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

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций origamix: %v [%v строк]", time.Since(start_time), len(Registry)))

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
					Registry[o.Operation_id] = o
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
	reader.Comma = ';'

	records, err := reader.ReadAll()
	if err != nil {
		//logs.Add(logs.ERROR, "reader.ReadAll() ", filename, ": ", err)
		return nil, err
	}

	map_fileds := validation.GetMapOfColumnNamesStrings(records[0])
	err = validation.CheckMapOfColumnNames(map_fileds, "origamix")
	if err != nil {
		return nil, err
	}

	ops = make([]Operation, 0, len(records))

	for i, record := range records {

		if i == 0 {
			continue
		}

		o := Operation{}
		o.Operation_id, _ = strconv.Atoi(record[map_fileds["operation id"]-1])
		o.Payment_id, _ = strconv.Atoi(record[map_fileds["payment id"]-1])
		o.Merchant_id, _ = strconv.Atoi(record[map_fileds["merchant id"]-1])
		o.Merchant_account_name = record[map_fileds["merchant account"]-1]
		o.Payment_type = record[map_fileds["payment type"]-1]
		o.Payment_method = record[map_fileds["payment method"]-1]

		o.Ps_id, _ = strconv.Atoi(record[map_fileds["ps operation id"]-1])
		o.Ps_account = record[map_fileds["ps account"]-1]
		o.Ps_provider = record[map_fileds["ps provider"]-1]

		o.Amount_init = util.FR(strconv.ParseFloat(record[map_fileds["amount init"]-1], 64)).(float64)
		o.Amount_processed, _ = strconv.Atoi(record[map_fileds["amount processed"]-1])

		o.Currency_str = record[map_fileds["currency"]-1]
		o.Status = record[map_fileds["status"]-1]
		o.Ps_code = record[map_fileds["ps code"]-1]
		o.Ps_message = record[map_fileds["ps message"]-1]
		o.Result_code = record[map_fileds["result code"]-1]
		o.Result_message = record[map_fileds["result message"]-1]

		o.Created_at = util.GetDateFromString(record[map_fileds["created at"]-1])
		o.Updated_at = util.GetDateFromString(record[map_fileds["updated at"]-1])

		o.StartingFill()

		ops = append(ops, o)

	}

	return ops, nil
}
