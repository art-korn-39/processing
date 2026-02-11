package convert

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

func readTradexComission(filename string) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	start_time := time.Now()

	filenames := []string{}

	info, _ := os.Stat(filename)
	if info.IsDir() {
		files, _ := util.ParseFoldersRecursively(filename)
		filenames = append(filenames, files...)
	} else {
		filenames = append(filenames, filename)
	}

	for _, filename := range filenames {

		if filepath.Ext(filename) == ".csv" {

			file, err := os.Open(filename)
			if err != nil {
				logs.Add(logs.FATAL, err)
			}
			defer file.Close()

			//fileInfo, _ := file.Stat()

			reader := csv.NewReader(file)
			reader.LazyQuotes = true
			reader.Comma = ','

			// строка с названиями колонок
			headers, _ := reader.Read()

			map_fileds := validation.GetMapOfColumnNamesStrings(headers)
			err = validation.CheckMapOfColumnNames(map_fileds, "convert_comission")
			if err != nil {
				logs.Add(logs.FATAL, err)
				return
			}

			// 150 000 records -> 43.500.000 bytes (~0.004)
			//capacity := fileInfo.Size() * 4 / 1000

			//tradex_registry = make([]*Tradex_operation, 0, capacity)

			channel_records := make(chan []string, 1000)

			wg.Add(config.NumCPU)
			for i := 1; i <= config.NumCPU; i++ {
				go func() {
					defer wg.Done()
					for record := range channel_records {
						op := ConvertRecordToTradexOperation(record, map_fileds)
						op.StartingFill()

						mu.Lock()
						tradex_registry[op.id] = op
						mu.Unlock()
					}
				}()
			}

			for {
				record, err := reader.Read()
				if err != nil {
					break
				}
				channel_records <- record
			}
			close(channel_records)

			wg.Wait()
		} else {
			continue
		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение комиссий tradex: %v [%s строк]", time.Since(start_time), util.FormatInt(len(tradex_registry))))

}

type Tradex_operation struct {
	id        string
	amount    float64
	comission float64
}

func (o *Tradex_operation) StartingFill() {

}

func ConvertRecordToTradexOperation(record []string, map_fileds map[string]int) (op *Tradex_operation) {

	op = &Tradex_operation{

		id:        record[map_fileds["originoperationid"]-1],
		amount:    util.FR(strconv.ParseFloat(record[map_fileds["amount"]-1], 64)).(float64) / 100,
		comission: util.FR(strconv.ParseFloat(record[map_fileds["commission"]-1], 64)).(float64) / 100,
	}

	return

}
