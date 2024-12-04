package conversion_raw

import (
	"app/config"
	"app/logs"
	"app/util"
	"app/validation"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func readFile(filename string) (map_fileds map[string]int, setting Setting, err error) {

	if filename == "" {
		return
	}

	start_time := time.Now()

	ext := filepath.Ext(filename)

	switch ext {
	case ".csv":
		map_fileds, setting, err = readCSV(filename)
	case ".xlsx":
		map_fileds, setting, err = readXLSX(filename)
	default:
		return nil, Setting{}, fmt.Errorf("формат файла не поддерживается")
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций провайдера: %v [%s строк]", time.Since(start_time), util.FormatInt(len(ext_registry))))

	return

}

func readCSV(filename string) (map_fileds map[string]int, setting Setting, baseError error) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	file, err := os.Open(filename)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true

	var last_iteration bool
	for _, setting = range all_settings {

		if setting.File_format != "CSV" {
			continue
		}

		runes := []rune(setting.Comma)
		if len(runes) == 0 {
			logs.Add(logs.INFO, fmt.Errorf("в настройке \"%s\" не указан разделитель", setting.Name))
			continue
		}
		reader.Comma = runes[0]

		// строка с названиями колонок
		headers, err := reader.Read()
		if err != nil {
			logs.Add(logs.INFO, fmt.Errorf("в настройке \"%s\" ошибка при чтении заголовка: %v", setting.Name, err))
			continue
		}

		map_fileds = validation.GetMapOfColumnNamesStrings(headers)
		err = checkFields(setting, map_fileds)
		if err != nil {
			logs.Add(logs.INFO, err)
			continue
		}

		last_iteration = true

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 150 000 records -> 43.500.000 bytes (~0.004)
		capacity := fileInfo.Size() * 4 / 1000

		ext_registry = make([]*raw_operation, 0, capacity)

		channel_records := make(chan []string, 1000)

		wg.Add(config.NumCPU)
		for i := 1; i <= config.NumCPU; i++ {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case record, ok := <-channel_records:
						if !ok {
							return
						}
						op, err := createRawOperation(record, map_fileds, setting)
						if err != nil {
							baseError = err
							cancel()
							return
						}
						//op.StartingFill(true)
						mu.Lock()
						ext_registry = append(ext_registry, op)
						mu.Unlock()
					}
				}
			}()
		}
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
				record, err := reader.Read()
				if err != nil || len(record) == 0 {
					break loop
				}
				channel_records <- record
			}
		}
		close(channel_records)
		wg.Wait()

		if last_iteration {
			break
		}
	}

	return
}

func checkFields(setting Setting, map_fileds map[string]int) error {

	for _, val := range setting.values {

		if val.Calculated || val.Skip || val.From_bof || val.External_source {
			continue
		}

		_, ok := map_fileds[val.Table_column]
		if !ok {
			return fmt.Errorf("в маппинге \"%s\" неверно указано поле стыковки для колонки %s", setting.Name, val.Registry_column)
		}

	}

	return nil
}
