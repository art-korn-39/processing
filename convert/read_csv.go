package convert

import (
	"app/config"
	"app/logs"
	"app/util"
	"app/validation"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func readFile(filename string) (err error) {

	if filename == "" {
		return
	}

	start_time := time.Now()

	ext := filepath.Ext(filename)

	switch ext {
	case ".csv":
		err = readCSV(filename)
	case ".xlsx":
		err = readXLSX(filename)
	default:
		return fmt.Errorf("формат файла не поддерживается")
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций провайдера: %v [%s строк]", time.Since(start_time), util.FormatInt(len(ext_registry))))

	return

}

func readCSV(filename string) (baseError error) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	var last_iteration bool
	for _, setting := range all_settings {

		if setting.File_format != "CSV" {
			continue
		}

		runes := []rune(setting.Comma)
		if len(runes) == 0 {
			logs.Add(logs.INFO, fmt.Errorf("в настройке \"%s\" не указан разделитель", setting.Name))
			continue
		}

		file, err := os.Open(filename)
		if err != nil {
			logs.Add(logs.FATAL, err)
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.LazyQuotes = true
		reader.Comma = runes[0]

		// строка с названиями колонок
		headers, err := reader.Read()
		if err != nil {
			logs.Add(logs.INFO, fmt.Errorf("в настройке \"%s\" ошибка при чтении заголовка: %v", setting.Name, err))
			continue
		}

		if len(headers) < 2 {
			logs.Add(logs.INFO, fmt.Errorf("в настройке \"%s\" неправильно указан разделитель: %v", setting.Name, err))
			continue
		}

		map_fileds := validation.GetMapOfColumnNamesStrings(headers)
		err = checkFields(setting, map_fileds)
		if err != nil {
			logs.Add(logs.INFO, err)
			continue
		}

		used_settings[setting.Guid] = setting
		last_iteration = true

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
						bp, err := createBaseOperation(record, map_fileds, setting)
						if err != nil {
							baseError = err
							cancel()
							return
						}
						mu.Lock()
						ext_registry = append(ext_registry, bp)
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
				if err == io.EOF || len(record) == 0 {
					break loop
				} else if err != nil {
					logs.Add(logs.FATAL, err)
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
