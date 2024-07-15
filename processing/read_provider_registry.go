package processing

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
	"app/validation"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/tealeg/xlsx"
)

func Read_ProviderRegistry(registry_done chan struct{}) {

	if config.Get().Rates.Storage == config.PSQL {
		PSQL_ReadProviderRegistry(registry_done)
		// PSQL_ReadProviderRegistry_async(registry_done)
		// PSQL_ReadProviderRegistry_querry(registry_done)
		// PSQL_ReadProviderRegistry_timeout(registry_done)
		// PSQL_ReadProviderRegistry_timeout2(registry_done)
	} else {
		Read_XLSX_ProviderRegistry()
	}

}

func Read_XLSX_ProviderRegistry() {

	if config.Get().Rates.Filename == "" {
		return
	}

	start_time := time.Now()

	folderPath := config.Get().Rates.Filename

	storage.Rates = make([]ProviderOperation, 0, 1000)
	storage.Provider_operations = map[int]ProviderOperation{}

	ParseFolders_rates(folderPath)

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Rates))))

}

func ParseFolders_rates(folder string) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	channel := make(chan string, 1000)

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for filename := range channel {

				if strings.Contains(filename, "~$") || filepath.Ext(filename) != ".xlsx" {
					continue
				}

				err := CheckFileSize(filename)
				if err != nil {
					logs.Add(logs.ERROR, err)
					continue
				}

				operations, err := ReadRates(filename)
				if err != nil {
					logs.Add(logs.ERROR, err)
					continue
				}

				mu.Lock()
				for _, o := range operations {
					storage.Rates = append(storage.Rates, o)
					storage.Provider_operations[o.Id] = o
				}
				mu.Unlock()
			}
		}()
	}

	for _, v := range filenames {
		channel <- v
	}

	close(channel)

	wg.Wait()

}

func ReadRates(filename string) (ops []ProviderOperation, err error) {

	defer func() {
		if r := recover(); r != nil {
			logs.Add(logs.ERROR, "error:", r, " file:", filename)
		}
	}()

	//55s - 54164 KB
	//6.84s - 9924 KB
	//1.28s - 933 KB
	//0.21s - 135 KB

	xlFile, err := xlsx.OpenFile(filename)
	if err != nil {
		return nil, err
	}

	for _, sheet := range xlFile.Sheets {

		sheet_name := util.SubString(strings.ToLower(sheet.Name), 0, 7)
		if !(sheet_name == "конверт" || sheet_name == "реестр") {
			continue
		}

		if len(sheet.Rows) < 2 {
			return
		}

		if sheet.Rows[0].Cells[0].Value != "id / operation_id" {
			err = fmt.Errorf("некорректный формат файла: %s", filename)
			return nil, err
		}

		map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[0].Cells)
		err = validation.CheckMapOfColumnNames(map_fileds, "provider_registry")
		if err != nil {
			return nil, err
		}

		idx_br := map_fileds["br в валюте пс *при необходимости"] - 1
		idx_account := map_fileds["customer_purse / account_number"] - 1
		idx_operation_id := map_fileds["id / operation_id"] - 1

		ops = make([]ProviderOperation, 0, len(sheet.Rows))

		for i, row := range sheet.Rows {

			if i == 0 {
				continue
			}

			if len(row.Cells) == 0 || row.Cells[idx_operation_id].String() == "" {
				break
			}

			operation := ProviderOperation{}
			operation.Id, _ = row.Cells[map_fileds["id / operation_id"]-1].Int()
			operation.Transaction_completed_at, _ = row.Cells[map_fileds["transaction_completed_at"]-1].GetTime(false)
			operation.Transaction_completed_at_day = operation.Transaction_completed_at
			operation.Operation_type = row.Cells[map_fileds["operation_type"]-1].String()
			operation.Country = row.Cells[map_fileds["issuer_country"]-1].String()
			operation.Payment_type = row.Cells[map_fileds["payment_type_id / payment_method_type"]-1].String()
			operation.Merchant_name = row.Cells[map_fileds["merchant_name"]-1].String()
			operation.Channel_currency = NewCurrency(row.Cells[map_fileds["real_currency / channel_currency"]-1].String())
			operation.Provider_currency = NewCurrency(row.Cells[map_fileds["provider_currency"]-1].String())

			operation.Rate, _ = row.Cells[map_fileds["курс"]-1].Float()
			operation.Rate = util.TR(math.IsNaN(operation.Rate), float64(0), operation.Rate).(float64)

			operation.Amount, _ = row.Cells[map_fileds["provider_amount"]-1].Float()
			operation.Amount = util.TR(math.IsNaN(operation.Amount), float64(0), operation.Amount).(float64)

			if operation.Provider_currency.Name == "EUR" && operation.Rate != 0 {
				operation.Rate = 1 / operation.Rate
			}

			// additional columns
			operation.Provider_name = row.Cells[map_fileds["provider_name"]-1].String()
			operation.Merchant_account_name = row.Cells[map_fileds["merchant_account_name"]-1].String()
			operation.Provider_payment_id = row.Cells[map_fileds["acquirer_id / provider_payment_id"]-1].String()
			operation.Project_url = row.Cells[map_fileds["project_url"]-1].String()
			operation.Operation_status = row.Cells[map_fileds["operation_status"]-1].String()
			operation.Channel_currency_str = operation.Channel_currency.Name
			operation.Provider_currency_str = operation.Provider_currency.Name

			if idx_account >= 0 {
				operation.Account_number = row.Cells[map_fileds["customer_purse / account_number"]-1].String()
			}

			if len(row.Cells) > idx_br && idx_br >= 0 {
				operation.BR_amount, _ = row.Cells[idx_br].Float()
				operation.BR_amount = util.TR(math.IsNaN(operation.BR_amount), float64(0), operation.BR_amount).(float64)
			}

			ops = append(ops, operation)

		}

	}

	return ops, nil

}

func CheckFileSize(filename string) (err error) {

	file, err := os.OpenFile(filename, os.O_RDONLY, os.FileMode(0400))
	if err != nil {
		err = fmt.Errorf("os.OpenFile() %v", err)
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("file.Stat() %v", err)
		return err
	}

	// больше 20 Мб - пропускаем
	size := stat.Size()
	if size > 20480000 {
		err = fmt.Errorf("пропущен файл %s (%d МБайт)", filepath.Base(filename), size/1024000)
		return err
	}

	return nil

}

func PSQL_ReadProviderRegistry_async(registry_done chan struct{}) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	if storage.Postgres.DB == nil {
		return
	}

	// MERCHANT_NAME + DATE
	merchant_names, DateFrom, DateTo := GetArgsForProviderRegistry(registry_done)

	if len(merchant_names) == 0 {
		logs.Add(logs.INFO, `пустой массив "merchant_name" для чтения операций провайдера`)
		return
	}

	start_time := time.Now()

	storage.Rates = make([]ProviderOperation, 0, 1000000)

	channel_dates := GetChannelOfDays(DateFrom, DateTo, 24*time.Hour)

	stat := querrys.Stat_Select_provider_registry()

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {

				args := []any{pq.Array(merchant_names), period.startDay, period.endDay}

				res := []ProviderOperation{}

				err := storage.Postgres.Select(&res, stat, args...)
				if err != nil {
					logs.Add(logs.INFO, err)
					return
				}

				mu.Lock()
				storage.Rates = append(storage.Rates, res...)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	for i := range storage.Rates {
		operation := &storage.Rates[i]

		if operation.Provider_currency.Name == "EUR" && operation.Rate != 0 {
			operation.Rate = 1 / operation.Rate
		}

		operation.Channel_currency = NewCurrency(operation.Channel_currency_str)
		operation.Provider_currency = NewCurrency(operation.Provider_currency_str)

		storage.Provider_operations[operation.Id] = *operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера из Postgres async: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Rates))))

}

func PSQL_ReadProviderRegistry(registry_done chan struct{}) {

	if storage.Postgres.DB == nil {
		return
	}

	// MERCHANT_NAME + DATE
	merchant_names, DateFrom, DateTo := GetArgsForProviderRegistry(registry_done)

	if len(merchant_names) == 0 {
		logs.Add(logs.INFO, `пустой массив "merchant_name" для чтения операций провайдера`)
		return
	}

	start_time := time.Now()

	args := []any{pq.Array(merchant_names), DateFrom, DateTo}

	stat := querrys.Stat_Select_provider_registry()

	err := storage.Postgres.Select(&storage.Rates, stat, args...)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for i := range storage.Rates {
		operation := &storage.Rates[i]

		if operation.Provider_currency.Name == "EUR" && operation.Rate != 0 {
			operation.Rate = 1 / operation.Rate
		}

		operation.Channel_currency = NewCurrency(operation.Channel_currency_str)
		operation.Provider_currency = NewCurrency(operation.Provider_currency_str)

		storage.Provider_operations[operation.Id] = *operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Rates))))

}

func PSQL_ReadProviderRegistry_querry(registry_done chan struct{}) {

	if storage.Postgres.DB == nil {
		return
	}

	// MERCHANT_NAME + DATE
	merchant_names, DateFrom, DateTo := GetArgsForProviderRegistry(registry_done)

	if len(merchant_names) == 0 {
		logs.Add(logs.INFO, `пустой массив "merchant_name" для чтения операций провайдера`)
		return
	}

	start_time := time.Now()

	args := []any{pq.Array(merchant_names), DateFrom, DateTo}

	stat := querrys.Stat_Select_provider_registry()

	rows, err := storage.Postgres.Query(stat, args...)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}
	defer rows.Close()

	storage.Rates = make([]ProviderOperation, 0, 1000000)

	for rows.Next() {

		var r ProviderOperation
		if err := rows.Scan(&r); err != nil {
			logs.Add(logs.FATAL, err)
			return
		}

		if r.Provider_currency.Name == "EUR" && r.Rate != 0 {
			r.Rate = 1 / r.Rate
		}

		r.Channel_currency = NewCurrency(r.Channel_currency_str)
		r.Provider_currency = NewCurrency(r.Provider_currency_str)

		storage.Provider_operations[r.Id] = r

		storage.Rates = append(storage.Rates, r)

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера из Postgres Q: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Rates))))

}

func PSQL_ReadProviderRegistry_timeout(registry_done chan struct{}) {

	if storage.Postgres.DB == nil {
		return
	}

	// MERCHANT_NAME + DATE
	merchant_names, DateFrom, DateTo := GetArgsForProviderRegistry(registry_done)

	if len(merchant_names) == 0 {
		logs.Add(logs.INFO, `пустой массив "merchant_name" для чтения операций провайдера`)
		return
	}

	start_time := time.Now()

	args := []any{pq.Array(merchant_names), DateFrom, DateTo}

	stat := querrys.Stat_Select_provider_registry()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	chan_error := make(chan error)

	// запрос к БД
	go func(ctx context.Context, chan_error chan<- error) {
		err := storage.Postgres.SelectContext(ctx, &storage.Rates, stat, args...)
		chan_error <- err
	}(ctx, chan_error)

	select {
	case <-ctx.Done():
		fmt.Println("таймаут, строк в таблице: ", len(storage.Rates), " ", ctx.Err())
	case err := <-chan_error:
		fmt.Printf("ошибка: %v, строк в таблице: %d\n", err, len(storage.Rates))
	}

	for i := range storage.Rates {
		operation := &storage.Rates[i]

		if operation.Provider_currency.Name == "EUR" && operation.Rate != 0 {
			operation.Rate = 1 / operation.Rate
		}

		operation.Channel_currency = NewCurrency(operation.Channel_currency_str)
		operation.Provider_currency = NewCurrency(operation.Provider_currency_str)

		storage.Provider_operations[operation.Id] = *operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера из Postgres timeout: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Rates))))

}

func PSQL_ReadProviderRegistry_timeout2(registry_done chan struct{}) {

	if storage.Postgres.DB == nil {
		return
	}

	// MERCHANT_NAME + DATE
	merchant_names, DateFrom, DateTo := GetArgsForProviderRegistry(registry_done)

	if len(merchant_names) == 0 {
		logs.Add(logs.INFO, `пустой массив "merchant_name" для чтения операций провайдера`)
		return
	}

	start_time := time.Now()

	args := []any{pq.Array(merchant_names), DateFrom, DateTo}

	stat := querrys.Stat_Select_provider_registry()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := storage.Postgres.SelectContext(ctx, &storage.Rates, stat, args...)
	if err != nil {
		fmt.Printf("ошибка: %v, строк в таблице: %d\n", err, len(storage.Rates))
		return
	}

	for i := range storage.Rates {
		operation := &storage.Rates[i]

		if operation.Provider_currency.Name == "EUR" && operation.Rate != 0 {
			operation.Rate = 1 / operation.Rate
		}

		operation.Channel_currency = NewCurrency(operation.Channel_currency_str)
		operation.Provider_currency = NewCurrency(operation.Provider_currency_str)

		storage.Provider_operations[operation.Id] = *operation
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера из Postgres timeout: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Rates))))

}

func GetArgsForProviderRegistry(registry_done <-chan struct{}) (merchant_names []string, DateFrom, DateTo time.Time) {

	if config.Get().Registry.Storage == config.Clickhouse {
		merchant_names = config.Get().Registry.Merchant_name
		DateFrom = config.Get().Registry.DateFrom.Add(-20 * 24 * time.Hour)
		DateTo = config.Get().Registry.DateTo.Add(1 * 24 * time.Hour)
	} else {
		<-registry_done
		lenght := len(storage.Registry)
		if lenght > 0 {
			row := storage.Registry[0]
			merchant_names = append(merchant_names, row.Merchant_name)
			DateFrom = storage.Registry[0].Transaction_completed_at.Add(-3 * 24 * time.Hour)
			DateTo = storage.Registry[lenght-1].Transaction_completed_at.Add(1 * 24 * time.Hour)
		}
	}

	return

}
