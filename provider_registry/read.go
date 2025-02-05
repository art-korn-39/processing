package provider_registry

import (
	"app/config"
	"app/currency"
	"app/logs"
	"app/util"
	"app/validation"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tealeg/xlsx"
)

func Read_XLSX_files(folder string) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	start_time := time.Now()

	chan_files := make(chan string, 1000)

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for filename := range chan_files {

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
					rates = append(rates, o)
					registry.Set(o)
				}
				mu.Unlock()
			}
		}()
	}

	for _, v := range filenames {
		chan_files <- v
	}

	close(chan_files)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера: %v [%s строк]", time.Since(start_time), util.FormatInt(len(rates))))

}

func ReadRates(filename string) (ops []Operation, err error) {

	defer func() {
		if r := recover(); r != nil {
			logs.Add(logs.ERROR, "ReadRates(), error:", r, " file:", filename)
		}
	}()

	xlFile, err := xlsx.OpenFile(filename)
	if err != nil {
		return nil, err
	}

	sheet_names := []string{"конверт", "реестр", "sale", "payout", "rub"}

	for _, sheet := range xlFile.Sheets {

		var operations []Operation

		sheet_name := util.SubString(strings.ToLower(sheet.Name), 0, 7)
		if slices.Contains(sheet_names, sheet_name) {

			// if sheet_name == "конверт" ||
			// 	sheet_name == "реестр" ||
			// 	sheet_name == "sale" ||
			// 	sheet_name == "payout" ||
			// 	sheet_name == "rub" {

			operations, err = Read_convert_sheet(sheet, filename)
			if err != nil {
				return nil, err
			}

			// } else if sheet_name == "rub" {

			// 	operations, err = Read_rub_sheet(sheet, filename)
			// 	if err != nil {
			// 		return nil, err
			// 	}

		}

		ops = append(ops, operations...)

	}

	return ops, nil

}

func Read_convert_sheet(sheet *xlsx.Sheet, filename string) (ops []Operation, err error) {

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
	if idx_br < 0 {
		idx_br = map_fileds["br"] - 1
		if idx_br < 0 {
			idx_br = map_fileds["br в валюте пс"] - 1
			if idx_br < 0 {
				idx_br = map_fileds["br в валюте баланса"] - 1
			}
		}
	}
	idx_account := map_fileds["customer_purse / account_number"] - 1
	idx_operation_id := map_fileds["id / operation_id"] - 1
	idx_balance := map_fileds["баланс"] - 1
	idx_provider1c := map_fileds["поставщик"] - 1
	idx_project_id := map_fileds["project_id"] - 1
	idx_team := map_fileds["team"] - 1
	idx_project_url := map_fileds["project_url"] - 1
	idx_operation_status := map_fileds["operation_status"] - 1

	idx_amount := map_fileds["сумма в валюте баланса"] - 1
	if idx_amount < 0 {
		idx_amount = map_fileds["provider_amount"] - 1
	}

	idx_provider_currency := map_fileds["валюта баланса"] - 1
	if idx_provider_currency < 0 {
		idx_provider_currency = map_fileds["provider_currency"] - 1
	}

	ops = make([]Operation, 0, len(sheet.Rows))

	for i, row := range sheet.Rows {

		if i == 0 {
			continue
		}

		if len(row.Cells) == 0 || row.Cells[idx_operation_id].String() == "" {
			break
		}

		if len(row.Cells) <= idx_amount { // иначе словим panic
			continue
		}

		operation := Operation{}
		operation.Id, _ = row.Cells[map_fileds["id / operation_id"]-1].Int()
		operation.Transaction_completed_at, _ = row.Cells[map_fileds["transaction_completed_at"]-1].GetTime(false)
		operation.Transaction_completed_at_day = operation.Transaction_completed_at.Truncate(24 * time.Hour)
		operation.Operation_type = row.Cells[map_fileds["operation_type"]-1].String()
		operation.Country = row.Cells[map_fileds["issuer_country"]-1].String()
		operation.Payment_type = row.Cells[map_fileds["payment_type_id / payment_method_type"]-1].String()
		operation.Merchant_name = row.Cells[map_fileds["merchant_name"]-1].String()
		operation.Channel_currency = currency.New(row.Cells[map_fileds["real_currency / channel_currency"]-1].String())

		if idx_provider_currency > 0 {
			operation.Provider_currency = currency.New(row.Cells[idx_provider_currency].String())
		}

		operation.Rate, _ = row.Cells[map_fileds["курс"]-1].Float()
		operation.Rate = util.TR(math.IsNaN(operation.Rate), float64(0), operation.Rate).(float64)

		if idx_amount > 0 {
			operation.Amount, _ = row.Cells[idx_amount].Float()
			operation.Amount = util.TR(math.IsNaN(operation.Amount), float64(0), operation.Amount).(float64)
		}

		operation.Channel_amount, _ = row.Cells[map_fileds["real_amount / channel_amount"]-1].Float()
		operation.Channel_amount = util.TR(math.IsNaN(operation.Channel_amount), float64(0), operation.Channel_amount).(float64)

		// additional columns
		operation.Provider_name = row.Cells[map_fileds["provider_name"]-1].String()
		operation.Merchant_account_name = row.Cells[map_fileds["merchant_account_name"]-1].String()
		operation.Provider_payment_id = row.Cells[map_fileds["acquirer_id / provider_payment_id"]-1].String()

		if idx_account >= 0 {
			operation.Account_number = row.Cells[map_fileds["customer_purse / account_number"]-1].String()
		}

		if len(row.Cells) > idx_br && idx_br >= 0 {
			operation.BR_amount, _ = row.Cells[idx_br].Float()
			operation.BR_amount = util.TR(math.IsNaN(operation.BR_amount), float64(0), operation.BR_amount).(float64)
		}

		if len(row.Cells) > idx_balance && idx_balance >= 0 {
			operation.Balance = row.Cells[map_fileds["баланс"]-1].String()
		}

		if len(row.Cells) > idx_provider1c && idx_provider1c >= 0 {
			operation.Provider1c = row.Cells[map_fileds["поставщик"]-1].String()
		}

		if len(row.Cells) > idx_project_id && idx_project_id >= 0 {
			operation.Project_id, _ = row.Cells[map_fileds["project_id"]-1].Int()
		}

		if len(row.Cells) > idx_project_url && idx_project_url >= 0 {
			operation.Project_url = row.Cells[map_fileds["project_url"]-1].String()
		}

		if len(row.Cells) > idx_operation_status && idx_operation_status >= 0 {
			operation.Operation_status = row.Cells[map_fileds["operation_status"]-1].String()
		}

		if len(row.Cells) > idx_team && idx_team >= 0 {
			operation.Team = row.Cells[map_fileds["team"]-1].String()
		}

		operation.StartingFill(true)

		ops = append(ops, operation)

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
