package provider

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
					Rates = append(Rates, o)
					Registry.Set(o)
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

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Rates))))

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

	for _, sheet := range xlFile.Sheets {

		var operations []Operation

		sheet_name := util.SubString(strings.ToLower(sheet.Name), 0, 7)
		if sheet_name == "конверт" || sheet_name == "реестр" {

			operations, err = Read_convert_sheet(sheet, filename)
			if err != nil {
				return nil, err
			}

		} else if sheet_name == "rub" {

			operations, err = Read_rub_sheet(sheet, filename)
			if err != nil {
				return nil, err
			}

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
	}
	idx_account := map_fileds["customer_purse / account_number"] - 1
	idx_operation_id := map_fileds["id / operation_id"] - 1
	idx_provider_amount := map_fileds["provider_amount"] - 1
	idx_balance := map_fileds["баланс"] - 1

	ops = make([]Operation, 0, len(sheet.Rows))

	for i, row := range sheet.Rows {

		if i == 0 {
			continue
		}

		if len(row.Cells) == 0 || row.Cells[idx_operation_id].String() == "" {
			break
		}

		if len(row.Cells) <= idx_provider_amount { // иначе словим panic
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
		operation.Provider_currency = currency.New(row.Cells[map_fileds["provider_currency"]-1].String())

		operation.Rate, _ = row.Cells[map_fileds["курс"]-1].Float()
		operation.Rate = util.TR(math.IsNaN(operation.Rate), float64(0), operation.Rate).(float64)

		operation.Amount, _ = row.Cells[map_fileds["provider_amount"]-1].Float()
		operation.Amount = util.TR(math.IsNaN(operation.Amount), float64(0), operation.Amount).(float64)

		operation.Channel_amount, _ = row.Cells[map_fileds["real_amount / channel_amount"]-1].Float()
		operation.Channel_amount = util.TR(math.IsNaN(operation.Channel_amount), float64(0), operation.Channel_amount).(float64)

		// additional columns
		operation.Provider_name = row.Cells[map_fileds["provider_name"]-1].String()
		operation.Merchant_account_name = row.Cells[map_fileds["merchant_account_name"]-1].String()
		operation.Provider_payment_id = row.Cells[map_fileds["acquirer_id / provider_payment_id"]-1].String()
		operation.Project_url = row.Cells[map_fileds["project_url"]-1].String()
		operation.Operation_status = row.Cells[map_fileds["operation_status"]-1].String()

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

		operation.StartingFill(true)

		ops = append(ops, operation)

	}

	return ops, nil

}

func Read_rub_sheet(sheet *xlsx.Sheet, filename string) (ops []Operation, err error) {

	if len(sheet.Rows) < 2 {
		return
	}

	if sheet.Rows[0].Cells[0].Value != "id / operation_id" {
		err = fmt.Errorf("некорректный формат файла: %s", filename)
		return nil, err
	}

	map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[0].Cells)
	err = validation.CheckMapOfColumnNames(map_fileds, "provider_registry_rub")
	if err != nil {
		return nil, err
	}

	idx_br := map_fileds["br"] - 1
	if idx_br < 0 {
		idx_br = map_fileds["br в валюте пс *при необходимости"] - 1
	}

	idx_account := map_fileds["customer_purse / account_number"] - 1
	idx_operation_id := map_fileds["id / operation_id"] - 1
	idx_balance := map_fileds["баланс"] - 1

	ops = make([]Operation, 0, len(sheet.Rows))

	for i, row := range sheet.Rows {

		if i == 0 {
			continue
		}

		if len(row.Cells) == 0 || row.Cells[idx_operation_id].String() == "" {
			break
		}

		if len(row.Cells) <= idx_br { // иначе словим panic
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
		operation.Channel_amount, _ = row.Cells[map_fileds["real_amount / channel_amount"]-1].Float()
		operation.Channel_amount = util.TR(math.IsNaN(operation.Channel_amount), float64(0), operation.Channel_amount).(float64)

		operation.Provider_currency = operation.Channel_currency
		operation.Amount = operation.Channel_amount
		operation.Rate = 1

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

		if len(row.Cells) > idx_balance && idx_balance >= 0 {
			operation.Balance = row.Cells[map_fileds["баланс"]-1].String()
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
