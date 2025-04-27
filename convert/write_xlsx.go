package convert

import (
	"app/logs"
	"fmt"
	"strconv"
	"time"

	"github.com/tealeg/xlsx"
)

func writeIntoXLSX(filename string) {

	start_time := time.Now()

	xlsx.SetDefaultFont(11, "Calibri")

	f := xlsx.NewFile()

	add_page_convertation(f)
	add_page_absentInBof(f)
	add_page_absentInProiderRegistry(f)

	err := f.Save(filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить данные в Excel файл: ошибка совместного доступа к файлу")
		return
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение данных в Excel файл: %v", time.Since(start_time)))

}

func add_page_convertation(f *xlsx.File) {

	sheet, _ := f.AddSheet("Конвертация")

	headers := []string{"operation_id", "provider_payment_id", "provider_name", "merchant_account_name",
		"merchant_name", "project_id", "operation_type",
		"account_number", "channel_amount", "channel_currency", "issuer_country",
		"payment_method_type", "transaction_completed_at", "transaction_created_at", "provider_currency",
		"курс", "provider_amount", "BR", "balance", "provider1c", "team", "operation_status"}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 20, 14)

	var cell *xlsx.Cell

	for _, v := range final_registry {

		row := sheet.AddRow()

		row.AddCell().SetInt(v.Id)
		row.AddCell().SetString(v.Provider_payment_id)
		row.AddCell().SetString(v.Provider_name)
		row.AddCell().SetString(v.Merchant_account_name)
		row.AddCell().SetString(v.Merchant_name)
		row.AddCell().SetInt(v.Project_id)
		row.AddCell().SetString(v.Operation_type)
		row.AddCell().SetString(v.Account_number)

		cell = row.AddCell()
		cell.SetFloat(v.Channel_amount)
		cell.SetFormat("0.00")

		row.AddCell().SetString(v.Channel_currency.Name)
		row.AddCell().SetString(v.Country)
		row.AddCell().SetString(v.Payment_type)

		if v.Transaction_completed_at.IsZero() { //16
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(v.Transaction_completed_at)
		}

		if v.Transaction_created_at.IsZero() { //17
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(v.Transaction_created_at)
		}

		row.AddCell().SetString(v.Provider_currency.Name)

		cell = row.AddCell()
		cell.SetFloat(v.Rate)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.Amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.BR_amount)
		cell.SetFormat("0.00")

		row.AddCell().SetString(v.Balance)
		row.AddCell().SetString(v.Provider1c)
		row.AddCell().SetString(v.Team)
		row.AddCell().SetString(v.Operation_status)
	}

}

func add_page_absentInBof(f *xlsx.File) {

	sheet, _ := f.AddSheet("Нет в БОФ")

	headers := []string{"operation_id", "provider_payment_id",
		//"provider_name", "merchant_account_name",
		//"merchant_name", "project_url", "operation_type",
		"operation_status",
		//"account_number",
		"channel_amount", "channel_currency",
		//"issuer_country", "payment_method_type", "transaction_completed_at",
		"provider_currency",
		"курс", "provider_amount", "BR", "balance", "operation_status"}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 20, 14)

	var cell *xlsx.Cell

	for _, raw_op := range ext_registry {

		if raw_op.Bof_operation != nil {
			continue
		}

		v := raw_op.Provider_operation

		row := sheet.AddRow()

		row.AddCell().SetInt(v.Id)
		row.AddCell().SetString(v.Provider_payment_id)
		// row.AddCell().SetString(v.Provider_name)
		// row.AddCell().SetString(v.Merchant_account_name)
		// row.AddCell().SetString(v.Merchant_name)
		// row.AddCell().SetString(v.Project_url)
		//row.AddCell().SetString(v.Operation_type)
		row.AddCell().SetString(v.Operation_status)
		//row.AddCell().SetString(v.Account_number)

		cell = row.AddCell()
		cell.SetFloat(v.Channel_amount)
		cell.SetFormat("0.00")

		row.AddCell().SetString(v.Channel_currency.Name)
		//row.AddCell().SetString(v.Country)
		//row.AddCell().SetString(v.Payment_type)

		// if v.Transaction_completed_at.IsZero() { //16
		// 	row.AddCell().SetString("")
		// } else {
		// 	row.AddCell().SetDate(v.Transaction_completed_at)
		// }

		row.AddCell().SetString(v.Provider_currency.Name)

		cell = row.AddCell()
		cell.SetFloat(v.Rate)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.Amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.BR_amount)
		cell.SetFormat("0.00")

		row.AddCell().SetString(v.Balance)
		row.AddCell().SetString(v.Operation_status)
	}

}

func add_page_absentInProiderRegistry(f *xlsx.File) {

	sheet, _ := f.AddSheet("Нет в ПС")

	headers := []string{"operation_id", "provider_payment_id", "provider_name", "merchant_account_name",
		"merchant_name", "project_id", "operation_type",
		"channel_amount", "channel_currency", "issuer_country",
		"payment_method_type", "transaction_created_at", "transaction_completed_at",
	}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 20, 14)

	var cell *xlsx.Cell

	for _, v := range bof_registry {

		id, _ := strconv.Atoi(v.Operation_id)
		_, ok := final_registry[id]
		if ok {
			continue
		}

		row := sheet.AddRow()

		row.AddCell().SetString(v.Operation_id)
		row.AddCell().SetString(v.Provider_payment_id)
		row.AddCell().SetString(v.Provider_name)
		row.AddCell().SetString(v.Merchant_account_name)
		row.AddCell().SetString(v.Merchant_name)
		row.AddCell().SetInt(v.Project_id)
		row.AddCell().SetString(v.Operation_type)
		//row.AddCell().SetString(v.Status)

		cell = row.AddCell()
		cell.SetFloat(v.Channel_amount)
		cell.SetFormat("0.00")

		row.AddCell().SetString(v.Channel_currency.Name)
		row.AddCell().SetString(v.Country_code2)
		row.AddCell().SetString(v.Payment_type)

		if v.Transaction_created_at.IsZero() { //16
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(v.Transaction_created_at)
		}

		if v.Transaction_completed_at.IsZero() { //16
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(v.Transaction_completed_at)
		}
	}

}
