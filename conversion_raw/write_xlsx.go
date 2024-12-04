package conversion_raw

import (
	"app/logs"
	"fmt"
	"time"

	"github.com/tealeg/xlsx"
)

func writeIntoXLSX(filename string) {

	start_time := time.Now()

	xlsx.SetDefaultFont(11, "Calibri")

	f := xlsx.NewFile()

	add_page_convertation(f)

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
		"merchant_name", "project_url", "operation_type", "operation_status",
		"account_number", "channel_amount", "channel_currency", "issuer_country",
		"payment_method_type", "transaction_completed_at", "provider_currency",
		"курс", "provider_amount", "BR", "balance"}

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

	sheet.SetColWidth(13, 13, 14) // дата

	// sheet.SetColWidth(0, 0, 16)   // проверка
	// sheet.SetColWidth(1, 1, 30)   // баланс
	// sheet.SetColWidth(2, 2, 11)   // idbalance
	// sheet.SetColWidth(3, 3, 12)   // дата
	// sheet.SetColWidth(4, 4, 16)   // проверка
	// sheet.SetColWidth(5, 5, 15)   // operation_type
	// sheet.SetColWidth(6, 6, 18)   // payment_method_type
	// sheet.SetColWidth(9, 9, 35)   // merchant_account_name
	// sheet.SetColWidth(10, 10, 16) // подразделение
	// sheet.SetColWidth(11, 11, 25) // рассчетный счет
	// sheet.SetColWidth(12, 12, 16) // поставщик 1С
	// sheet.SetColWidth(13, 13, 14) // real_currency / channel_currency
	// sheet.SetColWidth(18, 18, 16) // tariff_condition_id
	// sheet.SetColWidth(19, 19, 12) // contract_id
	// sheet.SetColWidth(20, 21, 12) // PPрасхолд
	// sheet.SetColWidth(22, 36, 16)

	var cell *xlsx.Cell

	for _, v := range registry {

		row := sheet.AddRow()

		row.AddCell().SetInt(v.Id)
		row.AddCell().SetString(v.Provider_payment_id)
		row.AddCell().SetString(v.Provider_name)
		row.AddCell().SetString(v.Merchant_account_name)
		row.AddCell().SetString(v.Merchant_name)
		row.AddCell().SetString(v.Project_url)
		row.AddCell().SetString(v.Operation_type)
		row.AddCell().SetString(v.Operation_status)
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
	}

}
