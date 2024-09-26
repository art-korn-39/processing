package processing_provider

import (
	"app/config"
	"app/logs"
	"app/util"
	"fmt"
	"time"

	"github.com/tealeg/xlsx"
)

func Write_SummaryInfo(M map[KeyFields_SummaryInfo]SumFileds) {

	if !config.Get().SummaryInfo.Usage {
		return
	}

	if config.Get().SummaryInfo.Storage == config.PSQL {
		util.Unused()
	} else {
		Write_XLSX_SummaryInfo(M)
	}

}

func Write_XLSX_SummaryInfo(M map[KeyFields_SummaryInfo]SumFileds) {

	if config.Get().SummaryInfo.Filename == "" {
		return
	}

	start_time := time.Now()

	xlsx.SetDefaultFont(11, "Calibri")

	f := xlsx.NewFile()

	add_page_turnover(f, M)

	err := f.Save(config.Get().SummaryInfo.Filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить данные в Excel файл: ошибка совместного доступа к файлу")
		return
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение данных в Excel файл: %v", time.Since(start_time)))

}

func add_page_turnover(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("Обороты")

	headers := []string{"Баланс", "Проверка", "Дата", "Provider", "ЮЛ", "provider_name", "operation_type", "issuer_country",
		"payment_type_id / payment_method_type", "merchant_name", "real_currency / channel_currency", "Кол-во операций",
		"Сумма в валюте баланса", "BR Balance Currency", "Компенсация BR",
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

	sheet.SetColWidth(0, 0, 30)   // баланс
	sheet.SetColWidth(1, 1, 15)   // проверка
	sheet.SetColWidth(2, 2, 12)   // дата
	sheet.SetColWidth(3, 7, 15)   // provider, ЮЛ, provider_name, operation_type
	sheet.SetColWidth(8, 8, 12)   // country
	sheet.SetColWidth(9, 9, 18)   // payment_method_type
	sheet.SetColWidth(10, 15, 15) // merchant_name, real_currency / channel_currency...

	var cell *xlsx.Cell

	for k, v := range M {

		if k.verification == VRF_CHECK_RATE {
			continue
		}

		row := sheet.AddRow()
		row.AddCell().SetString(k.balance_name)
		row.AddCell().SetString(k.verification)
		row.AddCell().SetDate(k.document_date)
		row.AddCell().SetString(k.provider)
		row.AddCell().SetString(k.JL)
		row.AddCell().SetString(k.provider_name)
		row.AddCell().SetString(k.operation_type)
		row.AddCell().SetString(k.country)
		row.AddCell().SetString(k.payment_type)
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.channel_currency.Name)
		row.AddCell().SetInt(v.count_operations)

		cell = row.AddCell()
		cell.SetFloat(v.balance_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.BR_balance_currency)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.CompensationBR)
		cell.SetFormat("0.00")

	}

}
