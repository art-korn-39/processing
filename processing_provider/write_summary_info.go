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
	add_page_detail(f, M)

	err := f.Save(config.Get().SummaryInfo.Filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить данные в Excel файл: ошибка совместного доступа к файлу")
		return
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение данных в Excel файл: %v", time.Since(start_time)))

}

func add_page_turnover(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("Обороты")

	headers := []string{"Идентификатор сверки", "Дата", "Provider", "ЮЛ", "provider_name", "operation_type", "issuer_country",
		"payment_type_id / payment_method_type", "merchant account", "merchant_name", "region",
		"real_currency / channel_currency", "Кол-во операций",
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

	sheet.SetColWidth(0, 0, 30)  // баланс
	sheet.SetColWidth(1, 1, 12)  // дата
	sheet.SetColWidth(2, 2, 12)  // provider
	sheet.SetColWidth(3, 3, 24)  // организация
	sheet.SetColWidth(4, 4, 24)  // provider_name
	sheet.SetColWidth(5, 6, 12)  // operation_type, country
	sheet.SetColWidth(7, 7, 18)  // payment_method_type
	sheet.SetColWidth(8, 8, 30)  // MA
	sheet.SetColWidth(9, 15, 15) // merchant_name, real_currency / channel_currency...

	var cell *xlsx.Cell

	for k, v := range M {

		row := sheet.AddRow()
		row.AddCell().SetString(k.tariff.ID_revise)
		row.AddCell().SetDate(k.document_date)
		row.AddCell().SetString(k.provider)
		row.AddCell().SetString(k.tariff.Organization)
		row.AddCell().SetString(k.provider_name)
		row.AddCell().SetString(k.operation_type)
		row.AddCell().SetString(k.country)
		row.AddCell().SetString(k.payment_type)
		row.AddCell().SetString(k.merchant_account_name)
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.region)
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

func add_page_detail(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("Детализация")

	headers := []string{"Идентификатор сверки", "Дата", "Provider", "ЮЛ", "provider_name", "operation_type", "issuer_country",
		"payment_type_id / payment_method_type", "merchant_account", "merchant_name", "region", "account_bank_name",
		"real_currency / channel_currency", "Кол-во операций",
		"Сумма в валюте баланса", "BR Balance Currency", "Компенсация BR",
		"Акт. тариф формула", "Проверка", "Старт тарифа", "Range",
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

	sheet.SetColWidth(0, 0, 30)  // баланс
	sheet.SetColWidth(1, 1, 12)  // дата
	sheet.SetColWidth(2, 2, 12)  // provider
	sheet.SetColWidth(3, 3, 24)  // организация
	sheet.SetColWidth(4, 4, 24)  // provider_name
	sheet.SetColWidth(5, 6, 12)  // operation_type, country
	sheet.SetColWidth(7, 7, 18)  // payment_method_type
	sheet.SetColWidth(8, 8, 30)  // MA
	sheet.SetColWidth(9, 20, 15) // merchant_name, real_currency / channel_currency...

	var cell *xlsx.Cell

	for k, v := range M {

		row := sheet.AddRow()
		row.AddCell().SetString(k.tariff.ID_revise)
		row.AddCell().SetDate(k.document_date)
		row.AddCell().SetString(k.provider)
		row.AddCell().SetString(k.tariff.Organization)
		row.AddCell().SetString(k.provider_name)
		row.AddCell().SetString(k.operation_type)
		row.AddCell().SetString(k.country)
		row.AddCell().SetString(k.payment_type)
		row.AddCell().SetString(k.merchant_account_name)
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.region)
		row.AddCell().SetString(k.account_bank_name)
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

		row.AddCell().SetString(k.tariff.Formula) // Формула
		row.AddCell().SetString("")

		if k.tariff.DateStart.IsZero() { // Дата старт
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.tariff.DateStart)
		}

		row.AddCell().SetString(k.tariff.Range) // Range

	}

}
