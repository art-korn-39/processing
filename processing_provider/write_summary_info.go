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
	add_page_turnover_dragonpay(f, M)

	err := f.Save(config.Get().SummaryInfo.Filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить данные в Excel файл: ошибка совместного доступа к файлу")
		return
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение данных в Excel файл: %v", time.Since(start_time)))

}

func add_page_turnover(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("Обороты")

	headers := []string{"Баланс провайдера", "Ключ", "Наименование баланса ПС", "ЮЛ", "Дата учета",
		"provider_name", "merchant_account",
		"operation_type", "region", "payment_type", "merchant_name", "Валюта баланса", "Кол-во транз",
		"Сумма в валюте баланса", "BR в валюте баланса", "Surcharge amount", "Доп. BR в валюте баланса",
		"Сумма в валюте канала", "Валюта канала", "Мерч 1С",
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

	sheet.SetColWidth(0, 1, 30)   // баланс
	sheet.SetColWidth(2, 2, 12)   // дата
	sheet.SetColWidth(3, 3, 12)   // provider
	sheet.SetColWidth(4, 4, 24)   // организация
	sheet.SetColWidth(5, 5, 24)   // provider_name
	sheet.SetColWidth(6, 6, 12)   // operation_type, country
	sheet.SetColWidth(8, 8, 18)   // payment_method_type
	sheet.SetColWidth(9, 9, 30)   // MA
	sheet.SetColWidth(10, 16, 15) // merchant_name, Валюта канала...

	//var cell *xlsx.Cell

	for k, v := range M {

		row := sheet.AddRow()
		row.AddCell().SetString(k.balance)
		row.AddCell().SetString(k.id_revise)
		row.AddCell().SetString(k.contractor_provider)
		row.AddCell().SetString(k.organization)
		row.AddCell().SetDate(k.document_date)
		row.AddCell().SetString(k.provider_name)
		row.AddCell().SetString(k.merchant_account_name)
		row.AddCell().SetString(k.operation_type)
		row.AddCell().SetString(k.region)
		row.AddCell().SetString(k.payment_type)
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.balance_currency.Name)
		row.AddCell().SetInt(v.count_operations)

		// cell = row.AddCell()
		// cell.SetFloat(v.balance_amount)
		// cell.SetFormat("0.00")

		// cell = row.AddCell()
		// cell.SetFloat(v.BR_balance_currency)
		// cell.SetFormat("0.0000")

		// cell = row.AddCell()
		// cell.SetFloat(v.surcharge_amount)
		// cell.SetFormat("0.00")

		// cell = row.AddCell()
		// cell.SetFloat(v.Extra_BR_balance_currency)
		// cell.SetFormat("0.0000")

		// cell = row.AddCell()
		// cell.SetFloat(v.channel_amount)
		// cell.SetFormat("0.00")

		util.AddCellWithFloat(row, v.balance_amount, k.balance_currency.GetAccuracy(3))
		util.AddCellWithFloat(row, v.BR_balance_currency, k.balance_currency.GetAccuracy(4))
		util.AddCellWithFloat(row, v.surcharge_amount, 2)
		util.AddCellWithFloat(row, v.Extra_BR_balance_currency, k.balance_currency.GetAccuracy(4))
		util.AddCellWithFloat(row, v.channel_amount, 2)

		row.AddCell().SetString(k.channel_currency.Name)

		if k.isTestId == 2 {
			row.AddCell().SetString("Тест")
		} else {
			row.AddCell().SetString(k.contractor_merchant)
		}

	}
}

func add_page_detail(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("Детализация")

	headers := []string{"Наименование баланса ПС", "ЮЛ", "Идентификатор сверки", "Дата",
		"provider_name", "operation_type", //"issuer_country",
		"payment_type", "merchant_account", "merchant_name", "region", //"account_bank_name",
		"real_currency / channel_currency", "Валюта баланса", "Кол-во операций",
		"Сумма в валюте баланса", "BR Balance Currency", "Компенсация BR",
		"Акт. тариф формула", "Проверка", "Старт тарифа", "Range", "Мерч 1С",
		"project id", "project name", "Поставщик 1С",
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

	sheet.SetColWidth(0, 22, 18)

	//var cell *xlsx.Cell

	for k, v := range M {

		row := sheet.AddRow()
		row.AddCell().SetString(k.contractor_provider)
		row.AddCell().SetString(k.organization)
		row.AddCell().SetString(k.id_revise)
		row.AddCell().SetDate(k.document_date)
		row.AddCell().SetString(k.provider_name)
		row.AddCell().SetString(k.operation_type)
		//row.AddCell().SetString(k.country)
		row.AddCell().SetString(k.payment_type)
		row.AddCell().SetString(k.merchant_account_name)
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.region)
		//row.AddCell().SetString(k.account_bank_name)
		row.AddCell().SetString(k.channel_currency.Name)
		row.AddCell().SetString(k.balance_currency.Name)
		row.AddCell().SetInt(v.count_operations)

		// cell = row.AddCell()
		// cell.SetFloat(v.balance_amount)
		// cell.SetFormat("0.00")

		// cell = row.AddCell()
		// cell.SetFloat(v.BR_balance_currency)
		// cell.SetFormat("0.0000")

		// cell = row.AddCell()
		// cell.SetFloat(v.compensationBR)
		// cell.SetFormat("0.00")

		util.AddCellWithFloat(row, v.balance_amount, k.balance_currency.GetAccuracy(2))
		util.AddCellWithFloat(row, v.BR_balance_currency, k.balance_currency.GetAccuracy(4))
		util.AddCellWithFloat(row, v.compensationBR, 2)

		row.AddCell().SetString(k.tariff.Formula) // Формула
		row.AddCell().SetString(k.verification)

		if k.tariff.DateStart.IsZero() { // Дата старт
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.tariff.DateStart)
		}

		row.AddCell().SetString(k.tariff.Range) // Range

		row.AddCell().SetString(k.contractor_merchant)

		row.AddCell().SetInt(k.project_id)
		row.AddCell().SetString(k.project_name)
		row.AddCell().SetString(k.provider1c)

	}

}

func add_page_turnover_dragonpay(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("Обороты_Dragonpay")

	headers := []string{"Баланс провайдера", "Ключ", "Наименование баланса ПС", "ЮЛ", "Дата учета",
		"provider_name", "merchant_account",
		"operation_type", "region", "payment_type", "merchant_name", "Валюта баланса", "Кол-во транз",
		"Сумма в валюте баланса", "BR в валюте баланса", "Surcharge amount", "Доп. BR в валюте баланса",
		"Сумма в валюте канала", "Валюта канала", "Мерч 1С", "Подразделение", "Поставщик",
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

	sheet.SetColWidth(0, 1, 30)   // баланс
	sheet.SetColWidth(2, 2, 12)   // дата
	sheet.SetColWidth(3, 3, 12)   // provider
	sheet.SetColWidth(4, 4, 24)   // организация
	sheet.SetColWidth(5, 5, 24)   // provider_name
	sheet.SetColWidth(6, 6, 12)   // operation_type, country
	sheet.SetColWidth(8, 8, 18)   // payment_method_type
	sheet.SetColWidth(9, 9, 30)   // MA
	sheet.SetColWidth(10, 16, 15) // merchant_name, Валюта канала...

	//var cell *xlsx.Cell

	for k, v := range M {

		if !k.isDragonpay {
			continue
		}

		row := sheet.AddRow()
		row.AddCell().SetString(k.balance)
		row.AddCell().SetString(k.id_revise)
		row.AddCell().SetString(k.contractor_provider)
		row.AddCell().SetString(k.organization)
		row.AddCell().SetDate(k.document_date)
		row.AddCell().SetString(k.provider_name)
		row.AddCell().SetString(k.merchant_account_name)
		row.AddCell().SetString(k.operation_type)
		row.AddCell().SetString(k.region)
		row.AddCell().SetString(k.payment_type)
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.balance_currency.Name)
		row.AddCell().SetInt(v.count_operations)

		// cell = row.AddCell()
		// cell.SetFloat(v.balance_amount)
		// cell.SetFormat("0.00")

		// cell = row.AddCell()
		// cell.SetFloat(v.BR_balance_currency)
		// cell.SetFormat("0.0000")

		// cell = row.AddCell()
		// cell.SetFloat(v.surcharge_amount)
		// cell.SetFormat("0.00")

		// cell = row.AddCell()
		// cell.SetFloat(v.Extra_BR_balance_currency)
		// cell.SetFormat("0.0000")

		// cell = row.AddCell()
		// cell.SetFloat(v.channel_amount)
		// cell.SetFormat("0.00")

		util.AddCellWithFloat(row, v.balance_amount, k.balance_currency.GetAccuracy(2))
		util.AddCellWithFloat(row, v.BR_balance_currency, k.balance_currency.GetAccuracy(4))
		util.AddCellWithFloat(row, v.surcharge_amount, 2)
		util.AddCellWithFloat(row, v.Extra_BR_balance_currency, k.balance_currency.GetAccuracy(4))
		util.AddCellWithFloat(row, v.channel_amount, 2)

		row.AddCell().SetString(k.channel_currency.Name)
		row.AddCell().SetString(k.contractor_merchant)

		row.AddCell().SetString("DragonPay")
		row.AddCell().SetString(k.provider1c)

	}
}
