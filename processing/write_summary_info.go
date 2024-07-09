package processing

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

	f := xlsx.NewFile()
	sheet, _ := f.AddSheet("Копируем")
	xlsx.SetDefaultFont(11, "Calibri")

	headers := []string{"Баланс", "IDBALANCE", "Дата", "Проверка", "operation_type", "issuer_country",
		"payment_method_type", "merchant_name", "project_name", "merchant_account_name", "Подразделение", "Рассчетный счет",
		"Поставщик 1С", "real_currency / channel_currency", "Валюта баланса", "Акт. тариф", "Акт. Фикс",
		"Акт. Мин", "Акт. Макс", "Range min", "Range max", "Старт тарифа", "tariff_condition_id", "contract_id",
		"PPрасхолд", "CryptoNetWork", "real_amount / channel_amount", "real_amount, fee", "Сумма в валюте баланса",
		"SR Balance Currency", "ChecFee", "Кол-во операций", "Сумма холда"}

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
	sheet.SetColWidth(1, 1, 11)   // idbalance
	sheet.SetColWidth(2, 2, 12)   // дата
	sheet.SetColWidth(3, 3, 16)   // проверка
	sheet.SetColWidth(4, 4, 15)   // operation_type
	sheet.SetColWidth(6, 6, 18)   // payment_method_type
	sheet.SetColWidth(9, 9, 35)   // merchant_account_name
	sheet.SetColWidth(10, 10, 16) // подразделение
	sheet.SetColWidth(11, 11, 25) // рассчетный счет
	sheet.SetColWidth(12, 12, 16) // поставщик 1С
	sheet.SetColWidth(13, 13, 14) // real_currency / channel_currency
	sheet.SetColWidth(21, 21, 12) // старт тарифа
	sheet.SetColWidth(22, 22, 16) // tariff_condition_id
	sheet.SetColWidth(23, 23, 12) // contract_id
	sheet.SetColWidth(24, 24, 12) // PPрасхолд
	sheet.SetColWidth(26, 32, 16)

	var cell *xlsx.Cell

	for k, v := range M {
		row := sheet.AddRow()
		row.AddCell().SetString(k.balance_name)
		row.AddCell().SetInt(k.balance_id)
		row.AddCell().SetDate(k.document_date)
		row.AddCell().SetString(k.verification)
		row.AddCell().SetString(k.operation_type)
		row.AddCell().SetString(k.country)
		row.AddCell().SetString(k.payment_type)
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.project_name)
		row.AddCell().SetString(k.merchant_account_name)
		row.AddCell().SetString(k.subdivision1C)
		row.AddCell().SetString(k.ratedAccount)
		row.AddCell().SetString(k.provider1C)
		row.AddCell().SetString(k.channel_currency.Name)
		row.AddCell().SetString(k.currencyBP.Name)

		cell = row.AddCell()
		cell.SetFloat(k.percent)
		cell.SetFormat("0.00%")

		cell = row.AddCell()
		cell.SetFloat(k.fix)
		cell.SetFormat("0.00")

		row.AddCell().SetFloat(k.min)
		row.AddCell().SetFloat(k.max)
		row.AddCell().SetFloat(k.range_min)
		row.AddCell().SetFloat(k.range_max)

		if k.date_start.IsZero() {
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.date_start)
		}

		row.AddCell().SetInt(k.tariff_condition_id)
		row.AddCell().SetInt(k.contract_id)

		if k.PP_rashold.IsZero() {
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.PP_rashold)
		}

		row.AddCell().SetString(k.Crypto_network)

		cell = row.AddCell()
		cell.SetFloat(v.channel_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		//cell.SetFloat(v.fee_amount)
		cell.SetFloat(v.SR_channel_currency)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.balance_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.SR_balance_currency)
		cell.SetFormat("0.00")

		row.AddCell().SetFloat(v.checkFee)
		row.AddCell().SetInt(v.count_operations)

		cell = row.AddCell()
		cell.SetFloat(v.PP_amount)
		cell.SetFormat("0.00")
	}

	err := f.Save(config.Get().SummaryInfo.Filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить данные в Excel файл: ошибка совместного доступа к файлу")
		return
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение данных в Excel файл: %v", time.Since(start_time)))

}
