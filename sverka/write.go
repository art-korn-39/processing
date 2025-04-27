package sverka

import (
	"app/config"
	"app/logs"
	"fmt"
	"time"

	"github.com/tealeg/xlsx"
)

func writeResult(cfg *config.Config, differ_table []differ_struct) {

	start_time := time.Now()

	xlsx.SetDefaultFont(11, "Calibri")

	f := xlsx.NewFile()

	add_page_sverka(f, differ_table)

	err := f.Save(cfg.SummaryInfo.Filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить данные в Excel файл: ошибка совместного доступа к файлу")
		return
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение данных в Excel файл: %v", time.Since(start_time)))

}

// br, created_at, channel_amount
func add_page_sverka(f *xlsx.File, differ_table []differ_struct) {

	sheet, _ := f.AddSheet("Сверка")

	headers := []string{"operation_id", "provider_payment_id",
		"transaction_created_at (provider)", "transaction_created_at (detailed)", "transaction_created_at (bof)",
		"channel_amount (provider)", "channel_amount (detailed)", "channel_amount (bof)",
		"br amount (provider)", "br amount (detailed)",
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

	for _, v := range differ_table {

		prov_op := v.Provider_operation
		det_op := v.Detailed_operation
		bof_op := v.Bof_operation

		row := sheet.AddRow()

		row.AddCell().SetInt(prov_op.Id)
		row.AddCell().SetString(prov_op.Provider_payment_id)

		// TRANSACTION_CREATED_AT
		if prov_op.Transaction_created_at.IsZero() {
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(prov_op.Transaction_created_at)
		}

		if det_op.Transaction_created_at.IsZero() {
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(det_op.Transaction_created_at)
		}

		if bof_op.Transaction_created_at.IsZero() {
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(bof_op.Transaction_created_at)
		}

		// CHANNEL AMOUNT
		cell = row.AddCell()
		cell.SetFloat(prov_op.Channel_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(det_op.Channel_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(bof_op.Channel_amount)
		cell.SetFormat("0.00")

		// BR AMOUNT
		cell = row.AddCell()
		cell.SetFloat(prov_op.BR_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(det_op.BR_balance_currency)
		cell.SetFormat("0.00")

	}

}
