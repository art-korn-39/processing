package kgx

import (
	"app/currency"
	"app/logs"
	"app/validation"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/tealeg/xlsx"
)

func ReadSheet(sheet *xlsx.Sheet) {

	// определение строки с названиями колонок
	headerLine := 0
	for _, row := range sheet.Rows {

		if len(row.Cells) < 2 {
			continue
		}

		for i := range row.Cells {
			if row.Cells[i].Value == "Баланс" {
				headerLine = slices.Index(sheet.Rows, row)
				break
			}
		}
	}

	if headerLine == 0 {
		logs.Add(logs.FATAL, errors.New("[kgx] не обнаружена строка с названием колонки (\"Баланс\")"))
		return
	}

	start_time := time.Now()

	map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[headerLine].Cells)
	err := validation.CheckMapOfColumnNames(map_fileds, "kgx")
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	data = make([]KGX_line, 0, len(sheet.Rows))

	for _, row := range sheet.Rows {

		if slices.Index(sheet.Rows, row) <= headerLine {
			continue
		}

		if row.Cells[1].String() == "" {
			break
		}

		kgx_line := KGX_line{}

		kgx_line.Balance = row.Cells[map_fileds["баланс"]-1].String()
		kgx_line.Operation_type = row.Cells[map_fileds["operation_type"]-1].String()
		kgx_line.Balance_currency = currency.New(row.Cells[map_fileds["валюта баланса"]-1].String())
		kgx_line.Payment_type = row.Cells[map_fileds["payment_type_id / payment_method_type"]-1].String()
		kgx_line.Provider1c = row.Cells[map_fileds["поставщик 1с"]-1].String()

		data = append(data, kgx_line)

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение страницы KGX: %v [%d строк]", time.Since(start_time), len(data)))

}

func LineContains(balance, operation_type, payment_type string, balance_currency currency.Currency) bool {

	for _, op := range data {

		if op.Balance == balance &&
			op.Operation_type == operation_type &&
			op.Payment_type == payment_type &&
			op.Balance_currency == balance_currency {
			return true
		}

	}

	return false
}

func GetProvider1c(balance, operation_type, payment_type string, balance_currency currency.Currency) string {

	for _, op := range data {

		if op.Balance == balance &&
			op.Operation_type == operation_type &&
			op.Payment_type == payment_type &&
			op.Balance_currency == balance_currency {
			return op.Provider1c
		}

	}

	return ""
}

func GetDataLen() int {
	return len(data)
}
