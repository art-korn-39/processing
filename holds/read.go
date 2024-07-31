package holds

import (
	"app/currency"
	"app/logs"
	"app/validation"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
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
			if row.Cells[i].Value == "Схема" {
				headerLine = slices.Index(sheet.Rows, row)
				break
			}
		}
	}

	if headerLine == 0 {
		logs.Add(logs.FATAL, errors.New("[холды] не обнаружена строка с названием колоноки (\"Схема\")"))
		return
	}

	start_time := time.Now()

	map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[headerLine].Cells)
	err := validation.CheckMapOfColumnNames(map_fileds, "holds")
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	Data = make([]Hold, 0, len(sheet.Rows))

	for _, row := range sheet.Rows {

		if slices.Index(sheet.Rows, row) <= headerLine {
			continue
		}

		if row.Cells[1].String() == "" {
			break
		}

		hold := Hold{}

		hold.Schema = row.Cells[map_fileds["схема"]-1].String()
		hold.Currency = currency.New(row.Cells[map_fileds["валюта"]-1].String())
		hold.MA_id, _ = row.Cells[map_fileds["ma_id"]-1].Int()
		hold.MA_name = row.Cells[map_fileds["ma_name"]-1].String()
		hold.DateStart, _ = row.Cells[map_fileds["дата старта"]-1].GetTime(false)

		percent_str := strings.ReplaceAll(row.Cells[map_fileds["процент холда"]-1].String(), "%", "")
		hold.Percent, _ = strconv.ParseFloat(percent_str, 64)
		hold.Percent = hold.Percent / 100

		hold.Days, _ = row.Cells[map_fileds["кол-во дней"]-1].Int()

		Data = append(Data, hold)

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение условий холдов: %v [%d строк]", time.Since(start_time), len(Data)))

}
