package processing

import (
	"app/config"
	"app/logs"
	"app/util"
	"app/validation"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tealeg/xlsx"
)

const RANGE_MAX = float64(1000000000000)

func Read_Tariffs() {

	if config.Get().Tariffs.Storage == config.PSQL {
		util.Unused()
	} else {
		Read_XLSX_Tariffs()
	}

}

func Read_XLSX_Tariffs() {

	if config.Get().Tariffs.Filename == "" {
		return
	}

	storage.Tariffs = make([]Tariff, 0, 1000)

	start_time := time.Now()

	xlFile, err := xlsx.OpenFile(config.Get().Tariffs.Filename)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	for _, sheet := range xlFile.Sheets {

		if sheet.Name != "Тарифы" {
			continue
		}

		// определение строки с названиями колонок
		headerLine := 0
		for _, row := range sheet.Rows {
			if len(row.Cells) < 2 {
				continue
			}
			if row.Cells[1].Value == "Баланс" {
				headerLine = slices.Index(sheet.Rows, row)
				break
			}
		}

		if headerLine == 0 {
			logs.Add(logs.FATAL, errors.New("[тарифы] не обнаружена строка с названиями колонок (\"Баланс\" в колонке \"B\")"))
			return
		}

		map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[headerLine].Cells)
		err = validation.CheckMapOfColumnNames(map_fileds, "tariffs")
		if err != nil {
			logs.Add(logs.FATAL, err)
			return
		}

		for _, row := range sheet.Rows {

			if slices.Index(sheet.Rows, row) <= headerLine {
				continue
			}

			if row.Cells[1].String() == "" {
				break
			}

			tariff := Tariff{}

			tariff.Balance_name = row.Cells[map_fileds["баланс"]-1].String()
			tariff.Merchant = row.Cells[map_fileds["мерчант"]-1].String()
			tariff.Merchant_account_name = row.Cells[map_fileds["man"]-1].String()
			tariff.Merchant_account_id, _ = row.Cells[map_fileds["merchant account id"]-1].Int()
			tariff.Balance_code = row.Cells[map_fileds["код баланса по справочнику"]-1].String()

			tariff.Provider = row.Cells[map_fileds["provider"]-1].String()
			tariff.Schema = row.Cells[map_fileds["схема"]-1].String()
			tariff.Convertation = row.Cells[map_fileds["конверт"]-1].String()
			tariff.Operation_type = row.Cells[map_fileds["operation_type"]-1].String()
			tariff.RR_days, _ = row.Cells[map_fileds["рр, дней (пс)"]-1].Int()

			tariff.Balance_id, _ = row.Cells[map_fileds["id баланса в бофе"]-1].Int()
			tariff.Balance_type = row.Cells[map_fileds["тип баланса в бофе (in/ out/ in-out)"]-1].String()
			tariff.id, _ = row.Cells[map_fileds["tarif_condition_id"]-1].Int()

			tariff.Subdivision1C = row.Cells[map_fileds["подразделение 1с"]-1].String()
			tariff.Provider1C = row.Cells[map_fileds["поставщик в 1с"]-1].String()
			tariff.RatedAccount = row.Cells[map_fileds["расчетный счет"]-1].String()

			tariff.CurrencyBM = NewCurrency(row.Cells[map_fileds["валюта баланса мерчанта в боф"]-1].String())
			tariff.CurrencyBP = NewCurrency(row.Cells[map_fileds["валюта учетная"]-1].String())

			percent_str := strings.ReplaceAll(row.Cells[map_fileds["%"]-1].String(), "%", "")
			tariff.Percent, _ = strconv.ParseFloat(percent_str, 64)
			tariff.Percent = tariff.Percent / 100

			tariff.Fix, _ = strconv.ParseFloat(row.Cells[map_fileds["fix"]-1].String(), 64) // "числовой"
			tariff.Min, _ = strconv.ParseFloat(row.Cells[map_fileds["min"]-1].String(), 64)
			tariff.Max, _ = strconv.ParseFloat(row.Cells[map_fileds["max"]-1].String(), 64) // "общий"

			tariff.RangeMIN, _ = row.Cells[map_fileds["range min"]-1].Float() // "все форматы"
			tariff.RangeMIN = util.TR(math.IsNaN(tariff.RangeMIN), float64(0), tariff.RangeMIN).(float64)

			tariff.RangeMAX, _ = row.Cells[map_fileds["range max"]-1].Float()
			tariff.RangeMAX = util.TR(math.IsNaN(tariff.RangeMAX), float64(0), tariff.RangeMAX).(float64)
			tariff.RangeMAX = util.TR(tariff.RangeMAX == 0, RANGE_MAX, tariff.RangeMAX).(float64)

			tariff.RR_percent, _ = strconv.ParseFloat(row.Cells[map_fileds["рр, процент (пс)"]-1].String(), 64)

			tariff.DateStartPS, _ = row.Cells[map_fileds["дата нач.раб пс"]-1].GetTime(false)
			tariff.DateStart, _ = row.Cells[map_fileds["дата старта"]-1].GetTime(false)

			tariff.SetFormula()

			if tariff.Schema == "Crypto" {
				tariff.IsCrypto = true
			}

			storage.Tariffs = append(storage.Tariffs, tariff)

		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение тарифов: %v", time.Since(start_time)))

}

func CheckRequiredFileds_Tariffs(map_fileds map[string]int) error {

	M := []string{
		"Баланс", "Мерчант", "Merchant Account ID", "Provider", "Валюта баланса мерчанта в БОФ",
		"Валюта учетная", "Дата Старта", "Конверт", "operation_type",
		"%", "Fix", "Min", "Max", "Range min", "Range max", "ID Баланса в бофе", "tarif_condition_id",

		"ID Баланса в бофе", "ТИП Баланса в Бофе (IN/ OUT/ IN-OUT)", "Подразделение 1С", "Поставщик в 1С", "Расчетный счет",
		"РР, Процент (ПС)", "Дата нач.раб ПС", "Схема", "РР, дней (ПС)", "Код Баланса по справочнику",
	}

	for _, v := range M {

		_, ok := map_fileds[v]
		if !ok {
			return errors.New("Отсуствует обязательное поле! (" + v + ")")
		}

	}

	return nil

}
