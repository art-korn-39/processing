package processing

import (
	"app/config"
	"app/logs"
	"app/util"
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

	logs.Add(logs.INFO, fmt.Sprintf("Открытие файла с тарифами: %v", time.Since(start_time)))

	start_time = time.Now()

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
		}

		// мапа соответствий: имя колонки - индекс
		map_fileds := map[string]int{}
		for i, cell := range sheet.Rows[headerLine].Cells {
			map_fileds[cell.String()] = i + 1
		}

		// проверяем наличие обязательных полей
		err = CheckRequiredFileds_Tariffs(map_fileds)
		if err != nil {
			logs.Add(logs.ERROR, err)
		}

		for _, row := range sheet.Rows {

			if slices.Index(sheet.Rows, row) <= headerLine {
				continue
			}

			if row.Cells[1].String() == "" {
				break
			}

			tariff := Tariff{}

			tariff.Balance_name = row.Cells[map_fileds["Баланс"]-1].String()
			tariff.Merchant = row.Cells[map_fileds["Мерчант"]-1].String()
			tariff.Merchant_account_name = row.Cells[map_fileds["MAN"]-1].String()
			tariff.Merchant_account_id, _ = row.Cells[map_fileds["Merchant Account ID"]-1].Int()
			tariff.Balance_code = row.Cells[map_fileds["Код Баланса по справочнику"]-1].String()

			tariff.Provider = row.Cells[map_fileds["Provider"]-1].String()
			tariff.Schema = row.Cells[map_fileds["Схема"]-1].String()
			tariff.Convertation = row.Cells[map_fileds["Конверт"]-1].String()
			tariff.Operation_type = row.Cells[map_fileds["operation_type"]-1].String()
			tariff.PP_days, _ = row.Cells[map_fileds["РР, дней (ПС)"]-1].Int()

			tariff.Balance_id, _ = row.Cells[map_fileds["ID Баланса в бофе"]-1].Int()
			tariff.Balance_type = row.Cells[map_fileds["ТИП Баланса в Бофе (IN/ OUT/ IN-OUT)"]-1].String()
			tariff.id, _ = row.Cells[map_fileds["tarif_condition_id"]-1].Int()

			tariff.Subdivision1C = row.Cells[map_fileds["Подразделение 1С"]-1].String()
			tariff.Provider1C = row.Cells[map_fileds["Поставщик в 1С"]-1].String()
			tariff.RatedAccount = row.Cells[map_fileds["Расчетный счет"]-1].String()

			tariff.CurrencyBM = NewCurrency(row.Cells[map_fileds["Валюта баланса мерчанта в БОФ"]-1].String())
			tariff.CurrencyBP = NewCurrency(row.Cells[map_fileds["Валюта учетная"]-1].String())

			percent_str := strings.ReplaceAll(row.Cells[map_fileds["%"]-1].String(), "%", "")
			tariff.Percent, _ = strconv.ParseFloat(percent_str, 64)
			tariff.Percent = tariff.Percent / 100

			tariff.Fix, _ = strconv.ParseFloat(row.Cells[map_fileds["Fix"]-1].String(), 64) // "числовой"
			tariff.Min, _ = strconv.ParseFloat(row.Cells[map_fileds["Min"]-1].String(), 64)
			tariff.Max, _ = strconv.ParseFloat(row.Cells[map_fileds["Max"]-1].String(), 64) // "общий"

			tariff.RangeMIN, _ = row.Cells[map_fileds["Range min"]-1].Float() // "все форматы"
			tariff.RangeMIN = util.TR(math.IsNaN(tariff.RangeMIN), float64(0), tariff.RangeMIN).(float64)

			tariff.RangeMAX, _ = row.Cells[map_fileds["Range max"]-1].Float()
			tariff.RangeMAX = util.TR(math.IsNaN(tariff.RangeMAX), float64(0), tariff.RangeMAX).(float64)
			tariff.RangeMAX = util.TR(tariff.RangeMAX == 0, RANGE_MAX, tariff.RangeMAX).(float64)

			tariff.PP_percent, _ = strconv.ParseFloat(row.Cells[map_fileds["РР, Процент (ПС)"]-1].String(), 64)

			tariff.DateStartPS, _ = row.Cells[map_fileds["Дата нач.раб ПС"]-1].GetTime(false)
			tariff.DateStart, _ = row.Cells[map_fileds["Дата Старта"]-1].GetTime(false)

			tariff.SetFormula()

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
