package tariff_merchant

import (
	"app/config"
	"app/currency"
	"app/holds"
	"app/logs"
	"app/querrys"
	"app/util"
	"app/validation"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/tealeg/xlsx"
)

var data []*Tariff

func Read_Sources(db *sqlx.DB, registry_done chan querrys.Args) {

	if config.Get().Tariffs.Storage == config.PSQL {
		Read_PSQL_Tariffs(db, registry_done)
	} else {
		Read_XLSX_Tariffs()
	}

}

func Read_XLSX_Tariffs() {

	if config.Get().Tariffs.Filename == "" {
		return
	}

	data = make([]*Tariff, 0, 1000)

	start_time := time.Now()

	xlFile, err := xlsx.OpenFile(config.Get().Tariffs.Filename)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	for _, sheet := range xlFile.Sheets {

		if sheet.Name == "Условия холдов" {
			holds.ReadSheet(sheet)
			continue
			// } else if sheet.Name == "KGX" {
			// 	kgx.ReadSheet(sheet)
			//continue
		} else if sheet.Name != "Тарифы" {
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
		err = validation.CheckMapOfColumnNames(map_fileds, "tariff_merchant")
		if err != nil {
			logs.Add(logs.FATAL, err)
			return
		}

		for _, row := range sheet.Rows {

			if slices.Index(sheet.Rows, row) <= headerLine {
				continue
			}

			if len(row.Cells) == 0 || row.Cells[1].String() == "" {
				break
			}

			tariff := Tariff{}

			tariff.Balance_name = row.Cells[map_fileds["баланс"]-1].String()
			tariff.Merchant = row.Cells[map_fileds["мерчант"]-1].String()
			tariff.Merchant_account_name = row.Cells[map_fileds["man"]-1].String()
			tariff.Merchant_account_id, _ = row.Cells[map_fileds["merchant account id"]-1].Int()
			tariff.Balance_code = row.Cells[map_fileds["код баланса по справочнику"]-1].String()

			tariff.Provider_name = row.Cells[map_fileds["provider"]-1].String()
			tariff.Schema = row.Cells[map_fileds["схема"]-1].String()
			tariff.Convertation = row.Cells[map_fileds["конверт"]-1].String()
			tariff.Operation_type = row.Cells[map_fileds["operation_type"]-1].String()

			tariff.Balance_id, _ = row.Cells[map_fileds["id баланса в бофе"]-1].Int()
			tariff.Balance_type = row.Cells[map_fileds["тип баланса в бофе (in/ out/ in-out)"]-1].String()
			tariff.Id, _ = row.Cells[map_fileds["tarif_condition_id"]-1].Int()

			tariff.Subdivision1C = row.Cells[map_fileds["подразделение 1с"]-1].String()
			tariff.Provider1C = row.Cells[map_fileds["поставщик в 1с"]-1].String()
			tariff.RatedAccount = row.Cells[map_fileds["расчетный счет"]-1].String()

			tariff.CurrencyBM = currency.New(row.Cells[map_fileds["валюта баланса мерчанта в боф"]-1].String())
			tariff.Balance_currency_str = row.Cells[map_fileds["валюта учетная"]-1].String()

			percent_str := strings.ReplaceAll(row.Cells[map_fileds["%"]-1].String(), "%", "")
			tariff.Percent, _ = strconv.ParseFloat(percent_str, 64)

			tariff.Fix = util.FloatFromCell(row.Cells[map_fileds["fix"]-1])
			tariff.Min = util.FloatFromCell(row.Cells[map_fileds["min"]-1])
			tariff.Max = util.FloatFromCell(row.Cells[map_fileds["max"]-1])

			tariff.RangeMIN = util.FloatFromCell(row.Cells[map_fileds["range min"]-1])

			tariff.RangeMAX = util.FloatFromCell(row.Cells[map_fileds["range max"]-1])

			tariff.RR_days, _ = row.Cells[map_fileds["рр, дней (пс)"]-1].Int()
			tariff.RR_percent, _ = strconv.ParseFloat(row.Cells[map_fileds["рр, процент (пс)"]-1].String(), 64)

			tariff.DateStartPS, _ = row.Cells[map_fileds["дата нач.раб пс"]-1].GetTime(false)
			tariff.DateStart, _ = row.Cells[map_fileds["дата старта"]-1].GetTime(false)

			tariff.DK_percent = util.FloatFromCell(row.Cells[map_fileds["%дк"]-1])
			tariff.DK_fix = util.FloatFromCell(row.Cells[map_fileds["fixдк"]-1])
			tariff.DK_min = util.FloatFromCell(row.Cells[map_fileds["minдк"]-1])
			tariff.DK_max = util.FloatFromCell(row.Cells[map_fileds["maxдк"]-1])

			idx := map_fileds["валюта комиссии"]
			if idx > 0 && len(row.Cells) >= idx {
				tariff.CurrencyCommission = row.Cells[idx-1].String()
			}

			idx = map_fileds["тип сети"]
			if idx > 0 {
				tariff.NetworkType = row.Cells[idx-1].String()
			}

			idx = map_fileds["payment_method_type"]
			if idx > 0 {
				tariff.Payment_type = row.Cells[idx-1].String()
			}

			idx = map_fileds["юр.лицо"]
			if idx > 0 {
				tariff.Company = row.Cells[idx-1].String()
			}

			tariff.StartingFill()

			data = append(data, &tariff)

		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение тарифов: %v", time.Since(start_time)))

}

func SortTariffs() {
	sort.Slice(
		data,
		func(i int, j int) bool {
			return data[i].DateStart.After(data[j].DateStart)
		},
	)
}

func FindTariffForOperation(op Operation) *Tariff {

	var operation_date time.Time
	if op.GetBool("IsPerevodix") {
		operation_date = op.GetTime("Operation_created_at")
	} else {
		operation_date = op.GetTime("Transaction_completed_at")
	}

	for _, t := range data {

		if t.DateStart.IsZero() {
			continue
		}

		// для dragonpay ищем тариф два раза
		// первый раз по отдельной схеме - через Provider1C и без MAID [ClassicTariffDragonPay = false]
		// второй раз по общей схеме, как и остальные тарифы [ClassicTariffDragonPay = true]
		isDragonPay := op.GetBool("IsDragonPay") && !op.GetBool("ClassicTariffDragonPay")

		if t.Merchant_account_id == op.GetInt("Merchant_account_id") {

			if isDragonPay && op.GetString("Provider1c") != t.Provider1C {
				continue
			}

			if t.Balance_id != 0 && op.GetInt("Balance_id") != t.Balance_id {
				continue
			}

			if t.DateStartMA.Before(operation_date) &&
				(t.DateFinishMA.After(operation_date) || t.DateFinishMA.IsZero()) &&
				t.DateStart.Before(operation_date) &&
				t.Operation_type == op.GetString("Operation_type") {

				// тип сети будет колонка в тарифе и проверять на неё
				network := op.GetString("Crypto_network")
				if t.IsCrypto && !(network == t.NetworkType || t.NetworkType == "") {
					continue
				}

				channel_currency := op.Get_Channel_currency()
				if channel_currency != t.Balance_currency && t.Convertation == "Без конверта" {
					if !(channel_currency.Name == "USD" && (t.Balance_currency.Name == "USDT" || t.Balance_currency.Name == "WMZ")) {
						continue
					}
				}

				// Для tradex ещё нужен дополнительное условие по валюте баланса, которую возьмем из provider_registry
				// if op.is_Tradex && op.prov_reg.currency == t.Balance_currency {

				// }

				if t.Payment_type != "" && t.Payment_type != op.GetString("Payment_type") {
					continue
				}

				// проверяем наличие диапазона
				if t.RangeMIN != 0 || t.RangeMAX != 0 {

					// определелям попадание в диапазон тарифа если он заполнен
					channel_amount := op.GetFloat("Channel_amount")
					if channel_amount > t.RangeMIN &&
						channel_amount <= t.RangeMAX {
						return t
					}

				} else {
					return t
				}

			}
		}
	}

	return nil
}
