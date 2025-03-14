package tariff_provider

import (
	"app/config"
	"app/logs"
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

func Read_Sources(db *sqlx.DB) {

	if config.Get().Tariffs.Storage == config.PSQL {
		Read_PSQL_Tariffs(db)
	} else {
		Read_XLSX_Tariffs()
	}

}

func Read_XLSX_Tariffs() {

	if config.Get().Tariffs.Filename == "" {
		return
	}

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
			if row.Cells[1].Value == "Провайдер" {
				headerLine = slices.Index(sheet.Rows, row)
				break
			}
		}

		if headerLine == 0 {
			logs.Add(logs.FATAL, errors.New("[тарифы] не обнаружена строка с названиями колонок (\"Провайдер\" в колонке \"B\")"))
			return
		}

		map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[headerLine].Cells)
		err = validation.CheckMapOfColumnNames(map_fileds, "tariff_provider")
		if err != nil {
			logs.Add(logs.FATAL, err)
			return
		}

		data = make([]*Tariff, 0, 1000)

		for _, row := range sheet.Rows {

			if slices.Index(sheet.Rows, row) <= headerLine {
				continue
			}

			if len(row.Cells) == 0 || row.Cells[0].String() == "" {
				break
			}

			tariff := Tariff{}

			// tariff.ID_revise = row.Cells[map_fileds["идентификатор сверки"]-1].String()
			// tariff.Provider = row.Cells[map_fileds["провайдер"]-1].String()
			// tariff.Organization = row.Cells[map_fileds["организация"]-1].String()
			//tariff.Provider_name = row.Cells[map_fileds["provider_name"]-1].String()
			tariff.DateStart, _ = row.Cells[map_fileds["date_of_start"]-1].GetTime(false)
			tariff.Merchant_name = row.Cells[map_fileds["merchant_name"]-1].String()
			tariff.Merchant_account_name = row.Cells[map_fileds["merchant_account_name"]-1].String()
			tariff.Merchant_legal_entity, _ = row.Cells[map_fileds["merchant_legal_entity"]-1].Int()
			tariff.Payment_method = row.Cells[map_fileds["payment_method"]-1].String()
			tariff.Payment_method_type = row.Cells[map_fileds["payment_method_type"]-1].String()
			tariff.Region = row.Cells[map_fileds["region"]-1].String()
			tariff.ChannelCurrency_str = row.Cells[map_fileds["channel_currency"]-1].String()
			tariff.Project = row.Cells[map_fileds["project_name"]-1].String()
			tariff.Business_type = row.Cells[map_fileds["business_type"]-1].String()
			tariff.Operation_group = row.Cells[map_fileds["operation_group"]-1].String()
			tariff.Traffic_type = row.Cells[map_fileds["traffic_type"]-1].String()
			tariff.Account_bank_name = row.Cells[map_fileds["account_bank_name"]-1].String()

			tariff.Range_turnouver_min = util.FloatFromCell(row.Cells[map_fileds["tariff range turnouver min"]-1])
			tariff.Range_turnouver_max = util.FloatFromCell(row.Cells[map_fileds["tariff range turnouver max"]-1])
			tariff.Range_amount_min = util.FloatFromCell(row.Cells[map_fileds["tariff range amount min"]-1])
			tariff.Range_amount_max = util.FloatFromCell(row.Cells[map_fileds["tariff range amount max"]-1])

			percent_str := strings.ReplaceAll(row.Cells[map_fileds["percent"]-1].String(), "%", "")
			tariff.Percent, _ = strconv.ParseFloat(percent_str, 64)
			//tariff.Percent = tariff.Percent / 100

			tariff.Fix = util.FloatFromCell(row.Cells[map_fileds["fix"]-1])
			tariff.Min = util.FloatFromCell(row.Cells[map_fileds["min commission"]-1])
			tariff.Max = util.FloatFromCell(row.Cells[map_fileds["max commission"]-1])

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

func FindTariffForOperation(op Operation, field_balance_guid string) *Tariff {

	if len(data) == 0 {
		return nil
	}

	get_result := func(s []*Tariff) *Tariff {
		if len(s) > 0 {
			// сортируем по убыванию используемых полей
			sort.Slice(s,
				func(i int, j int) bool {
					return s[i].CountUsefulFields > s[j].CountUsefulFields
				})
			return s[0]
		}
		return nil
	}

	operation_balance_guid := op.GetString(field_balance_guid)
	transaction_completed_at := op.GetTime("Transaction_completed_at")
	transaction_created_at := op.GetTime("Transaction_created_at")

	if operation_balance_guid == "" {
		return nil
	}

	current_date_range := data[0].DateStart
	selected_tariffs := []*Tariff{}

	for _, t := range data {

		if operation_balance_guid != t.Provider_balance_guid {
			continue
		}

		// если это более ранняя дата, то смотрим текущий массив подходящих тарифов
		if !t.DateStart.Equal(current_date_range) {
			if tariff := get_result(selected_tariffs); tariff != nil {
				return tariff
			} else { // переходим на более ранний диапазон дат
				current_date_range = t.DateStart
				selected_tariffs = []*Tariff{}
			}
		}

		var operation_date time.Time
		if t.Use_transaction_created_at {
			operation_date = transaction_created_at
		} else {
			operation_date = transaction_completed_at
		}

		if t.DateStart.Before(operation_date) &&
			t.Operation_group == op.GetString("Operation_group") {

			if !t.IsValidForOperation(op) {
				continue
			}

			// проверяем наличие диапазона
			if t.Range_amount_min != 0 || t.Range_amount_max != 0 {

				// определелям попадание в диапазон тарифа если он заполнен
				channel_amount := op.GetFloat("Channel_amount")
				if channel_amount > t.Range_amount_min &&
					channel_amount <= t.Range_amount_max {
					selected_tariffs = append(selected_tariffs, t)
				}

			} else {
				selected_tariffs = append(selected_tariffs, t)
			}

		}
	}

	return get_result(selected_tariffs)
}

func (t *Tariff) IsValidForOperation(op Operation) bool {

	if t.Merchant_name != "" && t.Merchant_name != op.GetString("Merchant_name") {
		return false
	}

	if t.Merchant_account_name != "" && t.Merchant_account_name != op.GetString("Merchant_account_name") {
		return false
	}

	if t.Merchant_legal_entity != 0 && t.Merchant_legal_entity != op.GetInt("Legal_entity_id") {
		return false
	}

	if t.Payment_method != "" && t.Payment_method != op.GetString("Payment_method") {
		return false
	}

	if t.Payment_method_type != "" && t.Payment_method_type != op.GetString("Payment_type") {
		return false
	}

	if t.Region != "" && t.Region != op.GetString("Region") {
		return false
	}

	if t.Project != "" && t.Project != op.GetString("Project_name") {
		return false
	}

	if t.Business_type != "" && t.Business_type != op.GetString("Business_type") {
		return false
	}

	if t.ChannelCurrency.Name != "" && t.ChannelCurrency != op.Get_Balance_currency() {
		return false
	}

	if t.Traffic_type != "" && t.Traffic_type != op.GetString("Traffic_type") {
		return false
	}

	if t.Account_bank_name != "" && t.Account_bank_name != op.GetString("Account_bank_name") {
		return false
	}

	return true

}
