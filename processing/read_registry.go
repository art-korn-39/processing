package processing

import (
	"app/config"
	"app/logs"
	"app/util"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func Read_Registry(registry_done chan struct{}) {

	if config.Get().Registry.Storage == config.Clickhouse {
		close(registry_done)
		CH_ReadRegistry()
	} else {
		defer close(registry_done)
		Read_CSV_Registry()
		sort.Slice(
			storage.Registry,
			func(i int, j int) bool {
				return storage.Registry[i].Transaction_completed_at.Before(storage.Registry[j].Transaction_completed_at)
			},
		)
	}

}

func Read_CSV_Registry() {

	var wg sync.WaitGroup

	if config.Get().Registry.Filename == "" {
		return
	}

	start_time := time.Now()

	file, err := os.Open(config.Get().Registry.Filename)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()

	reader := csv.NewReader(file)
	reader.Comma = ';'
	reader.LazyQuotes = true

	// строка с названиями колонок
	headers, _ := reader.Read()

	// мапа соответствий: имя колонки - индекс
	map_fileds := map[string]int{}
	for i, field := range headers {
		map_fileds[field] = i + 1
	}

	// проверяем наличие обязательных полей
	err = CheckRequiredFileds_Registry(map_fileds)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// 150 000 records -> 43.500.000 bytes (~0.004)
	capacity := fileInfo.Size() * 4 / 1000

	storage.Registry = make([]*Operation, 0, capacity)

	channel_records := make(chan []string, 1000)

	wg.Add(NUM_GORUTINES)
	for i := 1; i <= NUM_GORUTINES; i++ {
		go func() {
			defer wg.Done()
			for record := range channel_records {
				op := ConvertRecordToOperation(record, map_fileds)
				op.StartingFill()
				mu.Lock()
				storage.Registry = append(storage.Registry, op)
				mu.Unlock()
			}
		}()
	}

	// чтение csv StartingFill построчно и запись в канал
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		channel_records <- record
	}
	close(channel_records)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Registry))))

}

func ConvertRecordToOperation(record []string, map_fileds map[string]int) (op *Operation) {

	op = &Operation{

		Transaction_completed_at: util.GetDateFromString(record[map_fileds["transaction_completed_at"]-1]),

		Operation_id:        util.FR(strconv.Atoi(record[map_fileds["id / operation_id"]-1])).(int),
		Transaction_id:      util.FR(strconv.Atoi(record[map_fileds["transaction_id"]-1])).(int),
		Merchant_id:         util.FR(strconv.Atoi(record[map_fileds["merchant_id"]-1])).(int),
		Merchant_account_id: util.FR(strconv.Atoi(record[map_fileds["merchant_account_id"]-1])).(int),
		Balance_id:          util.FR(strconv.Atoi(record[map_fileds["balance_id"]-1])).(int),
		Contract_id:         util.FR(strconv.Atoi(record[map_fileds["contract_id"]-1])).(int),
		Project_id:          util.FR(strconv.Atoi(record[map_fileds["project_id"]-1])).(int),
		Tariff_condition_id: util.FR(strconv.Atoi(record[map_fileds["tariff_condition_id"]-1])).(int),

		Provider_name: record[map_fileds["provider_name"]-1],
		IsDragonPay:   strings.Contains(record[map_fileds["provider_name"]-1], "Dragonpay"),

		Provider_payment_id: record[map_fileds["acquirer_id / provider_payment_id"]-1],
		Payment_method_type: record[map_fileds["payment_type_id / payment_method_type"]-1],
		Operation_type:      record[map_fileds["operation_type"]-1],
		Country:             record[map_fileds["issuer_country"]-1],
		Project_name:        record[map_fileds["project_name"]-1],

		Merchant_name:         record[map_fileds["merchant_name"]-1],
		Merchant_account_name: record[map_fileds["merchant_account_name"]-1],

		Count_operations:      1,
		Channel_currency_str:  record[map_fileds["real_currency / channel_currency"]-1],
		Channel_amount:        util.FR(strconv.ParseFloat(record[map_fileds["real_amount / channel_amount"]-1], 64)).(float64),
		Provider_currency_str: record[map_fileds["provider_currency"]-1],
		Provider_amount:       util.FR(strconv.ParseFloat(record[map_fileds["provider_amount"]-1], 64)).(float64),
		Fee_currency_str:      record[map_fileds["fee_currency"]-1],
		Fee_amount:            util.FR(strconv.ParseFloat(record[map_fileds["fee_amount"]-1], 64)).(float64),

		Tariff_bof: &Tariff{
			Percent: util.FR(strconv.ParseFloat(record[map_fileds["tariff_rate_percent"]-1], 64)).(float64) / 100,
			Fix:     util.FR(strconv.ParseFloat(record[map_fileds["tariff_rate_fix"]-1], 64)).(float64),
			Min:     util.FR(strconv.ParseFloat(record[map_fileds["tariff_rate_min"]-1], 64)).(float64),
			Max:     util.FR(strconv.ParseFloat(record[map_fileds["tariff_rate_max"]-1], 64)).(float64),
		},
	}

	return

}

func CheckRequiredFileds_Registry(map_fileds map[string]int) error {

	M := []string{
		"id / operation_id", "transaction_id", "transaction_completed_at",
		"merchant_id", "merchant_account_id", "project_id", "project_name",
		"provider_name", "merchant_name", "merchant_account_name",
		"acquirer_id / provider_payment_id", "issuer_country",
		"operation_type", "balance_id", "payment_type_id / payment_method_type",
		"contract_id", "tariff_condition_id",
		"real_currency / channel_currency", "real_amount / channel_amount",
		"fee_currency", "fee_amount",
		"provider_currency", "provider_amount",
		"tariff_rate_percent", "tariff_rate_fix", "tariff_rate_min", "tariff_rate_max",
	}

	for _, v := range M {

		_, ok := map_fileds[v]
		if !ok {
			return fmt.Errorf("отсуствует обязательное поле: %s", v)
		}

	}

	return nil

}
