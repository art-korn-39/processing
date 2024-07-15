package processing

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
	"app/validation"
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
		err := CH_ReadRegistry()
		if err != nil {
			logs.Add(logs.FATAL, err)
		}
		//CH_ReadRegistry_async()
		//CH_ReadRegistry_async2()
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
	var mu sync.Mutex

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

	map_fileds := validation.GetMapOfColumnNamesStrings(headers)
	err = validation.CheckMapOfColumnNames(map_fileds, "bof_registry")
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	// 150 000 records -> 43.500.000 bytes (~0.004)
	capacity := fileInfo.Size() * 4 / 1000

	storage.Registry = make([]*Operation, 0, capacity)

	channel_records := make(chan []string, 1000)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
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
		Payment_type:        record[map_fileds["payment_type_id / payment_method_type"]-1],
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

func CH_ReadRegistry() error {

	start_time := time.Now()

	Statement := querrys.Stat_Select_reports()

	merchant_str := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(config.Get().Registry.Merchant_id)), ","), "[]")

	Statement = strings.ReplaceAll(Statement, "$1", config.Get().Registry.DateFrom.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$2", config.Get().Registry.DateTo.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	err := storage.Clickhouse.Select(&storage.Registry, Statement)

	if err != nil {
		return err
	}

	for _, o := range storage.Registry {
		o.StartingFill()
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Registry))))

	return nil

}

func CH_ReadRegistry_async() error {

	var wg sync.WaitGroup
	var mu sync.Mutex

	start_time := time.Now()

	merchant_str := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(config.Get().Registry.Merchant_id)), ","), "[]")

	Statement := `SELECT COUNT(*) 
	FROM reports
	WHERE 
		billing__billing_operation_created_at BETWEEN toDateTime('$1') AND toDateTime('$2')
		AND billing__merchant_id IN ($3)`
	Statement = strings.ReplaceAll(Statement, "$1", config.Get().Registry.DateFrom.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$2", config.Get().Registry.DateTo.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	var count_rows int
	storage.Clickhouse.Get(&count_rows, Statement)

	storage.Registry = make([]*Operation, 0, count_rows)

	channel_dates := GetChannelOfDays(config.Get().Registry.DateFrom,
		config.Get().Registry.DateTo,
		24*time.Hour)

	Statement = querrys.Stat_Select_reports()
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {
				stat := strings.ReplaceAll(Statement, "$1", period.startDay.Format(time.DateTime))
				stat = strings.ReplaceAll(stat, "$2", period.endDay.Format(time.DateTime))

				res := []*Operation{}
				err := storage.Clickhouse.Select(&res, stat)
				if err != nil {
					logs.Add(logs.FATAL, "Clickhouse.Select() - ", err)
				}
				for _, o := range res {
					o.StartingFill()
				}
				mu.Lock()
				storage.Registry = append(storage.Registry, res...)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse async: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Registry))))

	return nil
}

// without get count
func CH_ReadRegistry_async2() error {

	var wg sync.WaitGroup
	var mu sync.Mutex

	start_time := time.Now()

	merchant_str := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(config.Get().Registry.Merchant_id)), ","), "[]")

	storage.Registry = make([]*Operation, 0, 1000000)

	channel_dates := GetChannelOfDays(config.Get().Registry.DateFrom,
		config.Get().Registry.DateTo,
		24*time.Hour)

	Statement := querrys.Stat_Select_reports()
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {
				stat := strings.ReplaceAll(Statement, "$1", period.startDay.Format(time.DateTime))
				stat = strings.ReplaceAll(stat, "$2", period.endDay.Format(time.DateTime))

				res := []*Operation{}
				err := storage.Clickhouse.Select(&res, stat)
				if err != nil {
					logs.Add(logs.FATAL, "Clickhouse.Select() - ", err)
				}
				for _, o := range res {
					o.StartingFill()
				}
				mu.Lock()
				storage.Registry = append(storage.Registry, res...)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse async2: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Registry))))

	return nil
}
