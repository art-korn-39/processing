package processing_merchant

import (
	"app/config"
	"app/currency"
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

const DUR = 24

// GET + Q best?

func Read_Registry(registry_done chan querrys.Args, channel_readers int) {
	defer close(registry_done)

	fill_channel := func(registry_done chan querrys.Args, from_cfg bool, channel_readers int) {
		args := NewQuerryArgs(from_cfg)
		for i := 1; i <= channel_readers; i++ {
			registry_done <- args
		}
	}

	if config.Get().Registry.Storage == config.Clickhouse {

		fill_channel(registry_done, true, channel_readers)

		err := CH_ReadRegistry()
		if err != nil {
			logs.Add(logs.FATAL, err)
		}

	} else {

		Read_CSV_Registry()
		sort.Slice(
			storage.Registry,
			func(i int, j int) bool {
				return storage.Registry[i].Transaction_completed_at.Before(storage.Registry[j].Transaction_completed_at)
			},
		)

		fill_channel(registry_done, false, channel_readers)
	}

}

func NewQuerryArgs(from_cfg bool) (args querrys.Args) {

	args = querrys.Args{}

	bof_reg := config.Get().Registry

	if from_cfg { // clickhouse
		args.Merchant_id = bof_reg.Merchant_id
		args.DateFrom = bof_reg.DateFrom.Add(-20 * 24 * time.Hour)
		args.DateTo = bof_reg.DateTo.Add(4 * 24 * time.Hour)
	} else { // file
		lenght := len(storage.Registry)
		if lenght > 0 {
			args.DateFrom = storage.Registry[0].Transaction_completed_at.Add(-3 * 24 * time.Hour)
			args.DateTo = storage.Registry[lenght-1].Transaction_completed_at.Add(1 * 24 * time.Hour)
		}

		for _, row := range storage.Registry {
			args.Merchant_id = append(args.Merchant_id, row.Merchant_id)
			args.Provider_id = append(args.Provider_id, row.Provider_id)

			if row.Balance_id == 0 {
				args.ID = append(args.ID, row.Operation_id)
			}
		}

		args.Merchant_id = util.Compact(args.Merchant_id)
		args.Provider_id = util.Compact(args.Provider_id)
	}

	return
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
	err = validation.CheckMapOfColumnNames(map_fileds, "bof_registry_merchant")
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
				if op.Skip {
					continue
				}
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

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из файла: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(storage.Registry))))

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
		Provider_id:         util.FR(strconv.Atoi(record[map_fileds["provider_id"]-1])).(int),
		Tariff_condition_id: util.FR(strconv.Atoi(record[map_fileds["tariff_condition_id"]-1])).(int),

		Provider_payment_id:   record[map_fileds["acquirer_id / provider_payment_id"]-1],
		Payment_type:          record[map_fileds["payment_type_id / payment_method_type"]-1],
		Operation_type:        record[map_fileds["operation_type"]-1],
		Country_code2:         record[map_fileds["issuer_country"]-1],
		Project_name:          record[map_fileds["project_name"]-1],
		Provider_name:         record[map_fileds["provider_name"]-1],
		Merchant_name:         record[map_fileds["merchant_name"]-1],
		Merchant_account_name: record[map_fileds["merchant_account_name"]-1],
		Payment_id:            record[map_fileds["external_id / payment_id"]-1],

		Count_operations:      1,
		Channel_currency_str:  record[map_fileds["real_currency / channel_currency"]-1],
		Channel_amount:        util.FR(strconv.ParseFloat(record[map_fileds["real_amount / channel_amount"]-1], 64)).(float64),
		Provider_currency_str: record[map_fileds["provider_currency"]-1],
		Provider_amount:       util.FR(strconv.ParseFloat(record[map_fileds["provider_amount"]-1], 64)).(float64),
		Fee_currency_str:      record[map_fileds["fee_currency"]-1],
		Fee_amount:            util.FR(strconv.ParseFloat(record[map_fileds["fee_amount"]-1], 64)).(float64),
		Currency_str:          record[map_fileds["currency / currency"]-1],

		Tariff_rate_percent: util.FR(strconv.ParseFloat(record[map_fileds["tariff_rate_percent"]-1], 64)).(float64),
		Tariff_rate_fix:     util.FR(strconv.ParseFloat(record[map_fileds["tariff_rate_fix"]-1], 64)).(float64),
		Tariff_rate_min:     util.FR(strconv.ParseFloat(record[map_fileds["tariff_rate_min"]-1], 64)).(float64),
		Tariff_rate_max:     util.FR(strconv.ParseFloat(record[map_fileds["tariff_rate_max"]-1], 64)).(float64),
	}

	num, _ := strconv.Atoi(record[map_fileds["is_test"]-1])
	if num == 1 && (op.Balance_id == 0 || strings.Contains(op.Provider_name, "[MOCK]")) {
		op.IsTestId = 2
		op.IsTestType = "tech test"
	}

	idx := map_fileds["created_at / operation_created_at"]
	if idx > 0 {
		op.Operation_created_at = util.GetDateFromString(record[idx-1])
	} else {
		op.Operation_created_at = op.Transaction_completed_at
	}

	idx = map_fileds["operation_actual_amount"]
	if idx > 0 {
		op.Actual_amount, _ = strconv.ParseFloat(record[idx-1], 64)
	}

	idx = map_fileds["endpoint_id"]
	if idx > 0 {
		op.Endpoint_id = record[idx-1]
	}

	idx = map_fileds["surcharge_amount"]
	if idx > 0 {
		op.Surcharge_amount, _ = strconv.ParseFloat(record[idx-1], 64)
	}

	idx = map_fileds["surcharge_currency"]
	if idx > 0 {
		op.Surcharge_currency_str = record[idx-1]
	}

	idx = map_fileds["tariff_currency"]
	if idx > 0 {
		op.Tariff_currency = currency.New(record[idx-1])
	}

	idx = map_fileds["real_provider"]
	if idx > 0 {
		op.Real_provider = record[idx-1]
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

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(storage.Registry))))

	return nil

}

func CH_ReadRegistry_async() error {

	var wg sync.WaitGroup
	var mu sync.Mutex

	start_time := time.Now()

	bof_reg := config.Get().Registry

	merchant_str := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(bof_reg.Merchant_id)), ","), "[]")

	Statement := `SELECT COUNT(*) 
	FROM reports
	WHERE 
		billing__billing_operation_created_at BETWEEN toDateTime('$1') AND toDateTime('$2')
		AND billing__merchant_id IN ($3)`
	Statement = strings.ReplaceAll(Statement, "$1", bof_reg.DateFrom.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$2", bof_reg.DateTo.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	var count_rows int
	storage.Clickhouse.Get(&count_rows, Statement)

	storage.Registry = make([]*Operation, 0, count_rows)

	channel_dates := util.GetChannelOfDays(bof_reg.DateFrom,
		bof_reg.DateTo,
		DUR*time.Hour)

	Statement = querrys.Stat_Select_reports()
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {
				stat := strings.ReplaceAll(Statement, "$1", period.StartDay.Format(time.DateTime))
				stat = strings.ReplaceAll(stat, "$2", period.EndDay.Format(time.DateTime))

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

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse async: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(storage.Registry))))

	return nil
}

// without get count
func CH_ReadRegistry_async2() error {

	var wg sync.WaitGroup
	var mu sync.Mutex

	start_time := time.Now()

	bof_reg := config.Get().Registry

	merchant_str := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(bof_reg.Merchant_id)), ","), "[]")

	storage.Registry = make([]*Operation, 0, 1000000)

	channel_dates := util.GetChannelOfDays(bof_reg.DateFrom,
		bof_reg.DateTo,
		DUR*time.Hour)

	Statement := querrys.Stat_Select_reports()
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {
				stat := strings.ReplaceAll(Statement, "$1", period.StartDay.Format(time.DateTime))
				stat = strings.ReplaceAll(stat, "$2", period.EndDay.Format(time.DateTime))

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

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse async NO GET: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(storage.Registry))))

	return nil
}

func CH_ReadRegistry_async_querry() error {

	var wg sync.WaitGroup
	var mu sync.Mutex

	start_time := time.Now()

	bof_reg := config.Get().Registry

	merchant_str := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(bof_reg.Merchant_id)), ","), "[]")

	storage.Registry = make([]*Operation, 0, 1000000)

	channel_dates := util.GetChannelOfDays(bof_reg.DateFrom,
		bof_reg.DateTo,
		DUR*time.Hour)

	Statement := querrys.Stat_Select_reports()
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {
				stat := strings.ReplaceAll(Statement, "$1", period.StartDay.Format(time.DateTime))
				stat = strings.ReplaceAll(stat, "$2", period.EndDay.Format(time.DateTime))

				res := make([]*Operation, 0, 10000)

				rows, err := storage.Clickhouse.Queryx(stat)
				if err != nil {
					logs.Add(logs.FATAL, err)
					return
				}

				for rows.Next() {
					var op Operation
					if err := rows.StructScan(&op); err != nil {
						logs.Add(logs.FATAL, err)
						return
					}
					op.StartingFill()
					res = append(res, &op)
				}

				mu.Lock()
				storage.Registry = append(storage.Registry, res...)
				mu.Unlock()

			}
		}()
	}

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse async NO GET + Q: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(storage.Registry))))

	return nil
}

func CH_ReadRegistry_async_querry_cap() error {

	var wg sync.WaitGroup
	var mu sync.Mutex

	start_time := time.Now()

	bof_reg := config.Get().Registry

	merchant_str := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(bof_reg.Merchant_id)), ","), "[]")

	Statement := `SELECT COUNT(*) 
	FROM reports
	WHERE 
		billing__billing_operation_created_at BETWEEN toDateTime('$1') AND toDateTime('$2')
		AND billing__merchant_id IN ($3)`
	Statement = strings.ReplaceAll(Statement, "$1", bof_reg.DateFrom.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$2", bof_reg.DateTo.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	var count_rows int
	storage.Clickhouse.Get(&count_rows, Statement)

	storage.Registry = make([]*Operation, 0, count_rows)

	channel_dates := util.GetChannelOfDays(bof_reg.DateFrom,
		bof_reg.DateTo,
		DUR*time.Hour)

	Statement = querrys.Stat_Select_reports()
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	//fmt.Println(config.NumCPU)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {
				stat := strings.ReplaceAll(Statement, "$1", period.StartDay.Format(time.DateTime))
				stat = strings.ReplaceAll(stat, "$2", period.EndDay.Format(time.DateTime))

				res := make([]*Operation, 0, 100000)

				rows, err := storage.Clickhouse.Queryx(stat)
				if err != nil {
					logs.Add(logs.FATAL, err)
					return
				}

				for rows.Next() {
					var op Operation
					if err := rows.StructScan(&op); err != nil {
						logs.Add(logs.FATAL, err)
						return
					}
					op.StartingFill()
					res = append(res, &op)
				}

				//fmt.Println(len(res))

				mu.Lock()
				storage.Registry = append(storage.Registry, res...)
				mu.Unlock()

			}
		}()
	}

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse async GET + Q: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(storage.Registry))))

	return nil
}
