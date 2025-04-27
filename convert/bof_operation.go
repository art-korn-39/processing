package convert

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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

type Bof_operation struct {
	Operation_id             string    `db:"operation_id"`
	Provider_payment_id      string    `db:"provider_payment_id"`
	Transaction_created_at   time.Time `db:"transaction_created_at"`
	Transaction_completed_at time.Time `db:"transaction_completed_at"`
	Provider_id              int       `db:"provider_id"`
	Project_id               int       `db:"project_id"`
	Provider_name            string    `db:"provider_name"`
	Merchant_name            string    `db:"merchant_name"`
	Merchant_account_id      int       `db:"merchant_account_id"`
	Merchant_account_name    string    `db:"merchant_account_name"`
	Operation_type_id        int       `db:"operation_type_id"`
	Operation_type           string
	Payment_type             string `db:"payment_type"`
	Country_code2            string `db:"country"`

	Channel_amount       float64 `db:"channel_amount"`
	Channel_currency_str string  `db:"channel_currency"`
	Channel_currency     currency.Currency
}

func (op *Bof_operation) GetTime(name string) time.Time {
	var result time.Time
	switch name {
	case "Operation_created_at":
		result = op.Transaction_completed_at
	case "Transaction_completed_at":
		result = op.Transaction_completed_at
	case "Transaction_created_at":
		result = op.Transaction_created_at
	default:
		logs.Add(logs.ERROR, "неизвестное поле time: ", name)
	}
	return result
}
func (op *Bof_operation) GetInt(name string) int {
	var result int
	switch name {
	case "Merchant_account_id":
		result = op.Merchant_account_id
	case "Provider_id":
		result = op.Provider_id
	default:
		logs.Add(logs.ERROR, "неизвестное поле int: ", name)
	}
	return result
}

func (op *Bof_operation) GetString(name string) string {
	var result string
	switch name {
	case "Balance_type":
		result = "NULL"
	default:
		logs.Add(logs.ERROR, "неизвестное поле string: ", name)
	}
	return result
}

func (op *Bof_operation) fill() {

	op.Channel_currency = currency.New(op.Channel_currency_str)
	op.Channel_amount = util.TR(op.Channel_currency.Exponent, op.Channel_amount, op.Channel_amount/100).(float64)

	if op.Operation_type_id == 3 {
		op.Operation_type = "sale"
	} else if op.Operation_type_id == 2 {
		op.Operation_type = "capture"
	} else if op.Operation_type_id == 6 {
		op.Operation_type = "recurring"
	} else if op.Operation_type_id == 5 {
		op.Operation_type = "refund"
	} else if op.Operation_type_id == 11 {
		op.Operation_type = "payout"
	}

}

func readBofOperations(cfg *config.Config, db *sqlx.DB, key_column string) (err error) {

	start_time := time.Now()

	switch cfg.Registry.Storage {
	case config.File:
		err = readBofFile(cfg.Registry.Filename, key_column)
	case config.Clickhouse:
		err = readBofCH(db, key_column)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций БОФ: %v [%s строк]", time.Since(start_time), util.FormatInt(len(bof_registry))))

	return err

}

func readBofFile(filename string, key_column string) error {

	var wg sync.WaitGroup
	var mu sync.Mutex

	if filename == "" {
		return fmt.Errorf("файл реестра БОФ не укаазан")
	}

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'
	reader.LazyQuotes = true

	// строка с названиями колонок
	headers, _ := reader.Read()

	map_fileds := validation.GetMapOfColumnNamesStrings(headers)
	err = validation.CheckMapOfColumnNames(map_fileds, "bof_registry_raw_conversion")
	if err != nil {
		return err
	}

	channel_records := make(chan []string, 1000)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for record := range channel_records {
				op := ConvertRecordToOperation(record, map_fileds)
				op.fill()
				mu.Lock()
				switch key_column {
				case OPID:
					bof_registry[op.Operation_id] = op
				case PAYID:
					bof_registry[op.Provider_payment_id] = op
				}
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

	return nil

}

func ConvertRecordToOperation(record []string, map_fileds map[string]int) (op *Bof_operation) {

	op = &Bof_operation{

		Operation_id:             record[map_fileds["id / operation_id"]-1],
		Provider_id:              util.FR(strconv.Atoi(record[map_fileds["provider_id"]-1])).(int),
		Project_id:               util.FR(strconv.Atoi(record[map_fileds["project_id"]-1])).(int),
		Provider_payment_id:      record[map_fileds["acquirer_id / provider_payment_id"]-1],
		Merchant_account_id:      util.FR(strconv.Atoi(record[map_fileds["merchant_account_id"]-1])).(int),
		Transaction_created_at:   util.GetDateFromString(record[map_fileds["transaction_created_at"]-1]),
		Transaction_completed_at: util.GetDateFromString(record[map_fileds["completed_at / operation_completed_at"]-1]),
		Provider_name:            record[map_fileds["provider_name"]-1],
		Merchant_name:            record[map_fileds["merchant_name"]-1],
		Merchant_account_name:    record[map_fileds["merchant_account_name"]-1],
		Operation_type:           record[map_fileds["operation_type"]-1],
		Payment_type:             record[map_fileds["payment_type_id / payment_method_type"]-1],
		Country_code2:            record[map_fileds["issuer_country"]-1],
		//Status:                   record[map_fileds["operation_status"]-1],
		//Project_url:              record[map_fileds["project_url"]-1],

		Channel_currency_str: record[map_fileds["real_currency / channel_currency"]-1],
		Channel_amount:       util.FR(strconv.ParseFloat(record[map_fileds["real_amount / channel_amount"]-1], 64)).(float64),
	}

	return

}

func readBofCH(db *sqlx.DB, key_column string) error {

	var mu sync.Mutex
	var wg sync.WaitGroup

	ch := getBatchIdChan(key_column)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for b := range ch {
				statement := querrys.Stat_Select_reports_by_id()
				id_str := strings.Trim(strings.Join(b, "','"), "[]")
				statement = strings.ReplaceAll(statement, "$1", id_str)
				statement = strings.ReplaceAll(statement, "$2", key_column)
				result := []Bof_operation{}
				err := db.Select(&result, statement)
				if err != nil {
					logs.Add(logs.ERROR, err)
					continue
				}
				for _, v := range result {
					v.fill()
					mu.Lock()
					switch key_column {
					case OPID:
						bof_registry[v.Operation_id] = &v
					case PAYID:
						bof_registry[v.Provider_payment_id] = &v
					}

					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	return nil

}

func getBatchIdChan(key_column string) chan []string {

	batch_len := 10000
	ch := make(chan []string, 1000)

	go func() {
		var i int
		batch := make([]string, 0, batch_len)
		for _, v := range ext_registry {

			switch key_column {
			case "operation_id":
				batch = append(batch, v.operation_id)
			case "provider_payment_id":
				batch = append(batch, v.payment_id)
			}

			if (i+1)%batch_len == 0 {
				ch <- batch
				batch = make([]string, 0, batch_len)
			}
			i++
		}

		if len(batch) != 0 {
			ch <- batch
		}

		close(ch)
	}()

	return ch

}
