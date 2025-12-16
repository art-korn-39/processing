package crypto

import (
	"app/currency"
	"app/util"
	"app/validation"
	"encoding/csv"
	"os"
	"strconv"
	"time"
)

// 224 bytes
type Operation struct {
	Id                     int       `db:"operation_id"`
	Network                string    `db:"network"`
	Created_at             time.Time `db:"created_at"`
	Created_at_day         time.Time `db:"created_at_day"`
	Operation_type         string    `db:"operation_type"`
	Payment_amount         float64   `db:"payment_amount"`
	Payment_currency_str   string    `db:"payment_currency"`
	Crypto_amount          float64   `db:"crypto_amount"`
	Crypto_currency_str    string    `db:"crypto_currency"`
	Transfer_fee_rate_USDT float64   `db:"transfer_fee_rate_usdt"`

	Payment_currency currency.Currency
	Crypto_currency  currency.Currency
}

func (o *Operation) StartingFill() {

	o.Payment_currency = currency.New(o.Payment_currency_str)
	o.Crypto_currency = currency.New(o.Crypto_currency_str)

}

func ReadFile(filename string) (ops []Operation, err error) {

	file, err := os.Open(filename)
	if err != nil {
		//logs.Add(logs.ERROR, fmt.Sprint("os.Open() ", err))
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		//logs.Add(logs.ERROR, "reader.ReadAll() ", filename, ": ", err)
		return nil, err
	}

	map_fileds := validation.GetMapOfColumnNamesStrings(records[0])
	err = validation.CheckMapOfColumnNames(map_fileds, "crypto")
	if err != nil {
		return nil, err
	}

	ops = make([]Operation, 0, len(records))

	for i, record := range records {

		if i == 0 {
			continue
		}

		o := Operation{}
		o.Id, _ = strconv.Atoi(record[map_fileds["operation id"]-1])
		o.Network = record[map_fileds["crypto network"]-1]
		o.Created_at = util.GetDateFromString(record[map_fileds["created at"]-1])
		o.Created_at_day = o.Created_at
		o.Operation_type = record[map_fileds["operation type"]-1]
		o.Payment_amount, _ = strconv.ParseFloat(record[map_fileds["payment amount"]-1], 64)
		o.Payment_currency_str = record[map_fileds["payment currency"]-1]
		o.Crypto_amount, _ = strconv.ParseFloat(record[map_fileds["crypto amount"]-1], 64)
		o.Crypto_currency_str = record[map_fileds["crypto currency"]-1]
		o.Transfer_fee_rate_USDT, _ = strconv.ParseFloat(record[map_fileds["transfer fee rate, usdt"]-1], 64)

		o.StartingFill()

		ops = append(ops, o)

	}

	return ops, nil
}
