package crypto

import (
	"app/currency"
	"app/util"
	"app/validation"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Operation3 struct {
	Charge_id               string    `db:"charge_id"`
	Date                    time.Time `db:"date"`
	Merchant_email          string    `db:"merchant_email"`
	Project_name            string    `db:"project_name"`
	Transaction_type        string    `db:"transaction_type"`
	Transaction_id          string    `db:"transaction_id"`
	Status                  string    `db:"status"`
	Amount                  float64   `db:"amount"`
	Network                 string    `db:"network"`
	Fee                     float64   `db:"fee"`
	Fee_network             string    `db:"fee_network"`
	Merchant_amount         float64   `db:"merchant_amount"`
	Merchant_amount_network string    `db:"merchant_amount_network"`
	Fee_payer               string    `db:"fee_payer"`
	Transfer_fee            float64   `db:"transfer_fee"`
	Transfer_fee_network    string    `db:"transfer_fee_network"`
	Transfer_fee_rate       float64   `db:"transfer_fee_rate"`
	Transfer_fee_rate_usdt  float64   `db:"transfer_fee_rate_usdt"`
	Markup_amount           float64   `db:"markup_amount"`
	Markup_amount_usdt      float64   `db:"markup_amount_usdt"`

	Currency_str                 string `db:"currency"`
	Fee_currency_str             string `db:"fee_currency"`
	Merchant_amount_currency_str string `db:"merchant_amount_currency"`
	Transfer_fee_currency_str    string `db:"transfer_fee_currency"`
	Markup_amount_currency_str   string `db:"markup_amount_currency"`

	Currency                 currency.Currency
	Fee_currency             currency.Currency
	Merchant_amount_currency currency.Currency
	Transfer_fee_currency    currency.Currency
	Markup_amount_currency   currency.Currency
}

func (o *Operation3) StartingFill() {

	o.Currency = currency.New(o.Currency_str)
	o.Fee_currency = currency.New(o.Fee_currency_str)
	o.Merchant_amount_currency = currency.New(o.Merchant_amount_currency_str)
	o.Transfer_fee_currency = currency.New(o.Transfer_fee_currency_str)
	o.Markup_amount_currency = currency.New(o.Markup_amount_currency_str)

}

func ReadFile3(filename string) (ops []Operation3, err error) {

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
	err = validation.CheckMapOfColumnNames(map_fileds, "crypto3")
	if err != nil {
		return nil, err
	}

	ops = make([]Operation3, 0, len(records))

	for i, record := range records {

		if i == 0 {
			continue
		}

		// Date,Time (UTC+0),Merchant email,Project name,Charge ID,Transaction type,
		// Transaction id,Transaction Status,Invoice Amount (fiat),Invoice currency,
		// Transaction Amount,Transaction currency,Transaction network,Fee,Fee currency,
		// Fee network,Merchant amount,Merchant amount currency,Merchant amount network,
		// Fee payer,Transfer fee,Transfer fee currency,Transfer fee network,Transfer fee rate,
		// "Transfer fee rate, USDT",Markup amount,Markup amount currency,Markup amount (USDT)

		o := Operation3{}
		o.Transaction_id = record[map_fileds["transaction id"]-1]
		dateTime := fmt.Sprintf("%s %s:00", record[map_fileds["date"]-1], record[map_fileds["time (utc+0)"]-1])
		o.Date = util.GetDateFromString(dateTime)
		o.Merchant_email = record[map_fileds["merchant email"]-1]
		o.Project_name = record[map_fileds["project name"]-1]
		o.Transaction_type = record[map_fileds["transaction type"]-1]
		o.Status = record[map_fileds["transaction status"]-1]
		o.Currency_str = record[map_fileds["transaction currency"]-1]
		o.Amount, _ = strconv.ParseFloat(record[map_fileds["transaction amount"]-1], 64)
		o.Network = record[map_fileds["transaction network"]-1]
		o.Fee, _ = strconv.ParseFloat(record[map_fileds["fee"]-1], 64)
		o.Fee_currency_str = record[map_fileds["fee currency"]-1]
		o.Fee_network = record[map_fileds["fee network"]-1]
		o.Merchant_amount, _ = strconv.ParseFloat(record[map_fileds["merchant amount"]-1], 64)
		o.Merchant_amount_currency_str = record[map_fileds["merchant amount currency"]-1]
		o.Merchant_amount_network = record[map_fileds["merchant amount network"]-1]
		o.Fee_payer = record[map_fileds["fee payer"]-1]
		o.Transfer_fee, _ = strconv.ParseFloat(record[map_fileds["transfer fee"]-1], 64)
		o.Transfer_fee_currency_str = record[map_fileds["transfer fee currency"]-1]
		o.Transfer_fee_network = record[map_fileds["transfer fee network"]-1]
		o.Transfer_fee_rate, _ = strconv.ParseFloat(record[map_fileds["transfer fee rate"]-1], 64)
		o.Transfer_fee_rate_usdt, _ = strconv.ParseFloat(record[map_fileds["transfer fee rate, usdt"]-1], 64)
		o.Markup_amount, _ = strconv.ParseFloat(record[map_fileds["markup amount"]-1], 64)
		o.Markup_amount_currency_str = record[map_fileds["markup amount currency"]-1]
		o.Markup_amount_usdt, _ = strconv.ParseFloat(record[map_fileds["markup amount (usdt)"]-1], 64)

		idx := map_fileds["charge id"]
		if idx > 0 {
			o.Charge_id = record[map_fileds["charge id"]-1]
		}

		o.StartingFill()

		ops = append(ops, o)

	}

	return ops, nil
}
