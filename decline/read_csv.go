package decline

import (
	"app/file"
	"app/util"
	"app/validation"
	"encoding/csv"
	"os"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
)

//id,from_status,to_status,brand_id,brand_title,merchant_id,merchant_title,
//provider_id,provider_title,operation_type,incoming_amount,currency_incoming,
//coverted_amount,currency_converted,merchant_account_id,merchant_account_title,proof_link,created_at

func readCSV(db *sqlx.DB, filename string) ([]*Operation, *file.FileInfo, error, bool) {

	operations := []*Operation{}

	file, err := file.New(filename, "decline")
	if err != nil {
		return operations, nil, err, false
	}

	// проверка на последние изменения
	file.GetLastUpload(db)
	if file.LastUpload.After(file.Modified) {
		return operations, nil, nil, true
	}

	f, err := os.Open(filename)
	if err != nil {
		return operations, nil, err, false
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = ','
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		return operations, nil, err, false
	}

	map_fileds := validation.GetMapOfColumnNamesStrings(records[0])
	err = validation.CheckMapOfColumnNames(map_fileds, "decline_csv")
	if err != nil {
		return operations, nil, err, false
	}

	for i, record := range records {

		if i == 0 {
			continue
		}

		o := Operation{}
		o.Operation_id, _ = strconv.Atoi(record[map_fileds["id"]-1])
		o.Created_at = util.GetDateFromString(record[map_fileds["created_at"]-1])
		o.Created_at_day = o.Created_at.Truncate(24 * time.Hour)
		o.Merchant_id, _ = strconv.Atoi(record[map_fileds["merchant_id"]-1])
		o.Merchant_name = record[map_fileds["merchant_title"]-1]
		o.Provider_id, _ = strconv.Atoi(record[map_fileds["provider_id"]-1])
		o.Provider_name = record[map_fileds["provider_title"]-1]
		o.Merchant_account_id, _ = strconv.Atoi(record[map_fileds["merchant_account_id"]-1])
		o.Merchant_account_name = record[map_fileds["merchant_account_title"]-1]
		o.Operation_type = record[map_fileds["operation_type"]-1]
		o.Incoming_amount, _ = strconv.ParseFloat(record[map_fileds["incoming_amount"]-1], 64)
		o.Incoming_currency = record[map_fileds["currency_incoming"]-1]
		o.Coverted_amount, _ = strconv.ParseFloat(record[map_fileds["coverted_amount"]-1], 64)
		o.Coverted_currency = record[map_fileds["currency_converted"]-1]
		o.Link = record[map_fileds["proof_link"]-1]

		o.Date = o.Created_at
		o.Date_day = o.Created_at_day

		operations = append(operations, &o)

	}

	file.LastUpload = time.Now()
	file.Rows = len(records)

	return operations, file, nil, false

}
