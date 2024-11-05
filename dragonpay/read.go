package dragonpay

import (
	"app/currency"
	"app/logs"
	"app/util"
	"app/validation"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tealeg/xlsx"
)

const HANDBOOK_NAME = "handbook.xlsx"

func read_files(folder string) {

	start_time := time.Now()

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	err = read_xlsx_file(filenames)
	if err != nil {
		logs.Add(logs.MAIN, err)
		return
	}

	var files_readed int64

	for _, filename := range filenames {

		if filepath.Ext(filename) != ".csv" {
			continue
		}

		operations, err := read_csv_file(filename)
		if err != nil {
			logs.Add(logs.ERROR, filename, " : ", err)
			continue
		}

		atomic.AddInt64(&files_readed, 1)

		for _, o := range operations {
			Registry[o.Id] = o
		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение файлов: %v [%d прочитано]", time.Since(start_time), files_readed))

}

func read_csv_file(filename string) (ops []Operation, err error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	header := records[0]
	header_len := len(header)
	map_fileds := validation.GetMapOfColumnNamesStrings(header)
	err = validation.CheckMapOfColumnNames(map_fileds, "dragonpay_csv")
	if err != nil {
		return nil, err
	}

	ops = make([]Operation, 0, len(records))

	for i, record := range records {

		if i == 0 {
			continue
		}

		if record[0] == "" {
			continue
		}

		var arr_record []string
		if len(record) > 1 {
			arr_record = record
		} else {
			arr_record = strings.Split(record[0], ",")
		}

		if len(arr_record) != header_len {
			continue
		}

		o := Operation{}
		o.Id, _ = strconv.Atoi(arr_record[map_fileds["merchant txn id"]-1])
		o.Create_date = util.GetDateFromString2(arr_record[map_fileds["create date"]-1])
		//o.Settle_date = util.GetDateFromString2(arr_record[map_fileds["settle date"]-1])
		o.Refno = arr_record[map_fileds["refno"]-1]
		o.Currency = currency.New(arr_record[map_fileds["ccy"]-1])
		o.Currency_str = o.Currency.Name
		o.Amount, _ = strconv.ParseFloat(arr_record[map_fileds["amount"]-1], 64)
		o.Endpoint_id = arr_record[map_fileds["proc"]-1]
		o.Fee_amount, _ = strconv.ParseFloat(arr_record[map_fileds["fee"]-1], 64)

		o.Provider1c = Handbook[o.Endpoint_id]

		idx := map_fileds["settle date"]
		if idx > 0 {
			o.Settle_date = util.GetDateFromString2(arr_record[idx-1])
		} else {
			idx := map_fileds["success date"]
			if idx > 0 {
				o.Settle_date = util.GetDateFromString2(arr_record[idx-1])
			}
		}

		ops = append(ops, o)

	}

	return ops, nil
}

func read_xlsx_file(filenames []string) error {

	var xlsx_file string
	for _, filename := range filenames {
		if filepath.Base(filename) == HANDBOOK_NAME {
			xlsx_file = filename
			break
		}
	}

	if xlsx_file == "" {
		return fmt.Errorf("не обнаружен файл %s", HANDBOOK_NAME)
	}

	xlFile, err := xlsx.OpenFile(xlsx_file)
	if err != nil {
		return err
	}

	for _, sheet := range xlFile.Sheets {

		sheet_name := strings.ToLower(sheet.Name)
		if sheet_name == "dragonpay" {

			err = read_dragonpay_sheet(sheet)
			if err != nil {
				return err
			}

		}

	}

	return nil

}

func read_dragonpay_sheet(sheet *xlsx.Sheet) error {

	if len(sheet.Rows) < 2 || sheet.Rows[0].Cells[0].Value != "endpoint_id" {
		return fmt.Errorf("некорректный формат файла %s", HANDBOOK_NAME)
	}

	map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[0].Cells)
	err := validation.CheckMapOfColumnNames(map_fileds, "dragonpay_xlsx")
	if err != nil {
		return err
	}

	for i, row := range sheet.Rows {

		if i == 0 {
			continue
		}

		if len(row.Cells) == 0 || row.Cells[0].String() == "" {
			break
		}

		endpoint_id := row.Cells[map_fileds["endpoint_id"]-1].String()
		provider := row.Cells[map_fileds["поставщик dragonpay"]-1].String()

		Handbook[endpoint_id] = provider

	}

	return nil

}
