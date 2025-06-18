package dragonpay

import (
	"app/config"
	"app/currency"
	"app/file"
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

	"github.com/jmoiron/sqlx"
	"github.com/tealeg/xlsx"
)

const HANDBOOK_NAME = "handbook.xlsx"

func readFiles(db *sqlx.DB, folder string) (files []*file.FileInfo, err error) {

	start_time := time.Now()

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		return nil, err
	}

	// Handbook
	// err = readXLSXfile(filenames)
	// if err != nil {
	// 	return nil, err
	// }

	files = file.GetFiles(filenames, file.DRAGON_PAY, ".csv")
	new_files := []*file.FileInfo{}

	var files_readed int64
	var count_skipped int64

	for _, file := range files {

		if !config.Debug {
			file.GetLastUpload(db)
			if file.LastUpload.After(file.Modified) {
				atomic.AddInt64(&count_skipped, 1)
				continue
			}
		}

		operations, err := readCSVfile(file.Filename)
		if err != nil {
			logs.Add(logs.ERROR, file.Filename, " : ", err)
			continue
		}

		atomic.AddInt64(&files_readed, 1)

		for _, o := range operations {
			registry[o.Id] = o
		}

		file.Rows = len(operations)
		file.LastUpload = time.Now()

		new_files = append(new_files, file)

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение файлов: %v [%d пропущено, %d прочитано]", time.Since(start_time), count_skipped, files_readed))

	return new_files, nil

}

func readCSVfile(filename string) (ops []Operation, err error) {

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

		o.Provider1c = handbook[o.Endpoint_id].Provider1c

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

func readXLSXfile(filenames []string) error {

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

			err = readDragonpaySheet(sheet)
			if err != nil {
				return err
			}

		}

	}

	return nil

}

func readDragonpaySheet(sheet *xlsx.Sheet) error {

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

		handbook[endpoint_id] = Accord{Provider1c: provider}

	}

	return nil

}
