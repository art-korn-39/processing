package decline

import (
	"app/file"
	"app/util"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

func readJSON(db *sqlx.DB, filename string) ([]*Operation, *file.FileInfo, error, bool) {

	var data DeclineFile
	var err error

	operations := []*Operation{}

	// чтение метаданных файла
	data.fileInfo, err = file.New(filename, "decline")
	if err != nil {
		return operations, nil, err, false
	}

	file := data.fileInfo

	// проверка на последние изменения
	file.GetLastUpload(db)
	if file.LastUpload.After(file.Modified) {
		return operations, nil, nil, true
	}

	// чтение JSON содержимого
	err = util.ReadJsonFile(&data, filename)
	if err != nil {
		return operations, nil, err, false
	}

	var count_rows int

	for _, message := range data.Messages {
		if len(message.Text) < 2 {
			continue
		}

		// берём элемент с ключом "plain" из text_entities
		// у него находим элемент в котором type = plain
		var text string
		for _, map_ := range message.Text {
			if map_["type"] == "plain" {
				text = text + map_["text"]
			}
		}

		if text == "" {
			continue
		}

		var bank_card int
		for _, map_ := range message.Text {
			if map_["type"] == "bank_card" {
				bank_card, _ = strconv.Atoi(map_["text"])
			}
		}

		ops := strings.Split(text, "\n\n\n")
		for _, operation_str := range ops {

			M := map[string]string{}
			fields := strings.Split(operation_str, "\n")
			for _, field := range fields { // key: value
				index := strings.Index(field, ":")
				if index != -1 {
					key := strings.ToLower(util.SubString(field, 0, index))
					val := util.SubString(field, index+1, len(field))
					M[key] = strings.TrimSpace(val)
				}
			}

			if len(M) > 5 {
				o := Operation{}

				o.Date = util.GetDateFromString(message.Date_str)
				o.Created_at = util.GetDateFromString(M["created at"])

				o.Date_day = o.Date.Truncate(24 * time.Hour)
				o.Created_at_day = o.Created_at.Truncate(24 * time.Hour)

				o.Message_id = message.Id
				o.Operation_id, _ = strconv.Atoi(M["operation id"])
				o.Operation_type = M["operation type"]
				o.Comment = M["comment/proof link"]

				o.Merchant_id, o.Merchant_name = GetIDandName(M["merchant"])
				o.Provider_id, o.Provider_name = GetIDandName(M["provider"])
				o.Merchant_account_id, o.Merchant_account_name = GetIDandName(M["merchant account"])

				o.Incoming_amount, o.Incoming_currency = GetAmountAndCurrency(M["incoming amount"])
				o.Coverted_amount, o.Coverted_currency = GetAmountAndCurrency(M["coverted amount"])

				o.Bank_card = bank_card
				if o.Operation_id == 0 {
					o.Operation_id = bank_card
				}

				operations = append(operations, &o)

			}
			count_rows++
		}
	}

	file.LastUpload = time.Now()
	file.Rows = count_rows

	//список прочитанных файлов
	return operations, file, nil, false

}

func GetAmountAndCurrency(s string) (amount float64, currency string) {

	if s == "" {
		return
	}

	i := strings.Index(s, " ")
	if i == -1 {
		return
	}

	slice := strings.Split(s, " ")
	if len(slice) == 2 {
		amount, _ = strconv.ParseFloat(slice[0], 64)
		currency = slice[1]
	}

	return

}

func GetIDandName(s string) (id int, name string) {

	if s == "" {
		return
	}

	i := strings.Index(s, " ")
	if i == -1 {
		return
	}

	part1 := util.SubString(s, 0, i)
	id_str := strings.Trim(part1, "[]")
	id, _ = strconv.Atoi(id_str)

	name = util.SubString(s, i+1, len(s))
	return

}
