package decline

import (
	"app/logs"
	"app/util"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func ReadFiles(filenames []string) {

	start_time := time.Now()

	for _, f := range filenames {

		if filepath.Ext(f) != ".json" {
			continue
		}

		var data DeclineFile
		err := util.ReadJsonFile(&data, f)
		if err != nil {
			logs.Add(logs.INFO, err)
			continue
		}

		for _, message := range data.Messages {
			if len(message.Text) < 2 {
				continue
			}

			text, ok := message.Text[1]["text"]
			if !ok {
				continue
			}

			M := map[string]string{}
			fields := strings.Split(text, "\n")
			for _, field := range fields { // key: value
				index := strings.Index(field, ":")
				if index != -1 {
					key := strings.ToLower(util.SubString(field, 0, index))
					val := util.SubString(field, index+1, len(field))
					M[key] = strings.TrimSpace(val)
				}
			}

			if len(M) > 5 {
				o := DeclineOperation{}

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

				decline_operations[o.Operation_id] = o
			}
		}
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций decline: %v [%d строк]", time.Since(start_time), len(decline_operations)))

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
