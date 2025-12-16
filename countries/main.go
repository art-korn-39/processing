package countries

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	data_code2    map[string]Country
	data_currency map[string]Country
)

func Read_Data(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_countries()

	slice_countries := []Country{}

	err := db.Select(&slice_countries, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	data_code2 = map[string]Country{}
	data_currency = map[string]Country{}

	for _, country := range slice_countries {
		data_code2[country.Code2] = country
		data_currency[country.Currency] = country
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение стран: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(data_code2))))

}

func GetCountry(code2, currency string) (country Country) {
	if code2 != "" {
		country = data_code2[code2]
	} else {
		if currency == "USD" {
			country = data_code2["US"]
		} else if currency == "EUR" {
			country = data_code2["DE"]
		} else {
			country = data_currency[currency]
		}
	}
	return
}
