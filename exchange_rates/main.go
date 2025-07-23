package exchange_rates

import (
	"app/currency"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var data []rate

type rate struct {
	Contractor_name       string    `db:"contractor_name"`
	Contractor_guid       string    `db:"contractor_guid"`
	Provider_balance_name string    `db:"provider_balance_name"`
	Provider_balance_guid string    `db:"provider_balance_guid"`
	Balance_currency_str  string    `db:"balance_currency"`
	Channel_currency_str  string    `db:"channel_currency"`
	Operation_type        string    `db:"operation_type"`
	Date                  time.Time `db:"date"`
	Rate                  float64   `db:"rate"`
	Provider_id           int       `db:"provider_id"`
	Provider_name         string    `db:"provider_name"`

	Balance_currency currency.Currency
	Channel_currency currency.Currency
}

func init() {
	data = make([]rate, 1000)
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_providers_exchange_rates()

	err := db.Select(&data, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for i := range data {

		r := &data[i]
		r.Operation_type = strings.ToLower(r.Operation_type)
		r.Balance_currency = currency.New(r.Balance_currency_str)
		r.Channel_currency = currency.New(r.Channel_currency_str)

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение курсов валют из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(data))))

}

func GetRate(date time.Time, channel_currency, balance_currency currency.Currency, operation_type, provider_balance_guid string) *rate {

	day := util.TruncateToDay(date)

	for _, v := range data {
		if v.Date.Equal(day) &&
			v.Channel_currency == channel_currency &&
			v.Operation_type == operation_type &&
			v.Provider_balance_guid == provider_balance_guid {

			return &v
		}
	}
	return nil
}
