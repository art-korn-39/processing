package test_merchant_accounts

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	data map[int]*[]Test_MA
)

func init() {
	data = map[int]*[]Test_MA{}
}

type Test_MA struct {
	Date_start          time.Time `db:"date_start"`
	Date_finish         time.Time `db:"date_finish"`
	Merchant_id         int       `db:"merchant_id"`
	Merchant_account_id int       `db:"merchant_account_id"`
	Operation_type      string    `db:"operation_type"`
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_test_merchant_accounts()

	slice := []Test_MA{}

	err := db.Select(&slice, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, v := range slice {

		s0, ok := data[v.Merchant_account_id]

		var s []Test_MA
		if ok {
			s = append(*s0, v)
		} else {
			s = append(s, v)
		}

		data[v.Merchant_account_id] = &s

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение тестовых МА из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(slice))))

}

func Skip(date time.Time, ma_id int, operation_type string) bool {

	s, ok := data[ma_id]
	if ok {
		for _, v := range *s {

			if v.Date_start.Before(date) && v.Date_finish.After(date) &&
				(v.Operation_type == "" || v.Operation_type == operation_type) {

				return true
			}
		}
	}

	return false

}
