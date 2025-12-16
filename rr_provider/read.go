package rr_provider

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	data []*Tariff
)

func init() {
	data = []*Tariff{}
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_rr_provider()

	//slice_providers := []Tariff{}

	err := db.Select(&data, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	// for _, rr_provider := range slice_providers {

	// 	data = append(data, rr_provider)

	// }

	logs.Add(logs.INFO, fmt.Sprintf("Чтение RR провайдеров: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(data))))

}

// func GetRR_merchant(project_id int) (*RR_merchant, bool) {
// 	// m, ok := data[project_id]
// 	// return m, ok
// 	//return nil
// }
