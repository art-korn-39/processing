package providers

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	data_tradex map[int]bool
	data        map[int]*Provider
)

func init() {
	data_tradex = make(map[int]bool, 10000)
	data = make(map[int]*Provider, 10000)
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_providers()

	slice_providers := []Provider{}

	err := db.Select(&slice_providers, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, provider := range slice_providers {

		data_tradex[provider.Provider_id] = provider.Is_tradex
		data[provider.Provider_id] = &provider

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение провайдеров: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(slice_providers))))

}

func Is_tradex(provider_id int) bool {

	return data_tradex[provider_id]

}

func GetByID(provider_id int) (*Provider, bool) {

	p, ok := data[provider_id]
	return p, ok

}
