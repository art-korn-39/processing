package provider_registry

import (
	"app/config"
	"app/querrys"

	"github.com/jmoiron/sqlx"
)

var (
	registry Registry
	rates    []Operation
)

func init() {
	registry = make(map[int]*LinkedOperation, 1000000)
	rates = make([]Operation, 0, 1000000)
}

func Read_Registry(db *sqlx.DB, registry_done chan querrys.Args, by_merchant bool) {

	if config.Get().Provider_registry.Storage == config.PSQL {
		if by_merchant {
			PSQL_read_registry_by_merchant_async(db, registry_done)
		} else {
			PSQL_read_registry_by_provider_async(db, registry_done)
		}
	} else {
		Read_XLSX_files(config.Get().Provider_registry.Filename)
	}

}
