package provider

import (
	"app/config"
	"app/querrys"

	"github.com/jmoiron/sqlx"
)

var (
	Registry registry
	Rates    rates
)

func init() {
	Registry = make(map[int]*LinkedOperation, 1000000)
	Rates = make([]Operation, 0, 1000000)
}

func Read_Registry(db *sqlx.DB, registry_done chan querrys.Args) {

	if config.Get().Rates.Storage == config.PSQL {
		//PSQL_read_registry(db, registry_done)
		PSQL_read_registry_async(db, registry_done)
	} else {
		Read_XLSX_files(config.Get().Rates.Filename)
	}

}
