package origamix

import (
	"app/config"
	"app/logs"
	"app/storage"

	"github.com/jmoiron/sqlx"
)

var (
	Registry map[int]Operation
)

func init() {
	Registry = make(map[int]Operation, 300000)
}

func Start() {

	cfg := config.Get()

	var err error
	db, err := storage.PSQL_connect(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	} else {
		logs.Add(logs.INFO, "Successful connection to Postgres")
	}
	defer db.Close()

	Read_CSV_files(config.Get().Origamix.Filename)
	insert_into_db(db)

}

func Read_Registry(db *sqlx.DB) {

	if config.Get().Origamix.Storage == config.PSQL {
		PSQL_read_registry(db)
	} else {
		Read_CSV_files(config.Get().Origamix.Filename)
	}

}
