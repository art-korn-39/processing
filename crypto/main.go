package crypto

import (
	"app/config"
	"app/logs"
	"app/storage"

	"github.com/jmoiron/sqlx"
)

var (
	Registry  map[int]Operation
	Registry3 map[string]Operation3
)

func init() {
	Registry = make(map[int]Operation, 300000)
	Registry3 = make(map[string]Operation3, 300000)
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

	Read_CSV_files(config.Get().Crypto.Filename)
	insert_into_db(db)
	insert_into_db3(db)

}

func Read_Registry(db *sqlx.DB) {

	if config.Get().Crypto.Storage == config.PSQL {
		PSQL_read_registry(db)
	} else {
		Read_CSV_files(config.Get().Crypto.Filename)
	}

}

func GetNetwork(id int) string {
	return Registry[id].Network
}
