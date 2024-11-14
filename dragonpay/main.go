package dragonpay

import (
	"app/config"
	"app/logs"
	"app/storage"

	"github.com/jmoiron/sqlx"
)

var (
	registry map[int]Operation
	handbook map[string]string
)

func init() {
	registry = make(map[int]Operation, 1000)
	handbook = make(map[string]string, 200)
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

	readFiles(config.Get().Dragonpay.Filename)
	insertIntoDB(db)

}

func Read_Registry(db *sqlx.DB) {

	PSQL_read_registry(db)

}

func GetOperation(id int, endpoint_id string) (*Operation, string) {
	if id != 0 {
		op, ok := registry[id]
		if ok {
			return &op, op.Provider1c
		} else {
			return nil, handbook[endpoint_id]
		}
	}
	return nil, ""
}
