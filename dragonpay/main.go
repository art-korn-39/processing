package dragonpay

import (
	"app/config"
	"app/logs"
	"app/storage"

	"github.com/jmoiron/sqlx"
)

var (
	Registry map[int]Operation
	Handbook map[string]string
)

func init() {
	Registry = make(map[int]Operation, 1000)
	Handbook = make(map[string]string, 200)
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

	read_files(config.Get().Dragonpay.Filename)
	insert_into_db(db)

}

func Read_Registry(db *sqlx.DB) {

	PSQL_read_registry(db)

}

func GetOperation(id int, endpoint_id string) (*Operation, string) {
	if id != 0 {
		op, ok := Registry[id]
		if ok {
			return &op, op.Provider1c
		} else {
			return nil, Handbook[endpoint_id]
		}
	}
	return nil, ""
}
