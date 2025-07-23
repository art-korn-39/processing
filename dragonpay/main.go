package dragonpay

import (
	"app/config"
	"app/logs"
	"app/storage"

	"github.com/jmoiron/sqlx"
)

var (
	registry map[int]Operation
	handbook map[string]Accord
)

func init() {
	registry = make(map[int]Operation, 1000)
	handbook = make(map[string]Accord, 200)
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

	PSQL_read_registry(db, true)

	files, err := readFiles(db, config.Get().Dragonpay.Filename)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	err = insertIntoDB(db)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	for _, f := range files {
		f.InsertIntoDB(db, 0)
	}

}

func Read_Registry(db *sqlx.DB, handbookOnly bool) {

	PSQL_read_registry(db, handbookOnly)

}

func GetOperation(id int) *Operation {
	if id != 0 {
		op, ok := registry[id]
		if ok {
			return &op
		}
	}
	return nil
}

func GetProvider1C(endpoint_id string) string {

	return handbook[endpoint_id].Provider1c

}

func GetPaymentType(endpoint_id string) (string, int) {

	row := handbook[endpoint_id]
	return row.Payment_type, row.Payment_type_id

}
