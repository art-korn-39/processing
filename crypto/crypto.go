package crypto

import (
	"app/config"
	"app/logs"
	"app/processing"
	"app/util"

	"github.com/jmoiron/sqlx"
)

var (
	db                *sqlx.DB
	crypto_operations map[int]processing.CryptoOperation
)

func Start() {

	var err error
	db, err = processing.PSQL_connect()
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	} else {
		logs.Add(logs.INFO, "Successful connection to Postgres")
	}
	defer db.Close()

	crypto_operations = make(map[int]processing.CryptoOperation, 1000)

	folder := config.Get().Crypto.Filename
	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		return
	}

	ReadFiles(filenames)
	InsertIntoDB()

}
