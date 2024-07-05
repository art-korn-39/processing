package decline

import (
	"app/config"
	"app/logs"
	"app/processing"
	"app/util"

	"github.com/jmoiron/sqlx"
)

var (
	db                 *sqlx.DB
	decline_operations map[int]DeclineOperation
)

func Start() {

	var err error
	db, err = processing.PSQL_connect()
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}
	logs.Add(logs.INFO, "Successful connection to Postgres")

	decline_operations = make(map[int]DeclineOperation, 1000)

	folder := config.Get().Decline.Filename
	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		return
	}

	ReadFiles(filenames)
	InsertIntoDB()

}
