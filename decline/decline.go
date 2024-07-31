package decline

import (
	"app/config"
	"app/logs"
	"app/storage"
	"app/util"
)

var (
	decline_operations map[int]Operation
)

func Start() {

	db, err := storage.PSQL_connect()
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	} else {
		logs.Add(logs.INFO, "Successful connection to Postgres")
	}
	defer db.Close()

	decline_operations = make(map[int]Operation, 1000)

	folder := config.Get().Decline.Filename
	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		return
	}

	ReadFiles(filenames)
	InsertIntoDB(db)

}
