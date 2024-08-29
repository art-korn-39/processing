package decline

import (
	"app/config"
	"app/logs"
	"app/storage"
	"app/util"
)

func Start() {

	cfg := config.Get()

	db, err := storage.PSQL_connect(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	} else {
		logs.Add(logs.INFO, "Successful connection to Postgres")
	}
	defer db.Close()

	folder := cfg.Decline.Filename
	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		return
	}

	decline_operations, files := ReadFiles(db, filenames)
	InsertIntoDB(db, decline_operations, files)

}
