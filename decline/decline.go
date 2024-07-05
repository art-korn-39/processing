package decline

import (
	"app/config"
	"app/logs"
	"app/processing"
	"app/util"
	"fmt"

	"github.com/jmoiron/sqlx"
)

const (
	Version = "1.0.0"
)

var (
	db                 *sqlx.DB
	decline_operations map[int]DeclineOperation
)

func Start() {

	logs.Add(logs.INFO, fmt.Sprintf("Загружен файл конфигурации (ver %s)", Version))

	var err error
	db, err = processing.PSQL_connect()
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	decline_operations = make(map[int]DeclineOperation, 1000)

	folder := config.Get().Decline.Filename
	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		return
	}

	ReadFiles(filenames)
	InsertIntoDB()

}
