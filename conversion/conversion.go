package conversion

import (
	"app/config"
	"app/file"
	"app/logs"
	"app/storage"
	"app/util"
	"fmt"
	"sort"

	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB

func Start() {

	var err error
	db, err = storage.PSQL_connect()
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	} else {
		logs.Add(logs.INFO, "Successful connection to Postgres")
	}
	defer db.Close()

	folder := config.Get().Rates.Filename

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		logs.Add(logs.FATAL, "ParseFoldersRecursively(): ", err)
		return
	}

	files := file.GetFiles(filenames)

	logs.Add(logs.INFO, fmt.Sprint("Обнаружено файлов: ", len(files)))
	logs.Add(logs.INFO, "Выполняется чтение...")

	sort.Slice(files, func(i, j int) bool {
		return files[i].Size < files[j].Size
	})

	chan_operations, chan_readed_files := ReadFiles(files)

	WriteIntoDB(chan_operations, chan_readed_files)

}
