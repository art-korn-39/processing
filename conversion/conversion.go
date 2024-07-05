package conversion

import (
	"app/config"
	"app/logs"
	"app/processing"
	"app/util"
	"sort"

	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB

func Start() {

	var err error
	db, err = processing.PSQL_connect()
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	folder := config.Get().Rates.Filename

	filenames, err := util.ParseFoldersRecursively(folder)
	if err != nil {
		return
	}

	files := GetFiles(filenames)

	sort.Slice(files, func(i, j int) bool {
		return files[i].Size < files[j].Size
	})

	ch_operations, ch_readed_files := ReadFiles(files)

	WriteIntoDB(ch_operations, ch_readed_files)

}
