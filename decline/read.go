package decline

import (
	"app/file"
	"app/logs"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
)

func ReadFiles(db *sqlx.DB, filenames []string) (map[int]*Operation, []*file.FileInfo) {

	decline_operations := make(map[int]*Operation, 1000)
	files := make([]*file.FileInfo, 0)

	var count_skipped int64

	start_time := time.Now()

	for _, filename := range filenames {

		var ops []*Operation
		var file *file.FileInfo
		var err error
		var skipped bool
		if filepath.Ext(filename) == ".json" {
			ops, file, err, skipped = readJSON(db, filename)
		} else if filepath.Ext(filename) == ".csv" {
			ops, file, err, skipped = readCSV(db, filename)
		}

		if err != nil {
			logs.Add(logs.ERROR, err)
			continue
		}

		if skipped {
			atomic.AddInt64(&count_skipped, 1)
			continue
		}

		if file != nil {
			files = append(files, file)
		}

		for _, op := range ops {
			decline_operations[op.Operation_id] = op
		}

	}

	logs.Add(logs.INFO, fmt.Sprint("Пропущено файлов: ", count_skipped))
	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций decline: %v [%d строк]", time.Since(start_time), len(decline_operations)))

	return decline_operations, files

}
