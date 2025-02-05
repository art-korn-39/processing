package conversion_raw

import (
	"app/logs"
	"app/util"
	"fmt"
	"path/filepath"
	"time"
)

func readFolder(filename string) (map_fileds map[string]int, setting Setting, err error) {

	if filename == "" {
		return
	}

	start_time := time.Now()

	filenames, err := util.ParseFoldersRecursively(filename)
	for _, filename := range filenames {

		var err error
		if filepath.Ext(filename) == ".csv" {
			err = readCSV(filename)
		} else if filepath.Ext(filename) == ".xlsx" {
			err = readXLSX(filename)
		} else {
			continue
		}

		if err != nil {
			logs.Add(logs.MAIN, err)
		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций провайдера: %v [%s строк]", time.Since(start_time), util.FormatInt(len(ext_registry))))

	return

}
