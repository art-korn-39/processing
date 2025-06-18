package convert

import (
	"app/logs"
	"app/validation"
	"slices"
	"strings"
	"sync"

	"github.com/tealeg/xlsx"
)

func readXLSX(filename string) (baseError error) {

	var mu sync.Mutex

	xlFile, err := xlsx.OpenFile(filename)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	var last_iteration bool
	for _, setting := range all_settings {

		if setting.File_format != "XLSX" {
			continue
		}

		var key_name string
		for _, v := range setting.values {
			if v.Registry_column == setting.Key_column {
				key_name = v.Table_column
				break
			}
		}

		for _, sheet := range xlFile.Sheets {
			if strings.ToLower(sheet.Name) == setting.Sheet_name {

				// определение строки с названиями колонок
				headerLine := -1
			loop:
				for _, row := range sheet.Rows {
					if len(row.Cells) < 2 {
						continue
					}
					for col := range row.Cells {
						if strings.ToLower(row.Cells[col].Value) == key_name {
							headerLine = slices.Index(sheet.Rows, row)
							break loop
						}
					}
				}

				if headerLine == -1 {
					continue
				}

				map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[headerLine].Cells)
				err = checkFields(setting, map_fileds)
				if err != nil {
					logs.Add(logs.INFO, err)
					continue
				}

				used_settings[setting.Guid] = setting
				last_iteration = true

				for _, row := range sheet.Rows {

					if slices.Index(sheet.Rows, row) <= headerLine {
						continue
					}

					if len(row.Cells) == 0 || row.Cells[1].String() == "" || len(row.Cells) < len(map_fileds) {
						break
					}

					record := getRecordFromCells(row.Cells)
					op, err := createBaseOperation(record, map_fileds, setting)
					if err != nil {
						baseError = err
						//cancel()
						return
					}
					//op.StartingFill(true)
					mu.Lock()
					ext_registry = append(ext_registry, op)
					mu.Unlock()
				}

			}
		}
		if last_iteration {
			break
		}
	}

	return nil
}

func getRecordFromCells(cells []*xlsx.Cell) []string {

	r := make([]string, 0, len(cells))
	for _, v := range cells {
		r = append(r, v.Value)
	}
	return r

}
