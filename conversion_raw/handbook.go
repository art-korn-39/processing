package conversion_raw

import (
	"app/validation"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/tealeg/xlsx"
)

func readHandbook(filename string) error {

	xlFile, err := xlsx.OpenFile(filename)
	if err != nil {
		return err
	}

	for _, sheet := range xlFile.Sheets {

		sheet_name := strings.ToLower(sheet.Name)
		if sheet_name == "команды" {

			err = readCommandSheet(sheet, filepath.Base(filename))
			if err != nil {
				return err
			}
		}
	}

	return nil

}

func readCommandSheet(sheet *xlsx.Sheet, file_name string) error {

	if len(sheet.Rows) < 2 || sheet.Rows[0].Cells[0].Value != "Team ID" {
		return fmt.Errorf("некорректный формат файла %s", file_name)
	}

	map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[0].Cells)
	err := validation.CheckMapOfColumnNames(map_fileds, "kgx__teams_xlsx")
	if err != nil {
		return err
	}

	for i, row := range sheet.Rows {

		if i == 0 {
			continue
		}

		if len(row.Cells) == 0 || row.Cells[0].String() == "" {
			break
		}

		team_id := row.Cells[map_fileds["team id"]-1].String()
		balance := row.Cells[map_fileds["баланс"]-1].String()

		handbook[team_id] = balance

	}

	return nil

}

func getBalanceByTeamID(record []string, map_fields map[string]int) string {

	idx := map_fields["partnerid"]
	if idx > 0 {
		partner_id := record[idx-1]
		return handbook[partner_id]
	}

	return ""

}
