package conversion_raw

import (
	"app/validation"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/tealeg/xlsx"
)

type provider_params struct {
	country,
	payment_type,
	currency,
	balance,
	provider1c string
}

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
		} else if sheet_name == "поставщики" {

			err = readProviderSheet(sheet, filepath.Base(filename))
			if err != nil {
				return err
			}
		}
	}

	return nil

}

type team_line struct {
	team_id, team, balance string
}

func readCommandSheet(sheet *xlsx.Sheet, file_name string) error {

	if len(sheet.Rows) < 2 || sheet.Rows[0].Cells[0].Value != "team_id" {
		return fmt.Errorf("некорректный формат файла %s", file_name)
	}

	map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[0].Cells)
	err := validation.CheckMapOfColumnNames(map_fileds, "kgx_teams_xlsx")
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

		v := team_line{
			team_id: strings.ToLower(row.Cells[map_fileds["team_id"]-1].String()),
			team:    row.Cells[map_fileds["team"]-1].String(),
			balance: row.Cells[map_fileds["balance"]-1].String(),
		}

		teams[v.team_id] = v

	}

	return nil

}

func readProviderSheet(sheet *xlsx.Sheet, file_name string) error {

	if len(sheet.Rows) < 2 || sheet.Rows[0].Cells[0].Value != "issuer_country" {
		return fmt.Errorf("некорректный формат файла %s", file_name)
	}

	map_fileds := validation.GetMapOfColumnNamesCells(sheet.Rows[0].Cells)
	err := validation.CheckMapOfColumnNames(map_fileds, "kgx_providers_xlsx")
	if err != nil {
		return err
	}

	for i, row := range sheet.Rows {

		if i == 0 {
			continue
		}

		if len(row.Cells) < 5 || row.Cells[4].String() == "" {
			break
		}

		params := provider_params{
			country:      row.Cells[map_fileds["issuer_country"]-1].String(),
			payment_type: row.Cells[map_fileds["payment_type_id / payment_method_type"]-1].String(),
			currency:     row.Cells[map_fileds["валюта в пс"]-1].String(),
			balance:      row.Cells[map_fileds["баланс"]-1].String(),
			provider1c:   row.Cells[map_fileds["provider1c"]-1].String(),
		}

		providers = append(providers, params)

	}

	return nil

}

func getTeamByTeamID(record []string, map_fields map[string]int) string {

	idx := map_fields["partnerid"]
	if idx > 0 {
		partner_id := strings.ToLower(record[idx-1])
		return teams[partner_id].team
	}

	return ""

}

func getBalanceByTeamID(record []string, map_fields map[string]int) string {

	idx := map_fields["partnerid"]
	if idx > 0 {
		partner_id := strings.ToLower(record[idx-1])
		return teams[partner_id].balance
	}

	return ""

}
