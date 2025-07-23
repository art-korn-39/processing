package convert

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type Setting struct {
	Guid           string
	Name           string
	File_format    string
	Sheet_name     string
	Comma          string
	values         map[string]Mapping
	bof_usage      bool
	external_usage bool
	Key_column     string
	Daily_rates    bool
}

type Mapping struct {
	Registry_column    string
	Table_column       string
	Calculated         bool
	From_bof           bool
	External_source    bool
	Skip               bool
	Date_format        string
	Calculation_format string
}

func (s *Setting) getCalculatedFields() []string {

	result := []string{}

	for _, v := range s.values {
		if v.Calculated {
			result = append(result, v.Registry_column)
		}
	}

	return result

}

func readSettings(db *sqlx.DB, provider_guid []string) {

	if db == nil {
		return
	}

	type Row struct {
		Guid               string `db:"guid"`
		Name               string `db:"name"`
		Key_column         string `db:"key_column"`
		File_format        string `db:"file_format"`
		Sheet_name         string `db:"sheet_name"`
		Comma              string `db:"comma"`
		Daily_rates        bool   `db:"daily_rates"`
		Registry_column    string `db:"registry_column"`
		Table_column       string `db:"table_column"`
		Date_format        string `db:"date_format"`
		Calculation_format string `db:"calculation_format"`
		Calculated         bool   `db:"calculated"`
		From_bof           bool   `db:"from_bof"`
		Skip               bool   `db:"skip"`
		External_source    bool   `db:"external_source"`
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_conversion()

	rows := []Row{}

	err := db.Select(&rows, stat, pq.Array(provider_guid))
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, row := range rows {

		mapping := Mapping{
			Registry_column: row.Registry_column, Table_column: row.Table_column, External_source: row.External_source,
			Date_format: row.Date_format, Calculated: row.Calculated, From_bof: row.From_bof, Skip: row.Skip,
			Calculation_format: row.Calculation_format,
		}

		var setting *Setting
		var ok bool

		setting, ok = all_settings[row.Guid]
		if !ok {
			setting = &Setting{Name: row.Name, Guid: row.Guid, File_format: row.File_format, Key_column: row.Key_column,
				Comma: row.Comma, Sheet_name: row.Sheet_name, Daily_rates: row.Daily_rates, values: map[string]Mapping{}}
		}
		setting.values[row.Registry_column] = mapping

		setting.bof_usage = setting.bof_usage || mapping.From_bof
		setting.external_usage = setting.external_usage || mapping.External_source

		all_settings[row.Guid] = setting
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение настроек: %v [найдено: %s]", time.Since(start_time), util.FormatInt(len(all_settings))))

}

// проверка, что каждому полю настройки соответствует колонка в таблице файла
// не сработает, если в настройке не хватает поля, которое будет использоваться дальше в методе getValue()
func checkFields(setting *Setting, map_fileds map[string]int) error {

	for _, val := range setting.values {

		if val.Calculated || val.Skip || val.From_bof || val.External_source {
			continue
		}

		_, ok := map_fileds[val.Table_column]
		if !ok {
			return fmt.Errorf("в маппинге \"%s\" неверно указано поле стыковки для колонки %s", setting.Name, val.Registry_column)
		}

	}

	return nil
}

func checkUsedSettings() (key_column string, external_usage bool, err error) {

	if use_daily_rates {

		// смотрим все настройки и ищем которую может подойти
		for _, s := range all_settings {

			if s.Daily_rates {
				main_setting = s
				key_column = s.Key_column
				external_usage = true
				break
			}
		}

		if main_setting == nil {
			err = fmt.Errorf("не найдена подходящая настрока для режима 'Курс на день'")
			return
		}

	} else {

		for _, v := range used_settings {

			if key_column != "" && v.Key_column != key_column {
				err = fmt.Errorf("обнаружены настройки с разными значениями key_column")
				return
			}

			key_column = v.Key_column

			external_usage = v.external_usage || external_usage

		}
	}
	return

}
