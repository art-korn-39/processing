package processing_provider

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"golang.org/x/text/encoding/charmap"
)

func Write_Detailed() {

	if !config.Get().Detailed.Usage {
		return
	}

	if config.Get().Detailed.Storage == config.PSQL {
		PSQL_Insert_Detailed()
	} else {
		Write_CSV_Detailed()

	}

}

func Write_CSV_Detailed() {

	var wg sync.WaitGroup

	start_time := time.Now()

	file, err := os.Create(config.Get().Detailed.Filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить детализированные данные: ошибка совместного доступа к файлу")
		return
	}
	defer file.Close()

	encoder := charmap.Windows1251.NewEncoder()
	writer1251 := encoder.Writer(file)
	writer := csv.NewWriter(writer1251)
	writer.Comma = ';'
	defer writer.Flush()

	SetHeaders_detailed(writer)

	channel_rows := make(chan []string, 1000)
	channel_indexes := make(chan int, 1000)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for i := range channel_indexes {
				o := storage.Registry[i]
				detailed_row := NewDetailedRow(o)
				row := MakeDetailedRow(detailed_row)
				channel_rows <- row
			}
		}()
	} // 15% each of all time

	go func() {
		wg.Wait()
		close(channel_rows)
	}()

	go func() {
		for i := range storage.Registry {
			channel_indexes <- i
		}
		close(channel_indexes)
	}()

	for row := range channel_rows {
		writer.Write(row) // 90% of all time
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение детализированных данных в файл: %v", time.Since(start_time)))

}

func SetHeaders_detailed(writer *csv.Writer) {
	headers := []string{
		"merchant_name", "merchant_id", "project_name", "project_id", "project_url",
		"operation_id", "merchant_account_id", "payment_method_type", "region", "issuer_country",
		"operation_type", "merchant_account_name",
		"transaction_completed_at", "provider_name",
		"real_amount / channel_amount", "real_currency / channel_currency",
		"endpoint_id", "account_bank_name", "business_type",
		"payment_method_group", "payment_method_name",
		"Сумма в валюте баланса", "Валюта баланса", "BR_balance_currency", "Компенсация BR",
		"Проверка", "Старт тарифа",
		"Акт. тариф", "Акт. фикс", "Акт. Мин", "Акт. Макс",
		"Range min", "Range max",
	}
	writer.Write(headers)
}

func MakeDetailedRow(d Detailed_row) (row []string) {

	row = []string{
		d.Merchant_name,
		fmt.Sprint(d.Merchant_id),
		d.Project_name,
		fmt.Sprint(d.Project_id),
		d.Project_url,
		fmt.Sprint(d.Operation_id),
		fmt.Sprint(d.Merchant_account_id),
		d.Payment_type,
		d.Region,
		d.Country,
		d.Operation_type,
		d.Merchant_account_name,
		d.Transaction_completed_at.Format(time.DateTime),
		d.Provider_name,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Channel_amount), ".", ","),
		d.Channel_currency_str,
		d.Endpoint_id,
		d.Account_bank_name,
		d.Business_type,
		"",
		d.Payment_method,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Provider_amount), ".", ","),
		d.Provider_currency_str,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.BR_balance_currency), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.CompensationBR), ".", ","),
		d.Verification,
		d.Tariff_date_start.Format(time.DateOnly),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_percent*100), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_fix), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_min), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_max), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Range_min), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Range_max), ".", ","),
	}

	return
}

func PSQL_Insert_Detailed() {

	if storage.Postgres == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Detailed_row, 500)

	const batch_len = 1000

	var wg sync.WaitGroup
	var once sync.Once

	stat := querrys.Stat_Insert_detailed()
	_, err := storage.Postgres.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for v := range channel {

				tx, _ := storage.Postgres.Beginx()

				sliceID := make([]int, 0, len(v))
				for _, row := range v {
					sliceID = append(sliceID, row.Operation_id)
				}

				_, err = tx.Exec("delete from detailed where operation_id = ANY($1);", pq.Array(sliceID))
				if err != nil {
					once.Do(func() { logs.Add(logs.INFO, err) })
					tx.Rollback()
					return
				}

				_, err := tx.NamedExec(stat, v)
				if err != nil {
					once.Do(func() { logs.Add(logs.INFO, err) })
					tx.Rollback()
					return
				} else if logs.Testing {
					tx.Rollback()
				} else {
					tx.Commit()
				}

			}
		}()
	}

	batch := make([]Detailed_row, 0, batch_len)
	for i, v := range storage.Registry {
		d := NewDetailedRow(v)
		batch = append(batch, d)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Detailed_row, 0, batch_len)
		}
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Загрузка детализированных данных в Postgres: %v", time.Since(start_time)))

}
