package processing_provider

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
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
		err := writer.Write(row) // 90% of all time
		if err != nil {
			logs.Add(logs.ERROR, err)
		}
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение детализированных данных в файл: %v", time.Since(start_time)))

}

func SetHeaders_detailed(writer *csv.Writer) {
	headers := []string{
		"operation_id", "provider_payment_id", "transaction_id", "RRN", "payment_id",
		"coupled_operation_id", "parent_payout_operation_id",
		"provider_name", "merchant_account_name", "merchant_name", "project_id", "operation_type",
		"payment_method_type", "issuer_country", "transaction_created_at", "transaction_completed_at",
		"real_amount / channel_amount", "real_currency / channel_currency",
		"provider_amount", "provider_currency", "operation_actual_amount",
		"surcharge amount", "surcharge currency", "endpoint_id", "account_bank_name", "operation_created_at",
		"Сумма в валюте баланса", "BR в валюте баланса", "Доп BR", "Валюта баланса", "Курс",
		"Компенсация BR", "Проверка", "Старт тарифа",
		"Акт. тариф", "Акт. фикс", "Акт. Мин", "Акт. Макс",
		"Range min", "Range max",
		"region", "Поставщик Dragonpay",
	}
	writer.Write(headers)
}

func MakeDetailedRow(d Detailed_row) (row []string) {

	row = []string{
		fmt.Sprint(d.Operation_id),
		d.Provider_payment_id,
		fmt.Sprint(d.Transaction_id),
		d.RRN,
		d.Payment_id,
		"", "",
		d.Provider_name,
		d.Merchant_account_name,
		d.Merchant_name,
		fmt.Sprint(d.Project_id),
		d.Operation_type,
		d.Payment_type,
		d.Country,
		d.Transaction_created_at.Format(time.DateTime),
		d.Transaction_completed_at.Format(time.DateTime),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Channel_amount), ".", ","),
		d.Channel_currency_str,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Provider_amount), ".", ","),
		d.Provider_currency_str,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Operation_actual_amount), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Surcharge_amount), ".", ","),
		d.Surcharge_currency_str,
		d.Endpoint_id,
		util.IsString1251(d.Account_bank_name),
		d.Operation_created_at.Format(time.DateTime),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Balance_amount), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.3f", d.BR_balance_currency), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.3f", d.Extra_BR_balance_currency), ".", ","),
		d.Balance_currency_str,
		strings.ReplaceAll(fmt.Sprintf("%.4f", d.Rate), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.CompensationBR), ".", ","),
		d.Verification,
		d.Tariff_date_start.Format(time.DateOnly),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_percent*100), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_fix), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_min), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_max), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Range_min), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Range_max), ".", ","),
		d.Region,
		d.Provider_dragonpay,
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

	stat := querrys.Stat_Insert_detailed_provider()
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

				_, err = tx.Exec("delete from detailed_provider where operation_id = ANY($1);", pq.Array(sliceID))
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
