package processing

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
		"merchant_id", "merchant_name",
		"operation_id", "merchant_account_id",
		"payment_method", "operation_type", "merchant_account_name", "issuer_country",
		"Поставщик", "Подраззделение", "Расчетный счет",
		"transaction_completed_at",
		"provider_name", "provider_amount", "provider_currency",
		"real_amount / channel_amount", "real_currency / channel_currency",
		"fee_amount", "fee_currency",
		"Сумма в валюте баланса", "Валюта баланса", "КурсПоРеестру", "Сумма Реестра Провайдера",
		"SR Real Currency", "SR_balance_currency", "checkFee", "Проверка",
		"Crypto_network", "balance_id", "tariff_condition_id", "contract_id",
		"Старт Тарифа", "Конвертация", "Акт. тариф", "Акт. фикс", "Акт. Мин", "Акт. Макс", "Range min", "Range max",
		"tariff_rate_percent", "tariff_rate_fix", "tariff_rate_min", "tariff_rate_max",
	}
	writer.Write(headers)
}

func MakeDetailedRow(d Detailed_row) (row []string) {

	row = []string{
		fmt.Sprint(d.Merchant_id),
		d.Merchant_name,
		fmt.Sprint(d.Operation_id),
		fmt.Sprint(d.Merchant_account_id),
		d.Payment_type,
		d.Operation_type,
		d.Merchant_account_name,
		d.Country,
		d.Provider1C,
		d.Subdivision1C,
		d.RatedAccount,
		d.Transaction_completed_at.Format(time.DateTime),
		d.Provider_name,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Provider_amount), ".", ","),
		d.Provider_currency_str,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Channel_amount), ".", ","),
		d.Channel_currency_str,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Fee_amount), ".", ","),
		d.Fee_currency_str,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Balance_amount), ".", ","),
		d.Balance_currency_str,
		strings.ReplaceAll(fmt.Sprint(d.Rate), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Provider_registry_amount), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.SR_channel_currency), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.SR_balance_currency), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.CheckFee), ".", ","),
		d.Verification,
		fmt.Sprint(d.Crypto_network),
		fmt.Sprint(d.Balance_id),
		fmt.Sprint(d.Tariff_condition_id),
		fmt.Sprint(d.Contract_id),
		d.Tariff_date_start.Format(time.DateOnly),
		d.Convertation,
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_percent*100), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_fix), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_min), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Act_max), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Range_min), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Range_max), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Tariff_rate_percent*100), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Tariff_rate_fix), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Tariff_rate_min), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Tariff_rate_max), ".", ","),
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

				tx.Exec("delete from detailed where id = ANY($1);", pq.Array(sliceID))

				_, err := tx.NamedExec(stat, v)
				if err != nil {
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
