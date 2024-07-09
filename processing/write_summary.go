package processing

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/charmap"
)

func Write_Summary(s []SummaryRowMerchant) {

	if !config.Get().Summary.Usage {
		return
	}

	if config.Get().Summary.Storage == config.PSQL {
		PSQL_Insert_SummaryMerchant(s)
	} else {
		Write_CSV_SummaryMerchant(s)
	}

}

func Write_CSV_SummaryMerchant(s []SummaryRowMerchant) {

	if config.Get().Summary.Filename == "" {
		return
	}

	start_time := time.Now()

	file, err := os.Create(config.Get().Summary.Filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить итоговые данные: ошибка совместного доступа к файлу")
		return
	}
	defer file.Close()

	encoder := charmap.Windows1251.NewEncoder()
	writer1251 := encoder.Writer(file)
	writer := csv.NewWriter(writer1251)
	writer.Comma = ';'
	defer writer.Flush()

	headers := []string{
		"document_date", "merchant_id", "merchant_account_id", "provider_id", //"provider_payment_id",
		"balance_id", "operation_group", "operation_type", "country", "region",
		"date start", "formula", "payment_type", "convertation", //network
		"count_operations",
		"channel_currency", "channel_amount", "SR_channel_currency",
		"balance_currency", "balance_amount", "SR_balance_currency",
	}
	writer.Write(headers)

	for _, v := range s {
		row := []string{
			v.Document_date.Format(time.DateOnly),
			fmt.Sprint(v.Merchant_id),
			fmt.Sprint(v.Merchant_account_id),
			fmt.Sprint(v.Provider_id),
			fmt.Sprint(v.Balance_id),
			v.Operation_group,
			v.Operation_type,
			v.Country,
			v.Region,
			v.Tariff_date_start.Format(time.DateOnly),
			v.Formula,
			//v.Crypto_network,
			v.Payment_type,
			v.Convertation,
			fmt.Sprint(v.Count_operations),
			v.Channel_currency_str,
			strings.ReplaceAll(fmt.Sprintf("%.2f", v.Channel_amount), ".", ","),
			strings.ReplaceAll(fmt.Sprintf("%.2f", v.SR_channel_currency), ".", ","),
			v.Balance_currency_str,
			strings.ReplaceAll(fmt.Sprintf("%.2f", v.Balance_amount), ".", ","),
			strings.ReplaceAll(fmt.Sprintf("%.2f", v.SR_balance_currency), ".", ","),
		}
		writer.Write(row)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение итоговых данных в файл: %v", time.Since(start_time)))

}

func PSQL_Insert_SummaryMerchant(s []SummaryRowMerchant) {

	if storage.Postgres == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []SummaryRowMerchant, 100)

	const batch_len = 400

	var wg sync.WaitGroup

	stat_delete :=
		`DELETE FROM summary_merchant 
		WHERE document_date = $1 AND merchant_id = $2` // AND convertation = $3

	stat_insert := querrys.Stat_Insert_summary_merchant()
	_, err := storage.Postgres.PrepareNamed(stat_insert)
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

				for _, row := range v {
					tx.Exec(stat_delete, row.Document_date, row.Merchant_id) //, row.Convertation)
				}

				_, err := storage.Postgres.NamedExec(stat_insert, v)
				if err != nil {
					logs.Add(logs.ERROR, err)
					tx.Rollback()
					return
				} else {
					tx.Commit()
				}

			}
		}()
	}

	sort.Slice(s, func(i int, j int) bool {
		return s[i].Document_date.Before(s[j].Document_date)
	})

	var current_date time.Time
	if len(s) > 0 {
		current_date = s[0].Document_date
	}

	batch := make([]SummaryRowMerchant, 0, batch_len)
	for _, v := range s {
		if current_date != v.Document_date {
			channel <- batch
			batch = make([]SummaryRowMerchant, 0, batch_len)
			current_date = v.Document_date
		}
		batch = append(batch, v)
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Загрузка итоговых данных в Postgres: %v", time.Since(start_time)))

}
