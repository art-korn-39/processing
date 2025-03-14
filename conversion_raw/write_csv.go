package conversion_raw

import (
	"app/config"
	"app/logs"
	pr "app/provider_registry"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/charmap"
)

func writeIntoCSV(filename string) {

	var wg sync.WaitGroup

	start_time := time.Now()

	file, err := os.Create(filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось выгрузить результат: ошибка совместного доступа к файлу")
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
				op := final_registry[i]
				row := makeDetailedRow(op)
				channel_rows <- row
			}
		}()
	}

	go func() {
		wg.Wait()
		close(channel_rows)
	}()

	go func() {
		for i := range final_registry {
			channel_indexes <- i
		}
		close(channel_indexes)
	}()

	for row := range channel_rows {
		writer.Write(row)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение данных в CSV файл: %v", time.Since(start_time)))

}

func SetHeaders_detailed(writer *csv.Writer) {
	headers := []string{
		"operation_id", "provider_payment_id", "provider_name", "merchant_account_name",
		"merchant_name", "project_id", "operation_type",
		"account_number", "channel_amount", "channel_currency", "issuer_country",
		"payment_method_type", "transaction_completed_at", "transaction_created_at", "provider_currency",
		"курс", "provider_amount", "BR", "balance", "provider1c", "team", "operation_status",
	}

	writer.Write(headers)
}

func makeDetailedRow(op *pr.Operation) []string {

	result := []string{
		strconv.Itoa(op.Id),
		op.Provider_payment_id,
		op.Provider_name,
		op.Merchant_account_name,
		op.Merchant_name,
		fmt.Sprint(op.Project_id),
		op.Operation_type,
		op.Account_number,
		strings.ReplaceAll(fmt.Sprintf("%.2f", op.Channel_amount), ".", ","),
		op.Channel_currency.Name,
		op.Country,
		op.Payment_type,
		op.Transaction_completed_at.Format(time.DateTime),
		op.Transaction_created_at.Format(time.DateTime),
		op.Provider_currency.Name,
		strings.ReplaceAll(fmt.Sprintf("%.2f", op.Rate), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", op.Amount), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", op.BR_amount), ".", ","),
		op.Balance,
		op.Provider1c,
		op.Team,
		op.Operation_status,
	}

	return result

}
