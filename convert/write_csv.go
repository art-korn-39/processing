package convert

import (
	"app/config"
	"app/logs"
	"app/provider_registry"
	pr "app/provider_registry"
	"app/util"
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
	channel_indexes_bof := make(chan string, 1000)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		if i%2 == 0 {
			go func() {
				defer wg.Done()
				for i := range channel_indexes {
					op := final_registry[i]
					row := makeDetailedRow(op)
					channel_rows <- row
				}
			}()
		} else {
			go func() {
				defer wg.Done()
				for i := range channel_indexes_bof {
					op := bof_registry[i]
					row := makeDetailedRowBof(op)
					if row != nil {
						channel_rows <- row
					}
				}
			}()
		}
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

	go func() {
		for i := range bof_registry {
			channel_indexes_bof <- i
		}
		close(channel_indexes_bof)
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
		"курс", "provider_amount", "BR", "balance", "provider1c", "team", "operation_status", "Проверка",
	}

	writer.Write(headers)
}

func makeDetailedRow(op *pr.Operation) []string {

	//var verification string

	operation_id := strconv.Itoa(op.Id)
	//payment_id := op.Provider_payment_id

	// _, ok1 := bof_registry[operation_id]
	// _, ok2 := bof_registry[payment_id]

	// if (!use_daily_rates && (ok1 || ok2)) || (use_daily_rates && op.Rate != 0) {
	// 	verification = "ОК"
	// } else {
	// 	verification = "Не найдено"
	// }

	result := []string{
		operation_id,
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
		strings.ReplaceAll(fmt.Sprintf("%.8f", op.Rate), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", op.Amount), ".", ","),
		strings.ReplaceAll(fmt.Sprintf("%.2f", op.BR_amount), ".", ","),
		op.Balance,
		op.Provider1c,
		op.Team,
		op.Operation_status,
		op.Verification,
	}

	return result

}

func makeDetailedRowBof(op *Bof_operation) []string {

	// если операция нашлась в final_reg то сюда не пишем, т.к. её запишет другая горутина
	id, _ := strconv.Atoi(op.Operation_id)
	_, ok := final_registry[id]
	if ok {
		return nil
	}

	_, opExist := provider_registry.GetOperation(id, op.Transaction_completed_at.Truncate(24*time.Hour), op.Channel_amount)

	result := []string{
		op.Operation_id,
		op.Provider_payment_id,
		op.Provider_name,
		op.Merchant_account_name,
		op.Merchant_name,
		fmt.Sprint(op.Project_id),
		op.Operation_type,
		"", //op.Account_number,
		strings.ReplaceAll(fmt.Sprintf("%.2f", op.Channel_amount), ".", ","),
		op.Channel_currency.Name,
		op.Country_code2,
		op.Payment_type,
		op.Transaction_completed_at.Format(time.DateTime),
		op.Transaction_created_at.Format(time.DateTime),
		"", //op.Provider_currency.Name,
		"", //strings.ReplaceAll(fmt.Sprintf("%.2f", op.Rate), ".", ","),
		"", //strings.ReplaceAll(fmt.Sprintf("%.2f", op.Amount), ".", ","),
		"", //strings.ReplaceAll(fmt.Sprintf("%.2f", op.BR_amount), ".", ","),
		"", //op.Balance,
		"", //op.Provider1c,
		"", //op.Team,
		"", //op.Operation_status,
		util.TR(opExist, "Есть данные по конвертации", "Не найдено").(string),
	}

	return result

}
