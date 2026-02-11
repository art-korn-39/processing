package processing_provider

import (
	"app/config"
	"app/currency"
	"app/logs"
	"app/util"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/charmap"
)

type Detailed_row_tradex struct {
	Operation_id        int    `db:"operation_id"`
	Provider_payment_id string `db:"provider_payment_id"`
	Transaction_id      int    `db:"transaction_id"`
	RRN                 string `db:"rrn"`
	Payment_id          string `db:"payment_id"`
	Provider_name       string `db:"provider_name"`
	// Provider_id               int       `db:"provider_id"`
	// Merchant_id               int       `db:"merchant_id"`
	Merchant_account_id      int       `db:"merchant_account_id"`
	Merchant_account_name    string    `db:"merchant_account_name"`
	Merchant_name            string    `db:"merchant_name"`
	Project_id               int       `db:"project_id"`
	Operation_type           string    `db:"operation_type"`
	Payment_type             string    `db:"payment_type"`
	Country                  string    `db:"country"`
	Transaction_created_at   time.Time `db:"transaction_created_at"`
	Transaction_completed_at time.Time `db:"transaction_completed_at"`
	Channel_amount           float64   `db:"channel_amount"`
	Channel_currency_str     string    `db:"channel_currency"`
	Provider_amount          float64   `db:"provider_amount"`
	Provider_currency_str    string    `db:"provider_currency"`
	Operation_actual_amount  float64   `db:"operation_actual_amount"`
	Surcharge_amount         float64   `db:"surcharge_amount"`
	Surcharge_currency_str   string    `db:"surcharge_currency"`
	Endpoint_id              string    `db:"endpoint_id"`
	Account_bank_name        string    `db:"account_bank_name"`
	Operation_created_at     time.Time `db:"operation_created_at"`
	Balance_id               int       `db:"balance_id"`
	Balance_amount           float64   `db:"balance_amount"`
	BR_balance_currency      float64   `db:"br_balance_currency"`
	Balance_currency_str     string    `db:"balance_currency"`
	Rate                     float64   `db:"rate"`
	Provider_1c              string    `db:"provider_1c"`
	Balance_currency         currency.Currency
	IsTestType               string `db:"is_test_type"` // is_test

	Tariff        string
	Real_provider string

	Provider_amount_tradex             float64 // Сумма в ЛК
	Bonus_tradex                       string  // из provider_registry, поле bonus_tradex
	BR_provider_registry               float64 // BR в ЛК, из provider_registry, поле br_amount
	Commission_tradex                  float64 // Доп BR, из provider_registry, поле comission_tradex
	Delta_BR_provider_registry         float64 // Дельта BR
	Delta_amount                       float64 // Дельта по сумме операции
	Agency_commission_base_currency    float64
	Agency_commission_balance_currency float64
	Total_BR                           float64 // расчетное поле, Доп BR + BR в валюте баланса
	Team_provider_registry             string
	Balance_provider_registry          string
	Status_provider_registry           string // статус в ПС, из provider_registry, поле operation_status
	Provider1C_provider_registry       string // Поставщик
	User_tradex                        string // из provider_registry, поле bonuses_tradex

}

func NewDetailedRowTradex(o *Operation) (d Detailed_row_tradex) {

	d = Detailed_row_tradex{}

	d.Operation_id = o.Operation_id
	d.Provider_payment_id = o.Provider_payment_id
	d.Transaction_id = o.Transaction_id
	d.Merchant_account_id = o.Merchant_account_id
	d.RRN = o.RRN
	d.Payment_id = o.Payment_id
	d.Provider_name = o.Provider_name
	// d.Provider_id = o.Provider_id
	// d.Merchant_id = o.Merchant_id
	d.Balance_id = o.Balance_id
	d.Merchant_name = o.Merchant_name
	d.Merchant_account_name = o.Merchant_account_name
	d.Project_id = o.Project_id
	d.Operation_type = o.Operation_type
	d.Payment_type = o.Payment_type
	if o.Country_code2 != "" {
		d.Country = o.Country_code2
	} else {
		d.Country = o.Country.Code2
	}
	d.Transaction_created_at = o.Transaction_created_at
	d.Transaction_completed_at = o.Transaction_completed_at
	d.Channel_amount = o.Channel_amount
	d.Channel_currency_str = o.Channel_currency.Name
	d.Provider_amount = o.Provider_amount
	d.Provider_currency_str = o.Provider_currency.Name
	d.Operation_actual_amount = o.Operation_actual_amount
	d.Surcharge_amount = o.Surcharge_amount
	d.Surcharge_currency_str = o.Surcharge_currency.Name
	if o.DragonpayOperation != nil {
		d.Endpoint_id = o.DragonpayOperation.Endpoint_id
	} else {
		d.Endpoint_id = o.Endpoint_id
	}
	d.Account_bank_name = o.Account_bank_name
	d.Operation_created_at = o.Operation_created_at
	d.Balance_amount = o.Balance_amount
	d.BR_balance_currency = o.BR_balance_currency
	if o.Channel_currency == o.Balance_currency {
		d.Rate = 1
	} else if o.ProviderOperation != nil {
		d.Rate = o.ProviderOperation.Rate
	}
	d.Balance_currency_str = o.Balance_currency.Name
	d.Balance_currency = o.Balance_currency
	d.Provider_1c = o.Provider1c
	d.IsTestType = o.IsTestType

	d.Agency_commission_base_currency = o.Extra_BR_balance_currency

	if o.Rate != 0 {
		d.Agency_commission_balance_currency = d.Agency_commission_base_currency / o.Rate
	}
	d.Total_BR = d.BR_balance_currency + d.Agency_commission_balance_currency

	if o.Tariff != nil {
		d.Tariff = strings.ReplaceAll(fmt.Sprintf("%.2f%%", o.Tariff.Percent*100), ".", ",")
	}

	prov_op := o.ProviderOperation
	if prov_op != nil {
		d.Provider_amount_tradex = prov_op.Provider_amount_tradex
		d.Bonus_tradex = prov_op.Bonuses_tradex
		d.BR_provider_registry = prov_op.BR_amount
		d.Commission_tradex = prov_op.Comission_tradex
		d.Delta_BR_provider_registry = d.BR_balance_currency - d.BR_provider_registry
		d.Delta_amount = d.Balance_amount - d.Provider_amount_tradex
		d.Team_provider_registry = prov_op.Team
		d.Balance_provider_registry = prov_op.Balance
		d.Status_provider_registry = prov_op.Operation_status
		d.Provider1C_provider_registry = prov_op.Provider1c
		d.User_tradex = prov_op.User_tradex
	}

	return d
}

func Write_Detailed_tradex() {

	if !config.Get().Detailed_2.Usage {
		return
	}

	if config.Get().Detailed_2.Storage == config.File {
		Write_CSV_Detailed_tradex()
	}

}

func Write_CSV_Detailed_tradex() {

	var wg sync.WaitGroup

	start_time := time.Now()

	file, err := os.Create(config.Get().Detailed_2.Filename)
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

	SetHeaders_detailed_tradex(writer)

	channel_rows := make(chan []string, 1000)
	channel_indexes := make(chan int, 1000)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for i := range channel_indexes {
				o := storage.Registry[i]
				detailed_row := NewDetailedRowTradex(o)
				row := MakeDetailedRowTradex(detailed_row)
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

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение детализированных данных (tradex) в файл: %v", util.FormatDuration(time.Since(start_time))))

}

func SetHeaders_detailed_tradex(writer *csv.Writer) {
	headers := []string{
		"operation_id", "provider_payment_id", "transaction_id", "RRN", "payment_id", "coupled_operation_id",
		"parent_payout_operation_id", "provider_name", "merchant_account_name", "merchant_name",
		"project_id", "operation_type", "payment_method_type", "issuer_country", "transaction_created_at",
		"transaction_completed_at", "real_amount / channel_amount", "real_currency / channel_currency",
		"provider_amount", "provider_currency", "operation_actual_amount", "surcharge amount", "surcharge currency",
		"endpoint_id", "account_bank_name", "operation_created_at", "balance_id", "merchant_account_id", "is_test",
		"real_provider", "Сумма в валюте баланса", "Валюта баланса", "Курс", "Сумма в ЛК", "Дельта по сумме операции",
		"Тариф", "BR в валюте баланса", "Bonus", "BR в ЛК", "Доп BR", "Дельта BR", "Агентская комиссия в инитной валюте",
		"Агентская комиссия в валюте баланса", "Total BR", "Team", "Баланс", "Статус в ПС", "Поставщик", "User",
	}
	writer.Write(headers)
}

func MakeDetailedRowTradex(d Detailed_row_tradex) (row []string) {

	return []string{
		fmt.Sprint(d.Operation_id),               // operation_id
		util.IsString1251(d.Provider_payment_id), // provider_payment_id
		fmt.Sprint(d.Transaction_id),             // transaction_id
		d.RRN,                                    // RRN
		d.Payment_id,                             // payment_id
		"",                                       // coupled_operation_id
		"",                                       // parent_payout_operation_id
		d.Provider_name,                          // provider_name
		d.Merchant_account_name,                  // merchant_account_name
		d.Merchant_name,                          // merchant_name
		fmt.Sprint(d.Project_id),                 // project_id
		d.Operation_type,                         // operation_type
		d.Payment_type,                           // payment_method_type
		d.Country,                                // issuer_country
		d.Transaction_created_at.Format(time.DateTime),                               // transaction_created_at
		d.Transaction_completed_at.Format(time.DateTime),                             // transaction_completed_at
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Channel_amount), ".", ","),          // real_amount / channel_amount
		d.Channel_currency_str,                                                       // real_currency / channel_currency
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Provider_amount), ".", ","),         // provider_amount
		d.Provider_currency_str,                                                      // provider_currency
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Operation_actual_amount), ".", ","), // operation_actual_amount
		strings.ReplaceAll(fmt.Sprintf("%.2f", d.Surcharge_amount), ".", ","),        // surcharge amount
		d.Surcharge_currency_str,                                                     // surcharge currency
		d.Endpoint_id,                                                                // endpoint_id
		util.IsString1251(d.Account_bank_name),                                       // account_bank_name
		d.Operation_created_at.Format(time.DateTime),                                 // operation_created_at
		fmt.Sprint(d.Balance_id),                                                     // balance_id
		fmt.Sprint(d.Merchant_account_id),                                            // merchant_account_id
		d.IsTestType,                                                                 // is_test
		d.Real_provider,                                                              // real_provider
		util.FloatToString(d.Balance_amount, 8),                                      // Сумма в валюте баланса
		d.Balance_currency_str,                                                       // Валюта баланса
		util.FloatToString(d.Rate, d.Balance_currency.GetAccuracy(2)),                // Курс
		util.FloatToString(d.Provider_amount_tradex, 2),                              // Сумма в ЛК
		util.FloatToString(d.Delta_amount, 2),                                        // Дельта по сумме операции
		d.Tariff,                                                                     // Тариф
		util.FloatToString(d.BR_balance_currency, 8),                                 // BR в валюте баланса
		d.Bonus_tradex,                                                               // Bonus
		util.FloatToString(d.BR_provider_registry, 2),                                // BR в ЛК
		util.FloatToString(d.Commission_tradex, 8),                                   // Доп BR
		util.FloatToString(d.Delta_BR_provider_registry, 2),                          // Дельта BR
		util.FloatToString(d.Agency_commission_base_currency, 2),                     // Агентская комиссия в инитной валюте
		util.FloatToString(d.Agency_commission_balance_currency, 2),                  // Агентская комиссия в валюте баланса
		util.FloatToString(d.Total_BR, 8),                                            // Total BR
		d.Team_provider_registry,                                                     // Team
		d.Balance_provider_registry,                                                  // Баланс
		d.Status_provider_registry,                                                   // Статус в ПС
		d.Provider1C_provider_registry,                                               // Поставщик
		util.IsString1251(d.User_tradex),                                             // User
	}
}
