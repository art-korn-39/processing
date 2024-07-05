package processing

import (
	"app/config"
	"app/logs"
	"app/util"
	"fmt"
	"strings"
	"time"

	_ "github.com/alexbrainman/odbc"
	"github.com/jmoiron/sqlx"
)

func CH_Connect() (*sqlx.DB, error) {

	connInfo := fmt.Sprintf("driver=ClickHouse ODBC Driver (Unicode);host=%s;port=%d;username=%s;password=%s;dbname=%s",
		config.Get().Clickhouse.Host, config.Get().Clickhouse.Port, config.Get().Clickhouse.User, config.Get().Clickhouse.Password, config.Get().Clickhouse.Name)

	connect, err := sqlx.Connect("odbc", connInfo)
	if err != nil {
		return nil, err
	}

	return connect, nil
}

func CH_ReadRegistry() error {

	start_time := time.Now()

	Statement := `
	SELECT 
	operation__operation_id AS operation_id, 
	billing__transaction_id AS transaction_id,
	billing__billing_operation_created_at AS operation_created_at,
	billing__merchant_id AS merchant_id, 
	operation__merchant_account_id AS merchant_account_id,
	billing__balance_id AS balance_id, 
	billing__company_id AS company_id, 
	billing__contract_id AS contract_id, 
	billing__project_id AS project_id, 
	billing__provider_id AS provider_id, 
	billing__tariff_conditions_id AS tariff_id,
	IFNULL(operation__provider_payment_id, '') AS provider_payment_id,
	operation__provider_name AS provider_name,
	operation__merchant_name AS merchant_name,
	operation__merchant_account_name AS merchant_account_name,
	IFNULL(operation__account_bank_name, '') AS account_bank_name,
	operation__project_name AS project_name,
	operation__payment_method_type AS payment_method_type,
	IFNULL(operation__issuer_country, '') AS country,
	IFNULL(operation__issuer_region, '') AS region,
	billing__operation_type_id AS operation_type_id,
	1 AS count_operations,
	operation__msc_amount AS msc_amount,
	IFNULL(operation__msc_currency, '') AS msc_currency,
	operation__provider_amount AS provider_amount,
	IFNULL(operation__provider_currency, '') AS provider_currency,	 
	operation__channel_amount AS channel_amount,
	IFNULL(operation__channel_currency, '') AS channel_currency,
	operation__fee_amount AS fee_amount,
	IFNULL(operation__fee_currency, '') AS fee_currency

	FROM reports
	
	WHERE 
		billing__billing_operation_created_at BETWEEN toDateTime('$1') AND toDateTime('$2')
		AND billing__merchant_id IN ($3)

	limit 3000000`

	merchant_str := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(config.Get().Registry.Merchant_id)), ","), "[]")

	Statement = strings.ReplaceAll(Statement, "$1", config.Get().Registry.DateFrom.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$2", config.Get().Registry.DateTo.Format(time.DateTime))
	Statement = strings.ReplaceAll(Statement, "$3", merchant_str)

	err := storage.Clickhouse.Select(&storage.Registry, Statement)

	if err != nil {
		return err
	}

	for _, o := range storage.Registry {
		o.StartingFill()
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра из Clickhouse: %v [%s строк]", time.Since(start_time), util.FormatInt(len(storage.Registry))))

	return nil

}
