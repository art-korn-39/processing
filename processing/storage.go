package processing

import (
	"app/config"
	"app/logs"

	"github.com/jmoiron/sqlx"
)

type Storage struct {
	Postgres   *sqlx.DB
	Clickhouse *sqlx.DB

	Registry            []*Operation
	Tariffs             []Tariff
	Crypto              map[int]string
	Rates               []ProviderOperation
	Provider_operations map[int]ProviderOperation
}

func (s *Storage) Close() {
	if s.Postgres != nil {
		s.Postgres.Close()
	}
	if s.Clickhouse != nil {
		s.Clickhouse.Close()
	}
}

func (s *Storage) Init() (err error) {

	if config.Get().Clickhouse.Usage {
		connect, err := CH_Connect()
		if err != nil {
			//logs.Add(logs.FATAL, err)
			return err
		}
		logs.Add(logs.INFO, "Successful connection to Clickhouse")
		s.Clickhouse = connect
	}

	if config.Get().PSQL.Usage {
		connect, err := PSQL_connect()
		if err != nil {
			//logs.Add(logs.FATAL, err)
			return err
		}
		logs.Add(logs.INFO, "Successful connection to Postgres")
		s.Postgres = connect
	}

	return nil
}

func GetRegistryCount() int {
	return len(storage.Registry)
}

func GetWithoutTariffCount() int {
	count := 0
	for _, o := range storage.Registry {
		if o.Tariff == nil {
			count++
		}
	}
	return count
}

func GetCheckFeeCount() int {
	count := 0
	for _, o := range storage.Registry {
		if o.CheckFee != 0 {
			count++
		}
	}
	return count
}

func Stat_Insert_provider_registry() string {
	return `INSERT INTO provider_registry (
	operation_id, transaction_completed_at, provider_name, merchant_name, merchant_account_name,
	project_url, payment_method_type, country, rate, operation_type, amount,
	provider_payment_id, operation_status, account_number, channel_currency, provider_currency, br_amount
	)
	VALUES (
	:operation_id, :transaction_completed_at, :provider_name, :merchant_name, :merchant_account_name,
	:project_url, :payment_method_type, :country, :rate, :operation_type, :amount,
	:provider_payment_id, :operation_status, :account_number, :channel_currency, :provider_currency, :br_amount
	)`
}

func Stat_Insert_detailed() string {
	return `INSERT INTO detailed (
		operation_id, transaction_completed_at, merchant_id, merchant_account_id, balance_id, company_id,
		contract_id, project_id, provider_id, provider_payment_id, provider_name, merchant_name, merchant_account_name,
		account_bank_name, project_name, payment_method_type, country, region, operation_type, provider_amount,
		provider_currency, msc_amount, msc_currency, channel_amount, channel_currency, fee_amount, fee_currency,
		balance_amount, balance_currency, rate, sr_channel_currency, sr_balance_currency, check_fee, provider_registry_amount,
		verification, crypto_network, convertation, provider_1c, subdivision_1c, rated_account, tariff_id,
		tariff_date_start, act_percent, act_fix, act_min, act_max, range_min, range_max,
		tariff_rate_percent, tariff_rate_fix, tariff_rate_min, tariff_rate_max
	)
	VALUES (
		:operation_id, :transaction_completed_at, :merchant_id, :merchant_account_id, :balance_id, :company_id,
		:contract_id, :project_id, :provider_id, :provider_payment_id, :provider_name, :merchant_name, :merchant_account_name,
		:account_bank_name, :project_name, :payment_method_type, :country, :region, :operation_type, :provider_amount,
		:provider_currency, :msc_amount, :msc_currency, :channel_amount, :channel_currency, :fee_amount, :fee_currency,
		:balance_amount, :balance_currency, :rate, :sr_channel_currency, :sr_balance_currency, :check_fee, :provider_registry_amount,
		:verification, :crypto_network, :convertation, :provider_1c, :subdivision_1c, :rated_account, :tariff_id,
		:tariff_date_start, :act_percent, :act_fix, :act_min, :act_max, :range_min, :range_max,
		:tariff_rate_percent, :tariff_rate_fix, :tariff_rate_min, :tariff_rate_max
		)`
}

func Stat_Insert_decline() string {
	return `INSERT INTO decline (
		operation_id, message_id, date, merchant_id, merchant_account_id, provider_id, provider_name, 
		merchant_name, merchant_account_name, operation_type, incoming_amount, coverted_amount, created_at,
		incoming_currency, coverted_currency, comment, date_day, created_at_day
	)
	VALUES (
		:operation_id, :message_id, :date, :merchant_id, :merchant_account_id, :provider_id, :provider_name, 
		:merchant_name, :merchant_account_name,	:operation_type, :incoming_amount, :coverted_amount, :created_at,
		:incoming_currency, :coverted_currency, :comment, :date_day, :created_at_day
		)`
}

func Stat_Insert_summary_merchant() string {
	return `INSERT INTO summary_merchant (document_date, operation_type, operation_group, 
		merchant_id, merchant_account_id, balance_id, provider_id, country, region, payment_type, channel_currency, 
		balance_currency, convertation, tariff_date_start, tariff_id, formula, channel_amount, balance_amount, 
		sr_channel_currency, sr_balance_currency, count_operations)
	VALUES (:document_date, :operation_type, :operation_group, :merchant_id, :merchant_account_id, 
		:balance_id, :provider_id, :country, :region, :payment_type, :channel_currency, :balance_currency, 
		:convertation, :tariff_date_start, :tariff_id, :formula, :channel_amount, :balance_amount, 
		:sr_channel_currency, :sr_balance_currency, :count_operations)`
}
