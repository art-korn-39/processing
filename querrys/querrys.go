package querrys

import (
	"fmt"
	"time"
)

type Args struct {
	Merhcant []string
	DateFrom time.Time
	DateTo   time.Time
}

func Stat_Select_reports() string {
	return `
	SELECT 
	operation__operation_id AS operation_id, 
	billing__transaction_id AS transaction_id,
	date_add(HOUR, 3, billing__billing_operation_created_at) AS operation_created_at,
	IFNULL(billing__merchant_id, 0) AS merchant_id, 
	IFNULL(operation__merchant_account_id, 0) AS merchant_account_id,
	IFNULL(billing__balance_id, 0) AS balance_id, 
	IFNULL(billing__company_id, 0) AS company_id, 
	IFNULL(billing__contract_id, 0) AS contract_id, 
	IFNULL(billing__provider_id, 0) AS provider_id, 
	IFNULL(billing__tariff_conditions_id, 0) AS tariff_id,
	IFNULL(operation__provider_payment_id, '') AS provider_payment_id,
	IFNULL(operation__provider_name, '') AS provider_name,
	IFNULL(operation__merchant_name, '') AS merchant_name,
	IFNULL(operation__merchant_account_name, '') AS merchant_account_name,
	IFNULL(operation__account_bank_name, '') AS account_bank_name,

	IFNULL(operation__business_type, '') AS business_type,
	IFNULL(operation__project_name, '') AS project_name,
	IFNULL(billing__project_id, 0) AS project_id,
	IFNULL(operation__payment_method_type, '') AS payment_type,
	IFNULL(billing__payment_type_id, 0) AS payment_type_id,
	IFNULL(billing__payment_method_id, 0) AS payment_method_id,

	IFNULL(operation__issuer_country, '') AS country,
	IFNULL(operation__issuer_region, '') AS region,
	IFNULL(billing__operation_type_id, 0) AS operation_type_id,
	1 AS count_operations,
	IFNULL(operation__msc_amount, 0) AS msc_amount,
	IFNULL(operation__msc_currency, '') AS msc_currency,
	IFNULL(operation__provider_amount, 0) AS provider_amount,
	IFNULL(operation__provider_currency, '') AS provider_currency,	 
	IFNULL(operation__channel_amount, 0) AS channel_amount,
	IFNULL(operation__channel_currency, '') AS channel_currency,
	IFNULL(operation__fee_amount, 0) AS fee_amount,
	IFNULL(operation__fee_currency, '') AS fee_currency,

	IFNULL(billing__tariff_rate_fix, 0) AS billing__tariff_rate_fix,
	IFNULL(billing__tariff_rate_percent, 0) AS billing__tariff_rate_percent,
	IFNULL(billing__tariff_rate_min, 0) AS billing__tariff_rate_min,
	IFNULL(billing__tariff_rate_max, 0) AS billing__tariff_rate_max

	FROM reports
	
	WHERE 
		operation__operation_status = 'success'
		AND billing__billing_operation_created_at BETWEEN toDateTime('$1') AND toDateTime('$2')
		AND billing__merchant_id IN ($3)
		--AND billing__merchant_id IN (73162, 278, 104, 7201)

	limit 5000000`
}

func Stat_Select_provider_registry() string {
	return `SELECT operation_id, transaction_completed_at, operation_type, country,
		payment_method_type, merchant_name, rate, amount, channel_amount, channel_currency, provider_currency
		FROM provider_registry 
		WHERE merchant_name = ANY($1) 
		AND transaction_completed_at BETWEEN $2 AND $3`
}

func Stat_Insert_provider_registry_prev() string {
	return `INSERT INTO provider_registry (
		operation_id, transaction_completed_at, provider_name, merchant_name, merchant_account_name,
		project_url, payment_method_type, country, rate, operation_type, amount,
		provider_payment_id, operation_status, account_number, channel_currency, provider_currency, br_amount,
		transaction_completed_at_day, channel_amount
	)
	VALUES (
		:operation_id, :transaction_completed_at, :provider_name, :merchant_name, :merchant_account_name,
		:project_url, :payment_method_type, :country, :rate, :operation_type, :amount,
		:provider_payment_id, :operation_status, :account_number, :channel_currency, :provider_currency, :br_amount,
		:transaction_completed_at_day, :channel_amount
	)`
}

func Stat_Insert_provider_registry() string {
	return `INSERT INTO provider_registry (
		operation_id, transaction_completed_at, provider_name, merchant_name, merchant_account_name,
		project_url, payment_method_type, country, rate, operation_type, amount,
		provider_payment_id, operation_status, account_number, channel_currency, provider_currency, br_amount,
		transaction_completed_at_day, channel_amount, balance
	)
	VALUES (
		:operation_id, :transaction_completed_at, :provider_name, :merchant_name, :merchant_account_name,
		:project_url, :payment_method_type, :country, :rate, :operation_type, :amount,
		:provider_payment_id, :operation_status, :account_number, :channel_currency, :provider_currency, :br_amount,
		:transaction_completed_at_day, :channel_amount, :balance
	)
	
	ON CONFLICT ON CONSTRAINT pk_id DO UPDATE

	SET rate = EXCLUDED.rate, amount = EXCLUDED.amount, br_amount = EXCLUDED.br_amount,
		operation_status = EXCLUDED.operation_status, balance = EXCLUDED.balance`

	//ON CONFLICT ON CONSTRAINT pk_id_date_amount DO UPDATE
}

func Stat_Insert_detailed() string {
	return `INSERT INTO detailed (
		operation_id, transaction_completed_at, merchant_id, merchant_account_id, balance_id, company_id,
		contract_id, project_id, provider_id, provider_payment_id, provider_name, merchant_name, merchant_account_name,
		account_bank_name, project_name, payment_type, country, region, operation_type, provider_amount,
		provider_currency, msc_amount, msc_currency, channel_amount, channel_currency, fee_amount, fee_currency,
		balance_amount, balance_currency, rate, sr_channel_currency, sr_balance_currency, check_fee, provider_registry_amount,
		verification, crypto_network, convertation, provider_1c, subdivision_1c, rated_account, tariff_id,
		tariff_date_start, act_percent, act_fix, act_min, act_max, range_min, range_max,
		tariff_rate_percent, tariff_rate_fix, tariff_rate_min, tariff_rate_max
	)
	VALUES (
		:operation_id, :transaction_completed_at, :merchant_id, :merchant_account_id, :balance_id, :company_id,
		:contract_id, :project_id, :provider_id, :provider_payment_id, :provider_name, :merchant_name, :merchant_account_name,
		:account_bank_name, :project_name, :payment_type, :country, :region, :operation_type, :provider_amount,
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

func Stat_Insert_crypto() string {
	return `INSERT INTO crypto (
		operation_id, created_at, created_at_day, network, operation_type, 
		payment_amount, payment_currency, crypto_amount, crypto_currency
	)
	VALUES (
		:operation_id, :created_at, :created_at_day, :network, :operation_type, 
		:payment_amount, :payment_currency, :crypto_amount, :crypto_currency
		)`
}

func Stat_Insert_summary_merchant() string {
	return `INSERT INTO summary_merchant (
		document_date, operation_type, operation_group, 
		merchant_id, merchant_account_id, balance_id, provider_id, country, region, payment_type, channel_currency, 
		balance_currency, convertation, tariff_date_start, tariff_id, formula, channel_amount, balance_amount, 
		sr_channel_currency, sr_balance_currency, count_operations, rate,
		payment_type_id, payment_method_id, rated_account, provider_1c, subdivision_1c, business_type, project_id,
		rr_amount, rr_date
	)
	VALUES (
		:document_date, :operation_type, :operation_group, :merchant_id, :merchant_account_id, 
		:balance_id, :provider_id, :country, :region, :payment_type, :channel_currency, :balance_currency, 
		:convertation, :tariff_date_start, :tariff_id, :formula, :channel_amount, :balance_amount, 
		:sr_channel_currency, :sr_balance_currency, :count_operations, :rate,
		:payment_type_id, :payment_method_id, :rated_account, :provider_1c, :subdivision_1c, :business_type, :project_id,
		:rr_amount, :rr_date
		)`
}

func Stat_Insert_source_files() string {
	return `INSERT INTO source_files (
		filename, category, size, size_mb, modified, rows, last_upload
	)
	VALUES (
		:filename, :category, :size, :size_mb, :modified, :rows, :last_upload
		)`
}

func Stat_Insert_reports() string {
	return `INSERT INTO reports 
	SELECT * 
	FROM s3('https://s3.$region.amazonaws.com/$bucket/$filename', '$key', '$secret', 'CSV');`
}

func Stat_Insert_reports_before_250624() string {
	fields := fileds_before_250624()
	return fmt.Sprintf(`INSERT INTO reports
	(%s)
	SELECT %s
	FROM s3('https://s3.$region.amazonaws.com/$bucket/$filename', '$key', '$secret', 'CSV');`, fields, fields)
}

func fileds_before_250624() string {
	return `billing__amount,billing__balance_currency_id,billing__balance_id,billing__balance_type
	,billing__billing_operation_created_at,billing__billing_operation_id,billing__billing_operation_type_id
	,billing__billing_operation_updated_at,billing__company_id,billing__contract_conditions_id
	,billing__contract_id,billing__legal_entity_id,billing__merchant_id,billing__operation_currency_id
	,billing__operation_id,billing__operation_type_id,billing__payment_method_id,billing__payment_method_type_id
	,billing__project_id,billing__provider_id,billing__tariff_conditions_id,billing__transaction_id
	,billing__transaction_type_id,operation__account_bank_name,operation__actual_amount,operation__amount
	,operation__brand_id,operation__business_type,operation__channel_amount,operation__channel_currency
	,operation__coupled_operation_id,operation__currency,operation__fee_amount,operation__fee_currency
	,operation__issuer_country,operation__issuer_region,operation__merchant_account_id
	,operation__merchant_account_name,operation__merchant_name,operation__msc_amount,operation__msc_currency
	,operation__operation_created_at,operation__operation_id,operation__operation_status,operation__payment_id
	,operation__payment_method_name,operation__payment_method_type,operation__project_name,operation__provider_amount
	,operation__provider_currency,operation__provider_name,operation__provider_payment_id
	,operation__split_commission_balance_id,operation__surcharge_amount,operation__surcharge_currency,correction_flag`
}

func Stat_Optimize_reports() string {
	return `OPTIMIZE TABLE reports FINAL DEDUPLICATE BY billing__billing_operation_id;`
}
