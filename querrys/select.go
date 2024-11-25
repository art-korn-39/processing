package querrys

import "time"

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
	IFNULL(operation__payment_method_name, '') AS payment_method,
	IFNULL(operation__payment_method_type, '') AS payment_type,
	IFNULL(billing__payment_type_id, 0) AS payment_type_id,
	IFNULL(billing__payment_method_id, 0) AS payment_method_id,
	IFNULL(operation__endpoint_id, '') AS endpoint_id,
	IFNULL(billing__legal_entity_id, 0) AS legal_entity_id,

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
	IFNULL(operation__currency, '') AS currency,
	IFNULL(operation__fee_amount, 0) AS fee_amount,
	IFNULL(operation__fee_currency, '') AS fee_currency,
	IFNULL(operation__surcharge_amount, 0) as surcharge_amount,
	IFNULL(operation__surcharge_currency, '') AS surcharge_currency,
	IFNULL(operation__actual_amount, 0) as actual_amount,

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

func Stat_Select_reports_by_id() string {
	return `
	SELECT
		toString(operation__operation_id) AS operation_id, 
		IFNULL(operation__provider_payment_id, '') AS provider_payment_id,
		date_add(HOUR, 3, billing__billing_operation_created_at) AS created_at,
		IFNULL(operation__provider_name, '') AS provider_name,
		IFNULL(operation__merchant_name, '') AS merchant_name,
		IFNULL(operation__merchant_account_name, '') AS merchant_account_name,
		IFNULL(billing__operation_type_id, 0) AS operation_type_id,
		IFNULL(operation__payment_method_type, '') AS payment_type,
		IFNULL(operation__issuer_country, '') AS country,
		IFNULL(operation__operation_status, '') AS status	
	FROM reports
	WHERE 
		toString(operation__$2) IN ('$1')`
}

func Stat_Select_provider_registry() string {
	return `SELECT operation_id, transaction_completed_at, operation_type, country, payment_method_type, 
			merchant_name, rate, amount, channel_amount, channel_currency, provider_currency, br_amount, balance, provider1c
		FROM provider_registry 
		WHERE lower(merchant_name) = ANY($1) 
		AND transaction_completed_at BETWEEN $2 AND $3`
}

func Stat_Select_conversion() string {
	return `SELECT 
				T1.guid,
				T1.name,
				T1.key_column,
				T1.file_format,
				T1.sheet_name,
				T1.comma,
				T2.registry_column,
				T2.table_column,
				T2.calculated,
				T2.from_bof,
				T2.date_format,
				T2.skip 
			FROM conversion_settings AS T1
				JOIN public.conversion_mapping AS T2
				ON T1.guid = T2.parent
			WHERE 
				T1.provider_guid = ANY($1)
			ORDER BY guid	`
}
