package querrys

import "time"

type Args struct {
	Merhcant    []string
	Merchant_id []int
	DateFrom    time.Time
	DateTo      time.Time
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
		IFNULL(operation__merchant_account_id, 0) AS merchant_account_id,
		IFNULL(billing__provider_id, 0) AS provider_id,
		IFNULL(billing__project_id, 0) AS project_id, 
		date_add(HOUR, 3, billing__billing_operation_created_at) AS transaction_created_at,
		date_add(HOUR, 3, billing__billing_operation_created_at) AS transaction_completed_at,
		IFNULL(operation__provider_name, '') AS provider_name,
		IFNULL(operation__merchant_name, '') AS merchant_name,
		IFNULL(operation__merchant_account_name, '') AS merchant_account_name,
		IFNULL(billing__operation_type_id, 0) AS operation_type_id,
		IFNULL(operation__payment_method_type, '') AS payment_type,
		IFNULL(operation__issuer_country, '') AS country	
	FROM reports
	WHERE 
		toString(operation__$2) IN ('$1')`
}

func Stat_Select_provider_registry() string {
	return `SELECT 
			operation_id, transaction_completed_at, operation_type, country, payment_method_type, 
			merchant_name, rate, amount, channel_amount, channel_currency, provider_currency, br_amount, 
			balance, provider1c, team, project_url, project_id
		FROM provider_registry 
		WHERE lower(merchant_name) = ANY($1) 
		AND transaction_completed_at BETWEEN $2 AND $3`
}

func Stat_Select_provider_registry_period_only() string {
	return `SELECT 
			operation_id, transaction_completed_at, operation_type, country, payment_method_type, 
			merchant_name, rate, amount, channel_amount, channel_currency, provider_currency, 
			br_amount, balance, provider1c, team, project_url, project_id
		FROM provider_registry 
		WHERE transaction_completed_at BETWEEN $1 AND $2`
}

func Stat_Select_tariffs_merchant() string {
	return `SELECT 
				id,balance_id,balance_name,balance_code,merchant_account_id,
				merchant_account_name,provider_name,schema,convertation,merchant_id, 
				operation_type,rr_days,rr_percent,subdivision1c,provider1c, 
				ratedaccount,balance_type,date_start_ps,balance_currency,
				date_start,range_min,range_max,percent,fix,min,max,dk_percent,
				dk_fix,dk_min,dk_max,currency_commission,network_type, 
				payment_type,company,date_start_ma,date_finish_ma
			FROM tariffs_merchant
			WHERE merchant_id = ANY($1)`
}

func Stat_Select_tariffs_provider() string {
	return `SELECT 
				guid,provider_balance_guid,provider_balance_name,
				date_start,merchant_name,merchant_account_name,merchant_legal_entity,
				payment_method,payment_method_type,region,channel_currency,project_name,
				business_type,operation_group,traffic_type,account_bank_name, use_transaction_created_at,
				tariff_range_turnouver_min,tariff_range_turnouver_max,tariff_range_amount_min,
				tariff_range_amount_max,percent,fix,min,max,search_string_ma,endpoint_id
			FROM tariffs_provider`
}

func Stat_Select_provider_balances() string {
	return `SELECT 
				guid,provider_balance,contractor,provider_name,provider_id,balance_code,
				legal_entity,merchant_account,merchant_account_id,date_start,nickname,
				date_finish,convertation,convertation_id,key_record,balance_currency,type,
				extra_balance_guid
			FROM provider_balances
			WHERE provider_id > 0 AND merchant_account_id > 0`
}

func Stat_Select_crypto() string {
	return `SELECT 
				operation_id,network,created_at,created_at_day,operation_type,
				payment_amount,payment_currency,crypto_amount,crypto_currency
			FROM crypto`
}

func Stat_Select_countries() string {
	return `SELECT 
				region_name,name,name_en,code,code2,code3,currency
			FROM countries`
}

func Stat_Select_merchants() string {
	return `SELECT 
				contractor_name,contractor_guid,merchant_name,
				merchant_id,project_name,project_id,project_url
			FROM merchants`
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
				T2.external_source,
				T2.date_format,
				T2.calculation_format,
				T2.skip 
			FROM conversion_settings AS T1
				JOIN public.conversion_mapping AS T2
				ON T1.guid = T2.parent_guid
			WHERE 
				T1.provider_guid = ANY($1)
			ORDER BY guid	`
}

func Stat_Select_dragonpay() string {
	return `SELECT 
				operation_id,provider,create_date,settle_date,refno,
				currency,amount,endpoint_id,fee_amount,description,message
			FROM dragonpay`
}

func Stat_Select_dragonpay_handbook() string {
	return `SELECT 
				endpoint_id,provider1c,payment_type
			FROM dragonpay_handbook`
}
