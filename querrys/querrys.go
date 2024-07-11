package querrys

func Stat_Select_reports() string {
	return `
	SELECT 
	operation__operation_id AS operation_id, 
	billing__transaction_id AS transaction_id,
	billing__billing_operation_created_at AS operation_created_at,
	billing__merchant_id AS merchant_id, 
	operation__merchant_account_id AS merchant_account_id,
	billing__balance_id AS balance_id, 
	billing__company_id AS company_id, 
	billing__contract_id AS contract_id, 
	billing__provider_id AS provider_id, 
	billing__tariff_conditions_id AS tariff_id,
	IFNULL(operation__provider_payment_id, '') AS provider_payment_id,
	operation__provider_name AS provider_name,
	operation__merchant_name AS merchant_name,
	operation__merchant_account_name AS merchant_account_name,
	IFNULL(operation__account_bank_name, '') AS account_bank_name,

	operation__business_type AS business_type,
	operation__project_name AS project_name,
	billing__project_id AS project_id,
	operation__payment_method_type AS payment_type,
	billing__payment_type_id AS payment_type_id,
	billing__payment_method_id AS payment_method_id,

	IFNULL(operation__issuer_country, '') AS country,
	IFNULL(operation__issuer_region, '') AS region,
	billing__operation_type_id AS operation_type_id,
	1 AS count_operations,
	IFNULL(operation__msc_amount, 0) AS msc_amount,
	IFNULL(operation__msc_currency, '') AS msc_currency,
	IFNULL(operation__provider_amount, 0) AS provider_amount,
	IFNULL(operation__provider_currency, '') AS provider_currency,	 
	IFNULL(operation__channel_amount, 0) AS channel_amount,
	IFNULL(operation__channel_currency, '') AS channel_currency,
	IFNULL(operation__fee_amount, 0) AS fee_amount,
	IFNULL(operation__fee_currency, '') AS fee_currency

	FROM reports
	
	WHERE 
		billing__billing_operation_created_at BETWEEN toDateTime('$1') AND toDateTime('$2')
		AND billing__merchant_id IN ($3)

	limit 3000000`
}

func Stat_Select_provider_registry() string {
	return `SELECT operation_id, transaction_completed_at, operation_type, country,
		payment_method_type, merchant_name, rate, amount, channel_currency, provider_currency
		FROM provider_registry 
		WHERE merchant_name = ANY($1) 
		AND transaction_completed_at BETWEEN $2 AND $3`
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
		operation_id, created_at, network, operation_type, 
		payment_amount, payment_currency, crypto_amount, crypto_currency
	)
	VALUES (
		:operation_id, :created_at, :network, :operation_type, 
		:payment_amount, :payment_currency, :crypto_amount, :crypto_currency
		)`
}

func Stat_Insert_summary_merchant() string {
	return `INSERT INTO summary_merchant (
		document_date, operation_type, operation_group, 
		merchant_id, merchant_account_id, balance_id, provider_id, country, region, payment_type, channel_currency, 
		balance_currency, convertation, tariff_date_start, tariff_id, formula, channel_amount, balance_amount, 
		sr_channel_currency, sr_balance_currency, count_operations, rate,
		payment_type_id, payment_method_id, rated_account, provider_1c, subdivision_1c, business_type, project_id
	)
	VALUES (
		:document_date, :operation_type, :operation_group, :merchant_id, :merchant_account_id, 
		:balance_id, :provider_id, :country, :region, :payment_type, :channel_currency, :balance_currency, 
		:convertation, :tariff_date_start, :tariff_id, :formula, :channel_amount, :balance_amount, 
		:sr_channel_currency, :sr_balance_currency, :count_operations, :rate,
		:payment_type_id, :payment_method_id, :rated_account, :provider_1c, :subdivision_1c, :business_type, :project_id
		)`
}
