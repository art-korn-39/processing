package querrys

import (
	"fmt"
)

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
	return `
	INSERT INTO provider_registry (
		operation_id, transaction_completed_at, provider_name, merchant_name, merchant_account_name,
		project_url, payment_method_type, country, rate, operation_type, amount,
		provider_payment_id, operation_status, account_number, channel_currency, provider_currency, br_amount,
		transaction_completed_at_day, channel_amount, balance, provider1c
	)
	VALUES (
		:operation_id, :transaction_completed_at, :provider_name, :merchant_name, :merchant_account_name,
		:project_url, :payment_method_type, :country, :rate, :operation_type, :amount,
		:provider_payment_id, :operation_status, :account_number, :channel_currency, :provider_currency, :br_amount,
		:transaction_completed_at_day, :channel_amount, :balance, :provider1c
	)
	
	ON CONFLICT ON CONSTRAINT pk_id DO UPDATE

	SET rate = EXCLUDED.rate, amount = EXCLUDED.amount, br_amount = EXCLUDED.br_amount,
		channel_amount = EXCLUDED.channel_amount, provider_currency = EXCLUDED.provider_currency,
		transaction_completed_at = EXCLUDED.transaction_completed_at, 
		transaction_completed_at_day = EXCLUDED.transaction_completed_at_day, 
		operation_status = EXCLUDED.operation_status, balance = EXCLUDED.balance, 
		provider1c = EXCLUDED.provider1c;`
}

func Stat_Insert_detailed() string {
	return `INSERT INTO detailed (
		document_id, operation_id, transaction_completed_at, merchant_id, merchant_account_id, balance_id, company_id,
		contract_id, project_id, provider_id, provider_payment_id, provider_name, merchant_name, merchant_account_name,
		account_bank_name, project_name, payment_type, country, region, operation_type, provider_amount,
		provider_currency, msc_amount, msc_currency, channel_amount, channel_currency, fee_amount, fee_currency,
		balance_amount, balance_currency, rate, sr_channel_currency, sr_balance_currency, check_fee, provider_registry_amount,
		verification, crypto_network, convertation, provider_1c, subdivision_1c, rated_account, tariff_id,
		tariff_date_start, act_percent, act_fix, act_min, act_max, range_min, range_max,
		tariff_rate_percent, tariff_rate_fix, tariff_rate_min, tariff_rate_max
	)
	VALUES (
		:document_id, :operation_id, :transaction_completed_at, :merchant_id, :merchant_account_id, :balance_id, :company_id,
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
		incoming_currency, coverted_currency, comment, date_day, created_at_day, bank_card
	)
	VALUES (
		:operation_id, :message_id, :date, :merchant_id, :merchant_account_id, :provider_id, :provider_name, 
		:merchant_name, :merchant_account_name,	:operation_type, :incoming_amount, :coverted_amount, :created_at,
		:incoming_currency, :coverted_currency, :comment, :date_day, :created_at_day, :bank_card
	)	

	ON CONFLICT ON CONSTRAINT pk_decline_operation_id DO UPDATE

	SET date = EXCLUDED.date, incoming_amount = EXCLUDED.incoming_amount, coverted_amount = EXCLUDED.coverted_amount,
		incoming_currency = EXCLUDED.incoming_currency, coverted_currency = EXCLUDED.coverted_currency,
		comment = EXCLUDED.comment, 
		bank_card = EXCLUDED.bank_card;`

}

func Stat_Insert_crypto() string {
	return `INSERT INTO crypto (
		operation_id, created_at, created_at_day, network, operation_type, 
		payment_amount, payment_currency, crypto_amount, crypto_currency
	)
	VALUES (
		:operation_id, :created_at, :created_at_day, :network, :operation_type, 
		:payment_amount, :payment_currency, :crypto_amount, :crypto_currency
	)

	ON CONFLICT ON CONSTRAINT pk_operation_id DO UPDATE

	SET created_at = EXCLUDED.created_at, created_at_day = EXCLUDED.created_at_day,
		payment_amount = EXCLUDED.payment_amount, crypto_amount = EXCLUDED.crypto_amount`
}

func Stat_Insert_dragonpay() string {
	return `INSERT INTO dragonpay (
		operation_id, provider, create_date, settle_date, refno, endpoint_id,
		currency, amount, fee_amount, description, message
	)
	VALUES (
		:operation_id, :provider, :create_date, :settle_date, :refno, :endpoint_id,
		:currency, :amount, :fee_amount, :description, :message
	)

	ON CONFLICT ON CONSTRAINT pk_dragonpay_operation_id DO UPDATE

	SET provider = EXCLUDED.provider, create_date = EXCLUDED.create_date,
		settle_date = EXCLUDED.settle_date, amount = EXCLUDED.amount, fee_amount = EXCLUDED.fee_amount`
}

func Stat_Insert_dragonpay_handbook() string {
	return `INSERT INTO dragonpay_handbook (
		endpoint_id, provider1c
	)
	VALUES (
		$1, $2
	)

	ON CONFLICT ON CONSTRAINT pk_dragonpay_handbook_endpoint_id DO UPDATE

	SET provider1c = EXCLUDED.provider1c`
}

func Stat_Insert_summary_merchant() string {
	return `INSERT INTO summary_merchant (
		document_id, document_date, operation_type, operation_group, 
		merchant_id, merchant_account_id, balance_id, provider_id, country, region, payment_type, channel_currency, 
		balance_currency, convertation, tariff_date_start, tariff_id, formula, channel_amount, balance_amount, 
		sr_channel_currency, sr_balance_currency, count_operations, rate,
		payment_type_id, payment_method_id, rated_account, provider_1c, subdivision_1c, business_type, project_id,
		rr_amount, rr_date
	)
	VALUES (
		:document_id, :document_date, :operation_type, :operation_group, :merchant_id, :merchant_account_id, 
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
