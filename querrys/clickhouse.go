package querrys

import (
	"fmt"
	"strings"
)

func Stat_Insert_reports() string {
	return fmt.Sprintf(`INSERT INTO reports
	(%s)
	SELECT %s
	FROM s3('https://s3.$region.amazonaws.com/$bucket/$filename', '$key', '$secret', 'CSV')
	WHERE billing__billing_operation_id > 0;`,
		fileds_after_250624(false),
		fileds_after_250624(true))
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

func fileds_after_250624(selectPart bool) string {
	F := `billing__amount,
	billing__balance_currency_id,
	billing__balance_id,
	billing__balance_type,
	billing__billing_date,
	billing__billing_operation_created_at,
	billing__billing_operation_id,
	billing__billing_operation_type_id,
	billing__billing_operation_updated_at,
	billing__channel_amount,
	billing__channel_currency,
	billing__company_id,
	billing__contract_conditions_currency_id,
	billing__contract_conditions_id,
	billing__currency_rate,
	billing__currency_rate_exponent,
	billing__contract_id,
	billing__markup_percent,
	billing__legal_entity_id,
	billing__merchant_id,
	billing__operation_currency_id,
	billing__operation_id,
	billing__operation_type_id,
	billing__payment_method_id,
	billing__payment_method_type_id,
	billing__payment_type_id,
	billing__project_id,
	billing__provider_id,
	billing__tariff_amount,
	billing__tariff_condition_id,
	billing__tariff_conditions_id,
	billing__tariff_date_start,
	billing__tariff_percent_amount,
	billing__tariff_rate_fix,
	billing__tariff_rate_max,
	billing__tariff_rate_min,
	billing__tariff_rate_percent,
	billing__transaction_id,
	billing__transaction_type_id,
	operation__account_bank_name,
	operation__actual_amount,
	operation__amount,
	operation__brand_id,
	operation__business_type,
	operation__channel_amount,
	operation__channel_currency,
	operation__coupled_operation_id,
	operation__currency,
	operation__endpoint_id,
	operation__fee_amount,
	operation__fee_currency,
	operation__issuer_country,
	operation__issuer_region,
	operation__merchant_account_id,
	operation__merchant_account_name,
	operation__merchant_name,	
	operation__msc_amount,	
	operation__msc_currency,
	operation__operation_created_at,
	operation__operation_id,	
	operation__operation_status,
	operation__payment_id,	
	operation__payment_method_name,	
	operation__payment_method_type,	
	operation__project_name,
	operation__provider_amount,
	operation__provider_currency,
	operation__provider_name,	
	operation__provider_payment_id,
	operation__split_commission_balance_id,	
	operation__surcharge_amount,	
	operation__surcharge_currency,	
	correction_flag`
	// billing__billing_operation_updated_at_date,
	// operation__operation_created_at_date

	if selectPart {
		//F = strings.ReplaceAll(F, "billing__billing_operation_updated_at",
		//	"COALESCE(billing__billing_operation_updated_at, '0001-01-01 00:00:00')")
		F = strings.ReplaceAll(F, "operation__operation_created_at",
			"COALESCE(operation__operation_created_at, '0001-01-01 00:00:00')")
	}

	return F
}

func Stat_Optimize_reports() string {
	return `OPTIMIZE TABLE reports FINAL DEDUPLICATE BY billing__billing_operation_id;`
}
