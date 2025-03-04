package querrys

func Stat_Insert_provider_registry() string {
	return `
	INSERT INTO provider_registry (
		operation_id, transaction_completed_at, provider_name, merchant_name, merchant_account_name,
		project_url, payment_method_type, country, rate, operation_type, amount, transaction_created_at,
		provider_payment_id, operation_status, account_number, channel_currency, provider_currency, br_amount,
		transaction_completed_at_day, channel_amount, balance, provider1c, project_id, team
	)
	VALUES (
		:operation_id, :transaction_completed_at, :provider_name, :merchant_name, :merchant_account_name,
		:project_url, :payment_method_type, :country, :rate, :operation_type, :amount, :transaction_created_at,
		:provider_payment_id, :operation_status, :account_number, :channel_currency, :provider_currency, :br_amount,
		:transaction_completed_at_day, :channel_amount, :balance, :provider1c, :project_id, :team
	)
	
	ON CONFLICT ON CONSTRAINT pk_id DO UPDATE

	SET rate = EXCLUDED.rate, amount = EXCLUDED.amount, br_amount = EXCLUDED.br_amount,
		channel_amount = EXCLUDED.channel_amount, provider_currency = EXCLUDED.provider_currency,
		transaction_completed_at = EXCLUDED.transaction_completed_at, 
		transaction_completed_at_day = EXCLUDED.transaction_completed_at_day, 
		operation_status = EXCLUDED.operation_status, balance = EXCLUDED.balance,
		project_id = EXCLUDED.project_id, team = EXCLUDED.team, 
		transaction_created_at = EXCLUDED.transaction_created_at, provider1c = EXCLUDED.provider1c;`
}

func Stat_Insert_provider_registry_test() string {
	return `
	INSERT INTO provider_registry_test (
		operation_id, transaction_completed_at, provider_name, merchant_name, merchant_account_name,
		payment_method_type, country, rate, operation_type, amount,
		provider_payment_id, account_number, channel_currency, provider_currency, br_amount,
		transaction_completed_at_day, channel_amount, balance, provider1c, transaction_created_at, 
		project_id, team
	)
	VALUES (
		:operation_id, :transaction_completed_at, :provider_name, :merchant_name, :merchant_account_name,
		:payment_method_type, :country, :rate, :operation_type, :amount,
		:provider_payment_id, :account_number, :channel_currency, :provider_currency, :br_amount,
		:transaction_completed_at_day, :channel_amount, :balance, :provider1c, :transaction_created_at, 
		:project_id, :team
	)
	
	ON CONFLICT ON CONSTRAINT pk_provider_registry_test_id DO UPDATE

	SET rate = EXCLUDED.rate, amount = EXCLUDED.amount, br_amount = EXCLUDED.br_amount,
		channel_amount = EXCLUDED.channel_amount, provider_currency = EXCLUDED.provider_currency,
		transaction_completed_at = EXCLUDED.transaction_completed_at, 
		transaction_completed_at_day = EXCLUDED.transaction_completed_at_day, 
		balance = EXCLUDED.balance, 
		provider1c = EXCLUDED.provider1c, provider_name = EXCLUDED.provider_name, 
		merchant_name = EXCLUDED.merchant_name, merchant_account_name = EXCLUDED.merchant_account_name,
		payment_method_type = EXCLUDED.payment_method_type, team = EXCLUDED.team,
		country = EXCLUDED.country, operation_type = EXCLUDED.operation_type, 
		provider_payment_id = EXCLUDED.provider_payment_id, account_number = EXCLUDED.account_number,
		transaction_created_at = EXCLUDED.transaction_created_at, project_id = EXCLUDED.project_id,
		channel_currency = EXCLUDED.channel_currency;`
}

func Stat_Insert_detailed() string {
	return `INSERT INTO detailed (
		document_id, operation_id, transaction_completed_at, merchant_id, merchant_account_id, balance_id, company_id,
		contract_id, project_id, provider_id, provider_payment_id, provider_name, payment_id,
		merchant_name, merchant_account_name,
		account_bank_name, project_name, payment_type, country, region, operation_type, provider_amount,
		provider_currency, msc_amount, msc_currency, channel_amount, channel_currency, fee_amount, fee_currency,
		balance_amount, balance_currency, rate, sr_channel_currency, sr_balance_currency, check_fee, provider_registry_amount,
		verification, crypto_network, convertation, provider_1c, subdivision_1c, rated_account, tariff_id,
		tariff_date_start, act_percent, act_fix, act_min, act_max, range_min, range_max,
		tariff_rate_percent, tariff_rate_fix, tariff_rate_min, tariff_rate_max
	)
	VALUES (
		:document_id, :operation_id, :transaction_completed_at, :merchant_id, :merchant_account_id, :balance_id, :company_id,
		:contract_id, :project_id, :provider_id, :provider_payment_id, :provider_name, :payment_id,
		:merchant_name, :merchant_account_name,
		:account_bank_name, :project_name, :payment_type, :country, :region, :operation_type, :provider_amount,
		:provider_currency, :msc_amount, :msc_currency, :channel_amount, :channel_currency, :fee_amount, :fee_currency,
		:balance_amount, :balance_currency, :rate, :sr_channel_currency, :sr_balance_currency, :check_fee, :provider_registry_amount,
		:verification, :crypto_network, :convertation, :provider_1c, :subdivision_1c, :rated_account, :tariff_id,
		:tariff_date_start, :act_percent, :act_fix, :act_min, :act_max, :range_min, :range_max,
		:tariff_rate_percent, :tariff_rate_fix, :tariff_rate_min, :tariff_rate_max
		)`
}

func Stat_Insert_detailed_provider() string {
	return `INSERT INTO detailed_provider (
		document_id, operation_id, provider_payment_id, transaction_id, rrn, payment_id,
		provider_name, merchant_name, merchant_account_name, project_id, operation_type,
		payment_type, transaction_created_at, transaction_completed_at, channel_amount, channel_currency,
		provider_amount, provider_currency, operation_actual_amount, surcharge_amount, surcharge_currency,
		endpoint_id, account_bank_name, operation_created_at, balance_amount, br_balance_currency,
		balance_currency, rate, compensation_br, verification,
		tariff_date_start, act_percent, act_fix, act_min, act_max, range_min, range_max, region
	)
	VALUES (
		:document_id, :operation_id,  :provider_payment_id, :transaction_id, :rrn, :payment_id,
		:provider_name, :merchant_name, :merchant_account_name, :project_id, :operation_type,
		:payment_type, :transaction_created_at, :transaction_completed_at, :channel_amount, :channel_currency,
		:provider_amount, :provider_currency, :operation_actual_amount, :surcharge_amount, :surcharge_currency,
		:endpoint_id, :account_bank_name, :operation_created_at, :balance_amount, :br_balance_currency,
		:balance_currency, :rate, :compensation_br, :verification,
		:tariff_date_start, :act_percent, :act_fix, :act_min, :act_max, :range_min, :range_max, :region
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
		payment_amount, payment_currency, crypto_amount, crypto_currency,
		transfer_fee_rate_usdt
	)
	VALUES (
		:operation_id, :created_at, :created_at_day, :network, :operation_type, 
		:payment_amount, :payment_currency, :crypto_amount, :crypto_currency,
		:transfer_fee_rate_usdt
	)

	ON CONFLICT ON CONSTRAINT pk_operation_id DO UPDATE

	SET created_at = EXCLUDED.created_at, created_at_day = EXCLUDED.created_at_day,
		payment_amount = EXCLUDED.payment_amount, crypto_amount = EXCLUDED.crypto_amount,
		transfer_fee_rate_usdt = EXCLUDED.transfer_fee_rate_usdt`
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

func Stat_Insert_chargeback() string {
	return `INSERT INTO chargebacks (
		id, name, case_id, created_on, total_amount, account_number, deadline, receipt_date,
		status, brand, code_reason, merchant_id, merchant_name, provider_id, provider_name
	)
	VALUES (
		:id, :name, :case_id, :created_on, :total_amount, :account_number, :deadline, :receipt_date,
		:status, :brand, :code_reason, :merchant_id, :merchant_name, :provider_id, :provider_name
	)

	ON CONFLICT ON CONSTRAINT pk_chargebacks_id DO UPDATE

	SET total_amount = EXCLUDED.total_amount, status = EXCLUDED.status`
}

func Stat_Insert_chargeback_operations() string {
	return `INSERT INTO chargeback_operations (
		guid, id, created_on, modified_on, rrn, receipt_date, provider_payment_id, account_number,
		project_id, project_name, merchant_id, merchant_name, provider_id, provider_name, 
		merchant_account_id, merchant_account_name, payment_type_id, payment_type_name, amount,
		channel_amount, amount_usd, channel_amount_usd, amount_rub, channel_amount_rub,
		type, channel_currency, transaction_status, state, state_change_date,
		chargeback_id, chargeback_case_id, chargeback_status, chargeback_deadline, chargeback_code_reason
	)
	VALUES (
		:guid, :id, :created_on, :modified_on, :rrn, :receipt_date, :provider_payment_id, :account_number,
		:project_id, :project_name, :merchant_id, :merchant_name, :provider_id, :provider_name, 
		:merchant_account_id, :merchant_account_name, :payment_type_id, :payment_type_name, :amount,
		:channel_amount, :amount_usd, :channel_amount_usd, :amount_rub, :channel_amount_rub,
		:type, :channel_currency, :transaction_status, :state, :state_change_date,
		:chargeback_id, :chargeback_case_id, :chargeback_status, :chargeback_deadline, :chargeback_code_reason
	)

	ON CONFLICT ON CONSTRAINT pk_chargeback_operations_guid DO UPDATE

	SET modified_on = EXCLUDED.modified_on, amount = EXCLUDED.amount, 
		channel_amount = EXCLUDED.channel_amount, amount_usd = EXCLUDED.amount_usd, 
		channel_amount_usd = EXCLUDED.channel_amount_usd, amount_rub = EXCLUDED.amount_rub,
		channel_amount_rub = EXCLUDED.channel_amount_rub, type = EXCLUDED.type, 
		channel_currency = EXCLUDED.channel_currency, transaction_status = EXCLUDED.transaction_status, 
		chargeback_id = EXCLUDED.chargeback_id, state = EXCLUDED.state, state_change_date = EXCLUDED.state_change_date,
		chargeback_case_id = EXCLUDED.chargeback_case_id, chargeback_status = EXCLUDED.chargeback_status,
		chargeback_deadline = EXCLUDED.chargeback_deadline, chargeback_code_reason = EXCLUDED.chargeback_code_reason`
}

func Stat_Insert_payment_method() string {
	return `INSERT INTO payment_method (
		id, name, parent_id, bof_id
	)
	VALUES (
		:id, :name, :parent_id, :bof_id
	)

	ON CONFLICT ON CONSTRAINT pk_payment_method_id DO UPDATE

	SET name = EXCLUDED.name, parent_id = EXCLUDED.parent_id, bof_id = EXCLUDED.bof_id`
}

func Stat_Insert_payment_type() string {
	return `INSERT INTO payment_type (
		id, name, method_id, bof_id
	)
	VALUES (
		:id, :name, :method_id, :bof_id
	)

	ON CONFLICT ON CONSTRAINT pk_payment_type_id DO UPDATE

	SET name = EXCLUDED.name, method_id = EXCLUDED.method_id, bof_id = EXCLUDED.bof_id`
}

func Stat_Insert_summary_merchant() string {
	return `INSERT INTO summary_merchant (
		document_id, document_date, operation_type, operation_group, 
		merchant_id, merchant_account_id, balance_id, provider_id, country, region, payment_type, channel_currency, 
		balance_currency, convertation, tariff_date_start, tariff_id, formula, channel_amount, balance_amount, 
		sr_channel_currency, sr_balance_currency, count_operations, rate,
		payment_type_id, payment_method_id, rated_account, provider_1c, subdivision_1c, business_type, project_id,
		rr_amount, rr_date, schema, convertation_id
	)
	VALUES (
		:document_id, :document_date, :operation_type, :operation_group, :merchant_id, :merchant_account_id, 
		:balance_id, :provider_id, :country, :region, :payment_type, :channel_currency, :balance_currency, 
		:convertation, :tariff_date_start, :tariff_id, :formula, :channel_amount, :balance_amount, 
		:sr_channel_currency, :sr_balance_currency, :count_operations, :rate,
		:payment_type_id, :payment_method_id, :rated_account, :provider_1c, :subdivision_1c, :business_type, :project_id,
		:rr_amount, :rr_date, :schema, :convertation_id
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
