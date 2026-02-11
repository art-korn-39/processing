package querrys

func Stat_Insert_provider_registry() string {
	return `
	INSERT INTO provider_registry (
		operation_id, transaction_completed_at, provider_name, merchant_name, merchant_account_name,
		project_url, payment_method_type, country, rate, operation_type, amount, transaction_created_at,
		provider_payment_id, operation_status, account_number, channel_currency, provider_currency, br_amount,
		transaction_completed_at_day, channel_amount, balance, provider1c, project_id, team, br_fix,
		user_tradex, team_id, comission_tradex, bonuses_tradex, provider_amount_tradex
	)
	VALUES (
		:operation_id, :transaction_completed_at, :provider_name, :merchant_name, :merchant_account_name,
		:project_url, :payment_method_type, :country, :rate, :operation_type, :amount, :transaction_created_at,
		:provider_payment_id, :operation_status, :account_number, :channel_currency, :provider_currency, :br_amount,
		:transaction_completed_at_day, :channel_amount, :balance, :provider1c, :project_id, :team, :br_fix,
		:user_tradex, :team_id, :comission_tradex, :bonuses_tradex, :provider_amount_tradex
	)
	
	ON CONFLICT ON CONSTRAINT pk_id DO UPDATE

	SET rate = EXCLUDED.rate, amount = EXCLUDED.amount, br_amount = EXCLUDED.br_amount,
		channel_amount = EXCLUDED.channel_amount, provider_currency = EXCLUDED.provider_currency,
		transaction_completed_at = EXCLUDED.transaction_completed_at, merchant_name = EXCLUDED.merchant_name,
		transaction_completed_at_day = EXCLUDED.transaction_completed_at_day, 
		operation_status = EXCLUDED.operation_status, balance = EXCLUDED.balance,
		project_id = EXCLUDED.project_id, team = EXCLUDED.team, br_fix = EXCLUDED.br_fix,
		transaction_created_at = EXCLUDED.transaction_created_at, provider1c = EXCLUDED.provider1c,
		user_tradex = EXCLUDED.user_tradex, team_id = EXCLUDED.team_id, comission_tradex = EXCLUDED.comission_tradex,
		bonuses_tradex = EXCLUDED.bonuses_tradex, provider_amount_tradex = EXCLUDED.provider_amount_tradex;`
}

func Stat_Insert_detailed() string {
	return `INSERT INTO detailed (
		document_id, operation_id, transaction_completed_at, merchant_id, 
		merchant_account_id, balance_id, company_id, contract_id, project_id,
		provider_id, provider_payment_id, provider_name, payment_id,
		merchant_name, merchant_account_name, provider_registry_amount,
		account_bank_name, project_name, payment_type, country, region, 
		operation_type, provider_amount, provider_currency, msc_amount, 
		msc_currency, channel_amount, channel_currency, fee_amount, 
		fee_currency, balance_amount, balance_currency, rate, payment_method,
		sr_channel_currency, sr_balance_currency, check_fee, verification, 
		crypto_network, convertation, provider_1c, subdivision_1c, 
		rated_account, tariff_id, tariff_date_start, act_percent, act_fix, 
		act_min, act_max, range_min, range_max, sr_referal, referal_name,
		tariff_rate_percent, tariff_rate_fix, tariff_rate_min, 
		tariff_rate_max, rr_amount, rr_date, is_test_id, is_test_type
	)
	VALUES (
		:document_id, :operation_id, :transaction_completed_at, :merchant_id,
		:merchant_account_id, :balance_id, :company_id, :contract_id, 
		:project_id, :provider_id, :provider_payment_id, :provider_name, 
		:payment_id, :merchant_name, :merchant_account_name, 
		:provider_registry_amount, :account_bank_name, :project_name, 
		:payment_type, :country, :region, :operation_type, :provider_amount,
		:provider_currency, :msc_amount, :msc_currency, :channel_amount, 
		:channel_currency, :fee_amount, :fee_currency, :balance_amount, 
		:balance_currency, :rate, :payment_method, :sr_channel_currency, 
		:sr_balance_currency, :check_fee, :verification, :crypto_network, 
		:convertation, :provider_1c, :subdivision_1c, :rated_account, :tariff_id,
		:tariff_date_start, :act_percent, :act_fix, :act_min, :act_max, 
		:range_min, :range_max, :sr_referal, :referal_name, 
		:tariff_rate_percent, :tariff_rate_fix, :tariff_rate_min, 
		:tariff_rate_max, :rr_amount, :rr_date, :is_test_id, :is_test_type
		)
	ON CONFLICT ON CONSTRAINT pk_detailed_id DO UPDATE SET 
		transaction_completed_at = EXCLUDED.transaction_completed_at,merchant_id = EXCLUDED.merchant_id,
		merchant_account_id = EXCLUDED.merchant_account_id,balance_id = EXCLUDED.balance_id,
		company_id = EXCLUDED.company_id,contract_id = EXCLUDED.contract_id,
		project_id = EXCLUDED.project_id,provider_id = EXCLUDED.provider_id,
		provider_payment_id = EXCLUDED.provider_payment_id,provider_name = EXCLUDED.provider_name,
		payment_id = EXCLUDED.payment_id,merchant_name = EXCLUDED.merchant_name,
		merchant_account_name = EXCLUDED.merchant_account_name,account_bank_name = EXCLUDED.account_bank_name,
		project_name = EXCLUDED.project_name,payment_type = EXCLUDED.payment_type,
		country = EXCLUDED.country,region = EXCLUDED.region,operation_type = EXCLUDED.operation_type,
		provider_amount = EXCLUDED.provider_amount,provider_currency = EXCLUDED.provider_currency,
		msc_amount = EXCLUDED.msc_amount,msc_currency = EXCLUDED.msc_currency,
		channel_amount = EXCLUDED.channel_amount,channel_currency = EXCLUDED.channel_currency,
		fee_amount = EXCLUDED.fee_amount,fee_currency = EXCLUDED.fee_currency,
		balance_amount = EXCLUDED.balance_amount,balance_currency = EXCLUDED.balance_currency,
		rate = EXCLUDED.rate, payment_method = EXCLUDED.payment_method, 
		sr_channel_currency = EXCLUDED.sr_channel_currency,
		sr_balance_currency = EXCLUDED.sr_balance_currency,document_id = EXCLUDED.document_id,
		check_fee = EXCLUDED.check_fee,provider_registry_amount = EXCLUDED.provider_registry_amount,
		verification = EXCLUDED.verification,crypto_network = EXCLUDED.crypto_network,
		convertation = EXCLUDED.convertation,provider_1c = EXCLUDED.provider_1c,
		subdivision_1c = EXCLUDED.subdivision_1c,rated_account = EXCLUDED.rated_account,
		tariff_id = EXCLUDED.tariff_id,tariff_date_start = EXCLUDED.tariff_date_start,
		act_percent = EXCLUDED.act_percent,act_fix = EXCLUDED.act_fix,act_min = EXCLUDED.act_min,
		act_max = EXCLUDED.act_max,range_min = EXCLUDED.range_min,range_max = EXCLUDED.range_max,
		sr_referal = EXCLUDED.sr_referal,referal_name = EXCLUDED.referal_name,
		tariff_rate_percent = EXCLUDED.tariff_rate_percent,tariff_rate_fix = EXCLUDED.tariff_rate_fix,
		tariff_rate_min = EXCLUDED.tariff_rate_min,tariff_rate_max = EXCLUDED.tariff_rate_max,
		rr_amount = EXCLUDED.rr_amount,rr_date = EXCLUDED.rr_date,
		is_test_id = EXCLUDED.is_test_id,is_test_type = EXCLUDED.is_test_type
		`
}

func Stat_Insert_detailed_provider() string {
	return `INSERT INTO detailed_provider (
		document_id, operation_id, provider_payment_id, transaction_id, rrn, payment_id, provider_id,
		provider_name, merchant_name, merchant_account_name, project_id, operation_type, merchant_id,
		payment_type, transaction_created_at, transaction_completed_at, channel_amount, channel_currency,
		provider_amount, provider_currency, operation_actual_amount, surcharge_amount, surcharge_currency,
		endpoint_id, account_bank_name, operation_created_at, balance_amount, br_balance_currency,
		balance_currency, rate, compensation_br, verification,tariff_date_start, act_percent, 
		act_fix, act_min, act_max, range_min, range_max, region, provider_dragonpay, balance_id,
		rr_amount, rr_date, is_test_id, is_test_type, provider_1c, is_perevodix, br_compensation,
		merchant_account_id
	)
	VALUES (
		:document_id, :operation_id,  :provider_payment_id, :transaction_id, :rrn, :payment_id, :provider_id,
		:provider_name, :merchant_name, :merchant_account_name, :project_id, :operation_type, :merchant_id,
		:payment_type, :transaction_created_at, :transaction_completed_at, :channel_amount, :channel_currency,
		:provider_amount, :provider_currency, :operation_actual_amount, :surcharge_amount, :surcharge_currency,
		:endpoint_id, :account_bank_name, :operation_created_at, :balance_amount, :br_balance_currency,
		:balance_currency, :rate, :compensation_br, :verification, :tariff_date_start, :act_percent, 
		:act_fix, :act_min, :act_max, :range_min, :range_max, :region, :provider_dragonpay, :balance_id,
		:rr_amount, :rr_date, :is_test_id, :is_test_type, :provider_1c, :is_perevodix, :br_compensation,
		:merchant_account_id
		)
	ON CONFLICT ON CONSTRAINT pk_detailed_provider_id DO UPDATE SET
		document_id = EXCLUDED.document_id, provider_payment_id = EXCLUDED.provider_payment_id,
		transaction_id = EXCLUDED.transaction_id,rrn = EXCLUDED.rrn,payment_id = EXCLUDED.payment_id,
		provider_id = EXCLUDED.provider_id,provider_name = EXCLUDED.provider_name,
		merchant_name = EXCLUDED.merchant_name,merchant_account_name = EXCLUDED.merchant_account_name,
		project_id = EXCLUDED.project_id,operation_type = EXCLUDED.operation_type,merchant_id = EXCLUDED.merchant_id,
		payment_type = EXCLUDED.payment_type,transaction_created_at = EXCLUDED.transaction_created_at,
		transaction_completed_at = EXCLUDED.transaction_completed_at,channel_amount = EXCLUDED.channel_amount,
		channel_currency = EXCLUDED.channel_currency,provider_amount = EXCLUDED.provider_amount,
		provider_currency = EXCLUDED.provider_currency,operation_actual_amount = EXCLUDED.operation_actual_amount,
		surcharge_amount = EXCLUDED.surcharge_amount,surcharge_currency = EXCLUDED.surcharge_currency,
		endpoint_id = EXCLUDED.endpoint_id,account_bank_name = EXCLUDED.account_bank_name,
		operation_created_at = EXCLUDED.operation_created_at,balance_amount = EXCLUDED.balance_amount,
		br_balance_currency = EXCLUDED.br_balance_currency,balance_currency = EXCLUDED.balance_currency,
		rate = EXCLUDED.rate,compensation_br = EXCLUDED.compensation_br,verification = EXCLUDED.verification,
		tariff_date_start = EXCLUDED.tariff_date_start,act_percent = EXCLUDED.act_percent,act_fix = EXCLUDED.act_fix,
		act_min = EXCLUDED.act_min,act_max = EXCLUDED.act_max,range_min = EXCLUDED.range_min,
		range_max = EXCLUDED.range_max,region = EXCLUDED.region,provider_dragonpay = EXCLUDED.provider_dragonpay,
		balance_id = EXCLUDED.balance_id,rr_amount = EXCLUDED.rr_amount,rr_date = EXCLUDED.rr_date,
		is_test_id = EXCLUDED.is_test_id,is_test_type = EXCLUDED.is_test_type,provider_1c = EXCLUDED.provider_1c,
		is_perevodix = EXCLUDED.is_perevodix,br_compensation = EXCLUDED.br_compensation,
		merchant_account_id = EXCLUDED.merchant_account_id		
	`
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

func Stat_Insert_crypto3() string {
	return `INSERT INTO crypto3 (
		charge_id, date, merchant_email, project_name, transaction_type, transaction_id, status,
		amount, network, fee, fee_network, merchant_amount, merchant_amount_network,
		fee_payer, transfer_fee, transfer_fee_network, transfer_fee_rate, transfer_fee_rate_usdt, 	
		markup_amount, markup_amount_usdt, currency, fee_currency, merchant_amount_currency, 
		transfer_fee_currency, markup_amount_currency
	)
	VALUES (
		:charge_id, :date, :merchant_email, :project_name, :transaction_type, :transaction_id, :status,
		:amount, :network, :fee, :fee_network, :merchant_amount, :merchant_amount_network,
		:fee_payer, :transfer_fee, :transfer_fee_network, :transfer_fee_rate, :transfer_fee_rate_usdt, 	
		:markup_amount, :markup_amount_usdt, :currency, :fee_currency, :merchant_amount_currency, 
		:transfer_fee_currency, :markup_amount_currency

	)

	ON CONFLICT ON CONSTRAINT pk_crypto3_transaction_id DO UPDATE

	SET date = EXCLUDED.date, merchant_email = EXCLUDED.merchant_email, project_name = EXCLUDED.project_name,
		transaction_type = EXCLUDED.transaction_type, charge_id = EXCLUDED.charge_id, status = EXCLUDED.status,
		amount = EXCLUDED.amount, network = EXCLUDED.network, fee = EXCLUDED.fee, fee_network = EXCLUDED.fee_network,
		merchant_amount = EXCLUDED.merchant_amount, merchant_amount_network = EXCLUDED.merchant_amount_network,
		fee_payer = EXCLUDED.fee_payer, transfer_fee = EXCLUDED.transfer_fee, 
		transfer_fee_network = EXCLUDED.transfer_fee_network, transfer_fee_rate = EXCLUDED.transfer_fee_rate,
		transfer_fee_rate_usdt  = EXCLUDED.transfer_fee_rate_usdt, markup_amount = EXCLUDED.markup_amount, 
		markup_amount_usdt = EXCLUDED.markup_amount_usdt, currency = EXCLUDED.currency, 
		fee_currency = EXCLUDED.fee_currency, merchant_amount_currency = EXCLUDED.merchant_amount_currency, 
		transfer_fee_currency = EXCLUDED.transfer_fee_currency, markup_amount_currency = EXCLUDED.markup_amount_currency`
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

func Stat_Insert_crm_payment_method() string {
	return `INSERT INTO crm_payment_method (
		id, name, parent_id, bof_id
	)
	VALUES (
		:id, :name, :parent_id, :bof_id
	)

	ON CONFLICT ON CONSTRAINT pk_payment_method_id DO UPDATE

	SET name = EXCLUDED.name, parent_id = EXCLUDED.parent_id, bof_id = EXCLUDED.bof_id`
}

func Stat_Insert_crm_payment_type() string {
	return `INSERT INTO crm_payment_type (
		id, name, method_id, bof_id
	)
	VALUES (
		:id, :name, :method_id, :bof_id
	)

	ON CONFLICT ON CONSTRAINT pk_payment_type_id DO UPDATE

	SET name = EXCLUDED.name, method_id = EXCLUDED.method_id, bof_id = EXCLUDED.bof_id`
}

func Stat_Insert_crm_employees() string {
	return `INSERT INTO crm_employees (
		id, name, email, department, job_title, manager
	)
	VALUES (
		:id, :name, :email, :department, :job_title, :manager
	)

	ON CONFLICT ON CONSTRAINT pk_crm_employees_id DO UPDATE

	SET name = EXCLUDED.name, email = EXCLUDED.email, 
		department = EXCLUDED.department, job_title = EXCLUDED.job_title,
		manager = EXCLUDED.manager`
}

func Stat_Insert_crm_merchants() string {
	return `INSERT INTO crm_merchants (
		id, name, bof_id, type, fin_manager_id, kam_id, kam_sub_id, status
	)
	VALUES (
		:id, :name, :bof_id, :type, :fin_manager_id, :kam_id, :kam_sub_id, :status
	)

	ON CONFLICT ON CONSTRAINT pk_crm_merchants_id DO UPDATE

	SET name = EXCLUDED.name, bof_id = EXCLUDED.bof_id, 
		type = EXCLUDED.type, fin_manager_id = EXCLUDED.fin_manager_id,
		kam_id = EXCLUDED.kam_id, kam_sub_id = EXCLUDED.kam_sub_id,
		status = EXCLUDED.status`
}

func Stat_Insert_crm_providers() string {
	return `INSERT INTO crm_providers (
		id, name, manager_id, manager_name, owner_id, owner_name, status 
	)
	VALUES (
		:id, :name, :manager_id, :manager_name, :owner_id, :owner_name, :status
	)

	ON CONFLICT ON CONSTRAINT pk_crm_providers_id DO UPDATE

	SET name = EXCLUDED.name, manager_id = EXCLUDED.manager_id, 
		manager_name = EXCLUDED.manager_name, owner_id = EXCLUDED.owner_id,
		owner_name = EXCLUDED.owner_name, status = EXCLUDED.status`
}

func Stat_Insert_crm_provider_solutions() string {
	return `INSERT INTO crm_provider_solutions (
		id, solution_name, provider_id, provider_name, provider_id_bof, 
		provider_name_bof, payment_method_id_bof, payment_method_name_bof 
	)
	VALUES (
		:id, :solution_name, :provider_id, :provider_name, :provider_id_bof, 
		:provider_name_bof, :payment_method_id_bof, :payment_method_name_bof 
	)

	ON CONFLICT ON CONSTRAINT pk_crm_provider_solutions_id DO UPDATE

	SET solution_name = EXCLUDED.solution_name, provider_id = EXCLUDED.provider_id, 
		provider_name = EXCLUDED.provider_name, provider_id_bof = EXCLUDED.provider_id_bof,
		provider_name_bof = EXCLUDED.provider_name_bof, payment_method_id_bof = EXCLUDED.payment_method_id_bof,
		payment_method_name_bof = EXCLUDED.payment_method_name_bof`
}

func Stat_Insert_crm_provider_losses() string {
	return `INSERT INTO crm_provider_losses (
		id,title,created_on,created_by_id,modified_on,modified_by_id,
		payment_provider_id,status,loss_key,loss_balance_code,loss_date,loss_id,
		loss_type,legal_entity,provider_solution_name,currency,provider_margin,
		loss_potential_status,loss_provider_balance,revisor_id,loss_occurence_date,
		loss_recolection_date,loss_sum,our_loss_sum,write_off_date,provider_loss_sum,comment
	)
	VALUES (
		:id,:title,:created_on,:created_by_id,:modified_on,:modified_by_id,
		:payment_provider_id,:status,:loss_key,:loss_balance_code,:loss_date,:loss_id,
		:loss_type,:legal_entity,:provider_solution_name,:currency,:provider_margin,
		:loss_potential_status,:loss_provider_balance,:revisor_id,:loss_occurence_date,
		:loss_recolection_date,:loss_sum,:our_loss_sum,:write_off_date,:provider_loss_sum,:comment 
	)

	ON CONFLICT ON CONSTRAINT pk_crm_provider_losses_id DO UPDATE

	SET title = EXCLUDED.title, modified_on = EXCLUDED.modified_on, 
		modified_by_id = EXCLUDED.modified_by_id, payment_provider_id = EXCLUDED.payment_provider_id,
		status = EXCLUDED.status, loss_key = EXCLUDED.loss_key, loss_balance_code = EXCLUDED.loss_balance_code,		
		loss_date = EXCLUDED.loss_date, loss_id = EXCLUDED.loss_id,loss_type = EXCLUDED.loss_type,	
		legal_entity = EXCLUDED.legal_entity,provider_solution_name = EXCLUDED.provider_solution_name,
		currency = EXCLUDED.currency,				
		provider_margin = EXCLUDED.provider_margin, loss_potential_status = EXCLUDED.loss_potential_status,
		loss_provider_balance = EXCLUDED.loss_provider_balance,	revisor_id = EXCLUDED.revisor_id, 
		loss_occurence_date = EXCLUDED.loss_occurence_date,loss_recolection_date = EXCLUDED.loss_recolection_date,	
		loss_sum = EXCLUDED.loss_sum,our_loss_sum = EXCLUDED.our_loss_sum,write_off_date = EXCLUDED.write_off_date, 
		provider_loss_sum = EXCLUDED.provider_loss_sum, comment = EXCLUDED.comment
		`
}

func Stat_Insert_crm_merchant_losses() string {
	return `INSERT INTO crm_merchant_losses (
		id,title,created_on,created_by_id,modified_on,modified_by_id,merchant_id,loss_in_provider_id,loss_sum,
		merchant_name,hold_comment_revision,hold_comment_fin,hold_date_removal,hold_sum,hold_date,provider_loss_sum,our_loss_sum,
		write_off_comment_date,write_off_comment,write_off_margin_fact,write_off_sum_fact,write_off_margin,write_off_sum,
		merchant_kam_id,merchant_fin_id,loss_merchant_number,write_off_fin_comment,kam_last_date_notif,write_off_done_date,
		kam_done_date,hold_done_date,is_partial_write_off,write_off_reported_date,write_off_last_notification_date,
		is_sum_not_match,is_cd_approved,our_loss_sum_fact,revisor_id,actualization_date,unhold_cause_id,merchant_bof_id,
		merchant_kam_sub_id,revisor_contact_id,write_off_sum_left,written_off_before,date_written_off_before,
		sum_written_off_before,is_written_off_before,our_loss_full_sum_fact,merchant_kam_substitutes,is_hold_sum_not_match,
		status,currency,loss_potential_status,loss_coverage_method,result
	)
	VALUES (
		:id,:title,:created_on,:created_by_id,:modified_on,:modified_by_id,:merchant_id,:loss_in_provider_id,:loss_sum,
		:merchant_name,:hold_comment_revision,:hold_comment_fin,:hold_date_removal,:hold_sum,:hold_date,:provider_loss_sum,:our_loss_sum,
		:write_off_comment_date,:write_off_comment,:write_off_margin_fact,:write_off_sum_fact,:write_off_margin,:write_off_sum,
		:merchant_kam_id,:merchant_fin_id,:loss_merchant_number,:write_off_fin_comment,:kam_last_date_notif,:write_off_done_date,
		:kam_done_date,:hold_done_date,:is_partial_write_off,:write_off_reported_date,:write_off_last_notification_date,
		:is_sum_not_match,:is_cd_approved,:our_loss_sum_fact,:revisor_id,:actualization_date,:unhold_cause_id,:merchant_bof_id,
		:merchant_kam_sub_id,:revisor_contact_id,:write_off_sum_left,:written_off_before,:date_written_off_before,
		:sum_written_off_before,:is_written_off_before,:our_loss_full_sum_fact,:merchant_kam_substitutes,:is_hold_sum_not_match,
		:status,:currency,:loss_potential_status,:loss_coverage_method,:result 
	)

	ON CONFLICT ON CONSTRAINT pk_crm_merchant_losses_id DO UPDATE

	SET
		title = EXCLUDED.title,created_on = EXCLUDED.created_on,
		created_by_id = EXCLUDED.created_by_id,modified_on = EXCLUDED.modified_on,
		modified_by_id = EXCLUDED.modified_by_id,merchant_id = EXCLUDED.merchant_id,
		loss_in_provider_id = EXCLUDED.loss_in_provider_id,loss_sum = EXCLUDED.loss_sum,
		merchant_name = EXCLUDED.merchant_name,hold_comment_revision = EXCLUDED.hold_comment_revision,
		hold_comment_fin = EXCLUDED.hold_comment_fin,hold_date_removal = EXCLUDED.hold_date_removal,
		hold_sum = EXCLUDED.hold_sum,hold_date = EXCLUDED.hold_date,provider_loss_sum = EXCLUDED.provider_loss_sum,
		our_loss_sum = EXCLUDED.our_loss_sum,write_off_comment_date = EXCLUDED.write_off_comment_date,
		write_off_comment = EXCLUDED.write_off_comment,write_off_margin_fact = EXCLUDED.write_off_margin_fact,
		write_off_sum_fact = EXCLUDED.write_off_sum_fact,write_off_margin = EXCLUDED.write_off_margin,
		write_off_sum = EXCLUDED.write_off_sum,merchant_kam_id = EXCLUDED.merchant_kam_id,
		merchant_fin_id = EXCLUDED.merchant_fin_id,loss_merchant_number = EXCLUDED.loss_merchant_number,
		write_off_fin_comment = EXCLUDED.write_off_fin_comment,kam_last_date_notif = EXCLUDED.kam_last_date_notif,
		write_off_done_date = EXCLUDED.write_off_done_date,kam_done_date = EXCLUDED.kam_done_date,
		hold_done_date = EXCLUDED.hold_done_date,is_partial_write_off = EXCLUDED.is_partial_write_off,
		write_off_reported_date = EXCLUDED.write_off_reported_date,
		write_off_last_notification_date = EXCLUDED.write_off_last_notification_date,
		is_sum_not_match = EXCLUDED.is_sum_not_match,is_cd_approved = EXCLUDED.is_cd_approved,
		our_loss_sum_fact = EXCLUDED.our_loss_sum_fact,revisor_id = EXCLUDED.revisor_id,
		actualization_date = EXCLUDED.actualization_date,unhold_cause_id = EXCLUDED.unhold_cause_id,
		merchant_bof_id = EXCLUDED.merchant_bof_id,merchant_kam_sub_id = EXCLUDED.merchant_kam_sub_id,
		revisor_contact_id = EXCLUDED.revisor_contact_id,write_off_sum_left = EXCLUDED.write_off_sum_left,
		written_off_before = EXCLUDED.written_off_before,date_written_off_before = EXCLUDED.date_written_off_before,
		sum_written_off_before = EXCLUDED.sum_written_off_before,is_written_off_before = EXCLUDED.is_written_off_before,
		our_loss_full_sum_fact = EXCLUDED.our_loss_full_sum_fact,merchant_kam_substitutes = EXCLUDED.merchant_kam_substitutes,
		is_hold_sum_not_match = EXCLUDED.is_hold_sum_not_match,status = EXCLUDED.status,currency = EXCLUDED.currency,
		loss_potential_status = EXCLUDED.loss_potential_status,loss_coverage_method = EXCLUDED.loss_coverage_method,
		result = EXCLUDED.result	
		`
}

func Stat_Insert_summary_merchant() string {
	return `INSERT INTO summary_merchant (
		document_id, document_date, operation_type, operation_group, 
		merchant_id, merchant_account_id, balance_id, provider_id, country, region, payment_type, channel_currency, 
		balance_currency, convertation, tariff_date_start, tariff_id, formula, channel_amount, balance_amount, 
		sr_channel_currency, sr_balance_currency, count_operations, rate, referal_guid,
		payment_type_id, payment_method_id, rated_account, provider_1c, subdivision_1c, business_type, project_id,
		rr_amount, rr_date, schema, convertation_id, provider_balance_guid, sr_referal
	)
	VALUES (
		:document_id, :document_date, :operation_type, :operation_group, :merchant_id, :merchant_account_id, 
		:balance_id, :provider_id, :country, :region, :payment_type, :channel_currency, :balance_currency, 
		:convertation, :tariff_date_start, :tariff_id, :formula, :channel_amount, :balance_amount, 
		:sr_channel_currency, :sr_balance_currency, :count_operations, :rate, :referal_guid,
		:payment_type_id, :payment_method_id, :rated_account, :provider_1c, :subdivision_1c, :business_type, :project_id,
		:rr_amount, :rr_date, :schema, :convertation_id, :provider_balance_guid, :sr_referal
		)`
}

func Stat_Insert_summary_provider() string {
	return `INSERT INTO summary_provider (
		document_id, document_date, operation_type, operation_group, 
		merchant_id, merchant_account_id, business_type,
		provider_id, country, region, payment_type, channel_currency, 
		balance_currency, convertation, tariff_date_start, tariff_guid, formula, channel_amount, balance_amount, 
		br_channel_currency, br_balance_currency, count_operations, rate, provider_1c,
		payment_type_id,  project_id, rr_amount, rr_date, convertation_id, extra_br_balance_currency,
		br_compensation, balance_id
	)
	VALUES (
		:document_id, :document_date, :operation_type, :operation_group, :merchant_id, :merchant_account_id, 
		:business_type, :provider_id, :country, :region, :payment_type, :channel_currency, :balance_currency, 
		:convertation, :tariff_date_start, :tariff_guid, :formula, :channel_amount, :balance_amount, 
		:br_channel_currency, :br_balance_currency, :count_operations, :rate, :provider_1c,
		:payment_type_id,  :project_id, :rr_amount, :rr_date, :convertation_id, :extra_br_balance_currency,
		:br_compensation, :balance_id
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

func Stat_Insert_bof_origamix() string {
	return `INSERT INTO bof_origamix (
		operation_id, payment_id, merchant_id,
		merchant_account_name, payment_method, payment_type,
		ps_id, ps_account, ps_provider,
		amount_init, amount_processed, currency,
		status, ps_code, ps_message,
		result_code, result_message, created_at, updated_at
	)
	VALUES (
		:operation_id, :payment_id, :merchant_id,
		:merchant_account_name, :payment_method, :payment_type,
		:ps_id, :ps_account, :ps_provider,
		:amount_init, :amount_processed, :currency,
		:status, :ps_code, :ps_message,
		:result_code, :result_message, :created_at, :updated_at
	)

	ON CONFLICT ON CONSTRAINT bof_origamix_operation_id DO UPDATE

	SET payment_id = EXCLUDED.payment_id, merchant_id = EXCLUDED.merchant_id, 
		merchant_account_name = EXCLUDED.merchant_account_name,
		payment_method = EXCLUDED.payment_method, payment_type = EXCLUDED.payment_type,
		ps_id = EXCLUDED.ps_id, ps_account = EXCLUDED.ps_account,
		ps_provider = EXCLUDED.ps_provider, amount_init = EXCLUDED.amount_init,
		amount_processed = EXCLUDED.amount_processed, currency = EXCLUDED.currency,
		status = EXCLUDED.status, ps_code = EXCLUDED.ps_code,
		ps_message = EXCLUDED.ps_message, result_code = EXCLUDED.result_code,
		result_message = EXCLUDED.result_message, created_at = EXCLUDED.created_at,
		updated_at = EXCLUDED.updated_at
	`
}
