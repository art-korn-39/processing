package validation

import (
	"fmt"
	"strings"

	"github.com/tealeg/xlsx"
)

func GetMapOfColumnNamesStrings(s []string) (res map[string]int) {

	res = map[string]int{}

	for i, name := range s {
		res[strings.ToLower(name)] = i + 1
	}

	return res

}

func GetMapOfColumnNamesCells(s []*xlsx.Cell) (res map[string]int) {

	res = map[string]int{}

	for i, cell := range s {
		name := strings.ToLower(cell.String())
		if name == "" {
			continue
		}
		res[name] = i + 1
	}

	return res

}

func CheckMapOfColumnNames(map_fileds map[string]int, table string) error {

	var s []string

	switch table {
	case "bof_registry_merchant":
		s = fields_bof_registry_merchant()
	case "bof_registry_provider":
		s = fields_bof_registry_provider()
	case "tariff_merchant":
		s = fields_tariff_merchant()
	case "tariff_provider":
		s = fields_tariff_provider()
	case "holds":
		s = fields_holds()
	case "crypto":
		s = fields_crypto()
	case "decline_csv":
		s = fields_decline_csv()
	case "dragonpay_csv":
		s = fields_dragonpay_csv()
	case "dragonpay_xlsx":
		s = fields_dragonpay_xlsx()
	case "provider_registry":
		s = fields_provider_registry()
	case "kgx_teams_xlsx":
		s = fields_kgx_teams_xlsx()
	case "kgx_providers_xlsx":
		s = fields_kgx_providers_xlsx()
	case "bof_registry_raw_conversion":
		s = fields_bof_registry_raw_conversion()
	case "origamix":
		s = fields_origamix()
	default:
		return fmt.Errorf("table %s is not supported", table)
	}

	for _, v := range s {

		_, ok := map_fileds[v]
		if !ok {
			return fmt.Errorf("в таблице %s отсутствует обязательное поле: %s", table, v)
		}

	}

	return nil

}

func fields_bof_registry_merchant() []string {
	return []string{
		"id / operation_id", "transaction_id", "transaction_completed_at",
		"merchant_id", "merchant_account_id", "project_id", "project_name",
		"provider_name", "merchant_name", "merchant_account_name",
		"acquirer_id / provider_payment_id", "issuer_country",
		"operation_type", "balance_id", "payment_type_id / payment_method_type",
		"contract_id", "tariff_condition_id", "external_id / payment_id",
		"currency / currency",
		"real_currency / channel_currency", "real_amount / channel_amount",
		"fee_currency", "fee_amount",
		"provider_currency", "provider_amount",
		"tariff_rate_percent", "tariff_rate_fix", "tariff_rate_min", "tariff_rate_max",
	}
}

func fields_bof_registry_provider() []string {
	return []string{
		"id / operation_id", "transaction_id", "transaction_completed_at",
		"merchant_id", "merchant_account_id", "project_id", "project_name", "project_url",
		"provider_name", "merchant_name", "merchant_account_name", "provider_id",
		"acquirer_id / provider_payment_id", "issuer_country",
		"operation_type", "payment_type_id / payment_method_type",
		"real_currency / channel_currency", "real_amount / channel_amount",
		"currency / currency",
		"provider_currency", "provider_amount",
		"legal_entity_id", "business_type", "account_bank_name", "payment_method_name",
		"surcharge_currency", "surcharge_amount",
		"rrn", "external_id / payment_id", "transaction_created_at", "operation_actual_amount",
	}
}

func fields_tariff_merchant() []string {
	return []string{
		"баланс", "мерчант", "merchant account id", "provider", "валюта баланса мерчанта в боф",
		"валюта учетная", "дата старта", "конверт", "operation_type", "man",
		"%", "fix", "min", "max", "range min", "range max", "id баланса в бофе", "tarif_condition_id",
		"id баланса в бофе", "тип баланса в бофе (in/ out/ in-out)", "подразделение 1с", "поставщик в 1с", "расчетный счет",
		"рр, процент (пс)", "дата нач.раб пс", "схема", "рр, дней (пс)", "код баланса по справочнику",
		"%дк", "fixдк", "minдк", "maxдк",
	}
}

func fields_tariff_provider() []string {
	return []string{
		//"идентификатор сверки", "организация", "провайдер", "provider_name",
		"date_of_start", "merchant_name", "merchant_account_name", "merchant_legal_entity",
		"payment_method", "payment_method_type", "region", "channel_currency", "project_name", "business_type",
		"operation_group", "tariff range turnouver min", "tariff range turnouver max",
		"tariff range amount min", "tariff range amount max", "percent", "fix", "min commission", "max commission",
		"traffic_type", "account_bank_name",
	}
}

func fields_holds() []string {
	return []string{
		"схема", "валюта", "ma_id", "ma_name", "дата старта", "процент холда", "кол-во дней",
	}
}

func fields_crypto() []string {
	return []string{"operation id", "crypto network", "created at", "operation type",
		"payment amount", "payment currency", "crypto amount", "crypto currency",
		"transfer fee rate, usdt"}
}

func fields_decline_csv() []string {
	return []string{
		"id", "created_at",
		"merchant_id", "merchant_title",
		"provider_id", "provider_title",
		"merchant_account_id", "merchant_account_title",
		"operation_type",
		"incoming_amount", "currency_incoming",
		"coverted_amount", "currency_converted",
		"proof_link",
	}
}

func fields_dragonpay_csv() []string {
	return []string{"merchant txn id", "create date", "refno",
		"ccy", "amount", "proc", "fee"}
}

func fields_dragonpay_xlsx() []string {
	return []string{"endpoint_id", "поставщик dragonpay"}
}

func fields_kgx_teams_xlsx() []string {
	return []string{"team_id", "balance", "team"}
}

func fields_kgx_providers_xlsx() []string {
	return []string{"issuer_country", "payment_type_id / payment_method_type", "баланс", "валюта в пс", "provider1c"}
}

func fields_provider_registry() []string {
	return []string{
		"id / operation_id", "transaction_completed_at", "transaction_created_at",
		"operation_type", "issuer_country",
		"payment_type_id / payment_method_type",
		"merchant_name", "real_currency / channel_currency", "real_amount / channel_amount", "курс",
		//"provider_currency",
		"provider_name", "merchant_account_name", "acquirer_id / provider_payment_id",
	}
}

func fields_bof_registry_raw_conversion() []string {
	return []string{
		"id / operation_id", "merchant_account_id", "provider_id",
		"provider_name", "merchant_name", "merchant_account_name",
		"operation_type", "transaction_completed_at", "transaction_created_at",
		"payment_type_id / payment_method_type", "issuer_country",
		"real_currency / channel_currency", "real_amount / channel_amount",
		"acquirer_id / provider_payment_id",
		"project_id",
		//"project_url", "operation_status",
	}
}

func fields_origamix() []string {
	return []string{
		"payment id", "merchant id",
		"merchant",         //"merchant account id",
		"merchant account", //"merchant payment id",
		"payment type", "payment method", "operation id",
		"ps provider", "ps account", //"ps account id",
		"ps operation id", "amount init",
		"amount processed", "currency",
		"status", "ps code", "ps message", "result code",
		"result message", "created at", "updated at",
	}
}
