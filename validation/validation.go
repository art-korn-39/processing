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
		res[strings.ToLower(cell.String())] = i + 1
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
	case "kgx":
		s = fields_kgx()
	case "crypto":
		s = fields_crypto()
	case "dragonpay_csv":
		s = fields_dragonpay_csv()
	case "dragonpay_xlsx":
		s = fields_dragonpay_xlsx()
	case "provider_registry":
		s = fields_provider_registry()
	case "provider_registry_rub":
		s = fields_provider_registry_rub()
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
		"contract_id", "tariff_condition_id",
		"real_currency / channel_currency", "real_amount / channel_amount",
		"fee_currency", "fee_amount",
		"provider_currency", "provider_amount",
		"tariff_rate_percent", "tariff_rate_fix", "tariff_rate_min", "tariff_rate_max",
	}
}

func fields_bof_registry_provider() []string {
	return []string{
		"id / operation_id", "transaction_id", "transaction_completed_at",
		"merchant_id", "merchant_account_id", "project_id", "project_name",
		"provider_name", "merchant_name", "merchant_account_name",
		"acquirer_id / provider_payment_id", "issuer_country",
		"operation_type", "payment_type_id / payment_method_type",
		"contract_id",
		"real_currency / channel_currency", "real_amount / channel_amount",
		"provider_currency", "provider_amount",
		"legal_entity_id", "business_type",
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
		"provider", "юл", "provider name", "date of start", "merchant_name", "merchant account", "merchant legal entity",
		"payment method", "payment method type", "region", "channel currency", "project", "business type",
		"operation type (группа)", "tariff range turnouver min", "tariff range turnouver max",
		"tariff range amount min", "tariff range amount max", "percent", "fix", "min commission", "max commission",
	}
}

func fields_holds() []string {
	return []string{
		"схема", "валюта", "ma_id", "ma_name", "дата старта", "процент холда", "кол-во дней",
	}
}

func fields_kgx() []string {
	return []string{
		"баланс", "operation_type", "валюта баланса", "payment_type_id / payment_method_type", "поставщик 1с",
	}
}

func fields_crypto() []string {
	return []string{"operation id", "crypto network", "created at", "operation type",
		"payment amount", "payment currency", "crypto amount", "crypto currency"}
}

func fields_dragonpay_csv() []string {
	return []string{"merchant txn id", "create date", "settle date", "refno",
		"ccy", "amount", "proc", "fee"}
}

func fields_dragonpay_xlsx() []string {
	return []string{"endpoint_id", "поставщик dragonpay"}
}

func fields_provider_registry() []string {
	return []string{
		"id / operation_id", "transaction_completed_at",
		"operation_type", "issuer_country",
		"payment_type_id / payment_method_type",
		"merchant_name", "real_currency / channel_currency", "real_amount / channel_amount",
		"provider_currency", "курс", "provider_amount",
		"provider_name", "merchant_account_name", "acquirer_id / provider_payment_id",
		"project_url", "operation_status",
	}
}

func fields_provider_registry_rub() []string {
	return []string{
		"id / operation_id", "transaction_completed_at",
		"operation_type", "issuer_country",
		"payment_type_id / payment_method_type",
		"merchant_name", "real_currency / channel_currency", "real_amount / channel_amount",
		"provider_name", "merchant_account_name", "acquirer_id / provider_payment_id",
		"project_url", "operation_status",
	}
}
