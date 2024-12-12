package conversion_raw

import (
	"app/provider_balances"
	pg "app/provider_registry"
	"app/util"
	"fmt"
	"slices"
	"strconv"
	"time"
)

const (
	OPID  = "operation_id"
	PAYID = "provider_payment_id"
)

type raw_operation struct {
	operation_id       string
	payment_id         string
	bof_operation      *Bof_operation
	provider_operation *pg.Operation
	record             []string
}

func createRawOperation(record []string, map_fields map[string]int, setting Setting) (op *raw_operation, err error) {

	op = &raw_operation{}

	switch setting.Key_column {
	case OPID:
		op.operation_id = getValue("operation_id", record, setting, map_fields, &Bof_operation{})
	case PAYID:
		op.payment_id = getValue("provider_payment_id", record, setting, map_fields, &Bof_operation{})
	default:
		return nil, fmt.Errorf("неизвестный ключ для стыковки: %s", setting.Key_column)
	}
	op.record = record

	return op, nil

}

func (ext_op *raw_operation) createProviderOperation(map_fields map[string]int, setting Setting) (op *pg.Operation, err error) {

	op = &pg.Operation{}

	if ext_op.operation_id != "" {
		op.Id, err = strconv.Atoi(ext_op.operation_id)
	} else {
		op.Id, err = strconv.Atoi(getValue("operation_id", ext_op.record, setting, map_fields, ext_op.bof_operation))
	}
	if err != nil {
		return nil, fmt.Errorf("field: operation_id - %v", err)
	}

	if ext_op.payment_id != "" {
		op.Provider_payment_id = ext_op.payment_id
	} else {
		op.Provider_payment_id = getValue("provider_payment_id", ext_op.record, setting, map_fields, ext_op.bof_operation)
	}

	op.Transaction_completed_at, err = time.Parse(time.DateTime, getValue("transaction_completed_at", ext_op.record, setting, map_fields, ext_op.bof_operation))
	if err != nil {
		return nil, err
	}

	op.Channel_amount, err = util.ParseFloat(getValue("channel_amount", ext_op.record, setting, map_fields, ext_op.bof_operation))
	if err != nil {
		return nil, err
	}

	op.Amount, err = util.ParseFloat(getValue("amount", ext_op.record, setting, map_fields, ext_op.bof_operation))
	if err != nil {
		return nil, err
	}

	op.BR_amount, err = util.ParseFloat(getValue("br_amount", ext_op.record, setting, map_fields, ext_op.bof_operation))
	if err != nil {
		return nil, err
	}

	op.Provider_name = getValue("provider_name", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Merchant_name = getValue("merchant_name", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Merchant_account_name = getValue("merchant_account_name", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Operation_type = getValue("operation_type", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Payment_type = getValue("payment_method_type", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Country = getValue("country", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Operation_status = getValue("operation_status", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Account_number = getValue("account_number", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Channel_currency_str = getValue("channel_currency", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Provider_currency_str = getValue("provider_currency", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Balance = getValue("balance", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Provider1c = getValue("provider1c", ext_op.record, setting, map_fields, ext_op.bof_operation)
	op.Project_url = getValue("project_url", ext_op.record, setting, map_fields, ext_op.bof_operation)

	op.Rate, err = util.ParseFloat(getValue("rate", ext_op.record, setting, map_fields, ext_op.bof_operation))
	if err != nil {
		return nil, err
	}

	op.StartingFill(false)

	ext_op.provider_operation = op

	return op, nil

}

func getValue(reg_name string, record []string, setting Setting, map_fields map[string]int, bof_op *Bof_operation) (result string) {

	mapping := setting.values[reg_name]
	float_names := []string{"amount", "channel_amount", "br_amount", "rate"}

	op := Bof_operation{Operation_id: "0"}
	if bof_op != nil {
		op = *bof_op
	}

	if mapping.Skip {
		if slices.Contains(float_names, reg_name) {
			result = "0"
		}
	} else if mapping.From_bof {
		switch reg_name {
		case "operation_id":
			result = op.Operation_id
		case "provider_name":
			result = op.Provider_name
		case "merchant_account_name":
			result = op.Merchant_account_name
		case "merchant_name":
			result = op.Merchant_name
		case "operation_type":
			result = op.Operation_type
		case "payment_method_type":
			result = op.Payment_type
		case "provider_payment_id":
			result = op.Provider_payment_id
		case "country":
			result = op.Country_code2
		case "operation_status":
			result = op.Status
		case "project_url":
			result = op.Project_url
		case "transaction_completed_at":
			result = op.Created_at.Format(time.DateTime)
		case "channel_amount":
			result = strconv.FormatFloat(op.Channel_amount, 'f', -1, 64)
		case "channel_currency":
			result = op.Channel_currency.Name
		}
	} else if mapping.External_source {
		switch reg_name {
		case "balance":
			result = getBalance(record, map_fields, op)
		case "provider_currency":
			result = getProviderCurrency(op)
		}
	} else if mapping.Calculated {
		switch reg_name {
		case "rate":
			result = "0"
		}
	} else {
		idx := map_fields[mapping.Table_column] - 1
		if mapping.Date_format != "" {
			date_str := record[idx]
			t, _ := time.Parse(mapping.Date_format, date_str)
			result = t.Format(time.DateTime)
		} else {
			result = record[idx]
		}
	}

	return result

}

func getBalance(record []string, map_fields map[string]int, op Bof_operation) (balance_name string) {
	if is_kgx_tradex {
		balance_name = getBalanceByTeamID(record, map_fields)
	} else {
		balance, ok := provider_balances.GetByProvierAndMA(op.Provider_id, op.Merchant_account_id)
		if ok {
			balance_name = balance.Name
		}
	}
	return balance_name
}

func getProviderCurrency(op Bof_operation) (currency string) {

	//balance, ok := provider_balances.GetByMAID(op.Merchant_account_id)
	balance, ok := provider_balances.GetByProvierAndMA(op.Provider_id, op.Merchant_account_id)
	if ok {
		currency = balance.Balance_currency.Name
	}

	return currency

}
