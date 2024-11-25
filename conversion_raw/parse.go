package conversion_raw

import (
	pg "app/provider_registry"
	"fmt"
	"slices"
	"strconv"
	"time"
)

const (
	OPID  = "operation_id"
	PAYID = "provider_payment_id"
)

type ext_operation struct {
	operation_id  string
	payment_id    string
	bof_operation *Bof_operation
	record        []string
}

func createExtOperation(record []string, map_fields map[string]int, setting Setting) (op *ext_operation, err error) {

	op = &ext_operation{}

	switch setting.Key_column {
	case OPID:
		op.operation_id = getValue("operation_id", record, setting, map_fields, Bof_operation{})
		// if err != nil {
		// 	return nil, err
		// }
	case PAYID:
		op.payment_id = getValue("provider_payment_id", record, setting, map_fields, Bof_operation{})
	default:
		return nil, fmt.Errorf("неизвестный ключ для стыковки: %s", setting.Key_column)
	}
	op.record = record

	return op, nil

}

func (ext_op *ext_operation) createProviderOperation(map_fields map[string]int, setting Setting) (op *pg.Operation, err error) {

	op = &pg.Operation{}

	if ext_op.operation_id != "" {
		op.Id, err = strconv.Atoi(ext_op.operation_id)
	} else {
		op.Id, err = strconv.Atoi(getValue("operation_id", ext_op.record, setting, map_fields, *ext_op.bof_operation))
	}
	if err != nil {
		return nil, fmt.Errorf("field: operation_id - %v", err)
	}

	if ext_op.payment_id != "" {
		op.Provider_payment_id = ext_op.payment_id
	} else {
		op.Provider_payment_id = getValue("provider_payment_id", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	}

	op.Transaction_completed_at, err = time.Parse(time.DateTime, getValue("transaction_completed_at", ext_op.record, setting, map_fields, *ext_op.bof_operation))
	if err != nil {
		return nil, err
	}

	op.Channel_amount, err = strconv.ParseFloat(getValue("channel_amount", ext_op.record, setting, map_fields, *ext_op.bof_operation), 64)
	if err != nil {
		return nil, err
	}

	op.Amount, err = strconv.ParseFloat(getValue("amount", ext_op.record, setting, map_fields, *ext_op.bof_operation), 64)
	if err != nil {
		return nil, err
	}

	op.BR_amount, err = strconv.ParseFloat(getValue("br_amount", ext_op.record, setting, map_fields, *ext_op.bof_operation), 64)
	if err != nil {
		return nil, err
	}

	op.Provider_name = getValue("provider_name", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Merchant_name = getValue("merchant_name", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Merchant_account_name = getValue("merchant_account_name", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Operation_type = getValue("operation_type", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Payment_type = getValue("payment_method_type", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Country = getValue("country", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Operation_status = getValue("operation_status", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Account_number = getValue("account_number", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Channel_currency_str = getValue("channel_currency", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Provider_currency_str = getValue("provider_currency", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Balance = getValue("balance", ext_op.record, setting, map_fields, *ext_op.bof_operation)
	op.Provider1c = getValue("provider1c", ext_op.record, setting, map_fields, *ext_op.bof_operation)

	op.Rate, err = strconv.ParseFloat(getValue("rate", ext_op.record, setting, map_fields, *ext_op.bof_operation), 64)
	if err != nil {
		return nil, err
	}

	op.StartingFill(false)

	return op, nil

}

func getValue(reg_name string, record []string, setting Setting, map_fields map[string]int, op Bof_operation) (result string) {

	mapping := setting.values[reg_name]
	float_names := []string{"amount", "channel_amount", "br_amount", "rate"}

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
		case "transaction_completed_at":
			result = op.Created_at.Format(time.DateTime)
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
