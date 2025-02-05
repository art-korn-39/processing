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

type base_operation struct {
	operation_id       string
	payment_id         string
	bof_operation      *Bof_operation
	provider_operation *pg.Operation
	record             []string

	map_fields map[string]int
	setting    *Setting
}

func createRawOperation(record []string, map_fields map[string]int, setting *Setting) (base_op *base_operation, err error) {

	base_op = &base_operation{record: record, map_fields: map_fields, setting: setting}

	switch setting.Key_column {
	case OPID:
		base_op.operation_id = base_op.getValue("operation_id")
	case PAYID:
		base_op.payment_id = base_op.getValue("provider_payment_id")
	default:
		return nil, fmt.Errorf("неизвестный ключ для стыковки: %s", setting.Key_column)
	}

	return base_op, nil

}

func (ext_op *base_operation) createProviderOperation() (op *pg.Operation, err error) {

	op = &pg.Operation{}

	if ext_op.operation_id != "" {
		op.Id, err = strconv.Atoi(ext_op.operation_id)
	} else {
		op.Id, err = strconv.Atoi(ext_op.getValue("operation_id"))
	}
	if err != nil {
		return nil, fmt.Errorf("field: operation_id - %v", err)
	}

	if ext_op.payment_id != "" {
		op.Provider_payment_id = ext_op.payment_id
	} else {
		op.Provider_payment_id = ext_op.getValue("provider_payment_id")
	}

	op.Transaction_completed_at, err = time.Parse(time.DateTime, ext_op.getValue("transaction_completed_at"))
	if err != nil {
		return nil, err
	}

	op.Transaction_created_at, err = time.Parse(time.DateTime, ext_op.getValue("transaction_created_at"))
	if err != nil {
		return nil, err
	}

	op.Channel_amount, err = util.ParseFloat(ext_op.getValue("channel_amount"))
	if err != nil {
		return nil, err
	}

	op.Amount, err = util.ParseFloat(ext_op.getValue("amount"))
	if err != nil {
		return nil, err
	}

	op.BR_amount, err = util.ParseFloat(ext_op.getValue("br_amount"))
	if err != nil {
		return nil, err
	}

	op.Provider_name = ext_op.getValue("provider_name")
	op.Merchant_name = ext_op.getValue("merchant_name")
	op.Merchant_account_name = ext_op.getValue("merchant_account_name")
	op.Operation_type = ext_op.getValue("operation_type")
	op.Payment_type = ext_op.getValue("payment_method_type")
	op.Country = ext_op.getValue("country")
	op.Account_number = ext_op.getValue("account_number")
	op.Channel_currency_str = ext_op.getValue("channel_currency")
	op.Provider_currency_str = ext_op.getValue("provider_currency")
	op.Balance = ext_op.getValue("balance")
	op.Provider1c = ext_op.getValue("provider1c")
	op.Project_id, _ = strconv.Atoi(ext_op.getValue("project_id"))

	op.Rate, err = util.ParseFloat(ext_op.getValue("rate"))
	if err != nil {
		return nil, err
	}

	op.StartingFill(false)

	ext_op.provider_operation = op

	return op, nil

}

func (base_op *base_operation) getValue(reg_name string) (result string) {

	mapping := base_op.setting.values[reg_name]
	float_names := []string{"amount", "channel_amount", "br_amount", "rate"}

	bof_op := Bof_operation{Operation_id: "0"}
	if base_op.bof_operation != nil {
		bof_op = *base_op.bof_operation
	}

	if mapping.Skip {
		if slices.Contains(float_names, reg_name) {
			result = "0"
		}
	} else if mapping.From_bof {
		switch reg_name {
		case "operation_id":
			result = bof_op.Operation_id
		case "provider_name":
			result = bof_op.Provider_name
		case "merchant_account_name":
			result = bof_op.Merchant_account_name
		case "merchant_name":
			result = bof_op.Merchant_name
		case "operation_type":
			result = bof_op.Operation_type
		case "payment_method_type":
			result = bof_op.Payment_type
		case "provider_payment_id":
			result = bof_op.Provider_payment_id
		case "country":
			result = bof_op.Country_code2
		case "project_id":
			result = fmt.Sprint(bof_op.Project_id)
		case "transaction_completed_at":
			result = bof_op.Transaction_completed_at.Format(time.DateTime)
		case "transaction_created_at":
			result = bof_op.Transaction_created_at.Format(time.DateTime)
		case "channel_amount":
			result = strconv.FormatFloat(bof_op.Channel_amount, 'f', -1, 64)
		case "channel_currency":
			result = bof_op.Channel_currency.Name
		}
	} else if mapping.External_source {
		switch reg_name {
		case "balance":
			result = getBalance(base_op.record, base_op.map_fields, bof_op)
			balances[bof_op] = result
		case "provider1c":
			result = getProvider1c(bof_op)
		case "provider_currency":
			result = getProviderCurrency(bof_op)
		}
	} else if mapping.Calculated {
		switch reg_name {
		case "rate":
			result = "0"
		}
	} else {
		idx := base_op.map_fields[mapping.Table_column] - 1
		if mapping.Date_format != "" {
			date_str := base_op.record[idx]
			t, _ := time.Parse(mapping.Date_format, date_str)
			result = t.Format(time.DateTime)
		} else {
			result = base_op.record[idx]
		}
	}

	return result

}

func getBalance(record []string, map_fields map[string]int, bof_op Bof_operation) (balance_name string) {
	if is_kgx_tradex {
		balance_name = getBalanceByTeamID(record, map_fields)
	} else {
		balance, ok := provider_balances.GetByProvierAndMA(bof_op.Provider_id, bof_op.Merchant_account_id)
		if ok {
			balance_name = balance.Name
		}
	}
	return balance_name
}

func getProvider1c(bof_op Bof_operation) (provider1c string) {

	balance := balances[bof_op]
	provider_currency := getProviderCurrency(bof_op)

	if is_kgx_tradex {

		for _, v := range providers {

			if v.balance != "" && v.balance != balance {
				continue
			}

			if v.currency != "" && v.currency != provider_currency {
				continue
			}

			if v.country != "" && v.country != bof_op.Country_code2 {
				continue
			}

			if v.payment_type != "" && v.payment_type != bof_op.Payment_type {
				continue
			}

			return v.provider1c

		}

	}

	return provider1c
}

func getProviderCurrency(op Bof_operation) (currency string) {

	balance, ok := provider_balances.GetByProvierAndMA(op.Provider_id, op.Merchant_account_id)
	if ok {
		currency = balance.Balance_currency.Name
	}

	return currency

}
