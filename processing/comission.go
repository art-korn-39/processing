package processing

import (
	"app/logs"
	"fmt"
	"time"
)

func CalculateCommission() {

	start_time := time.Now()

	var check_fee_counter int

	for _, operation := range storage.Registry {

		if operation.Tariff != nil {
			operation.SetBalanceAmount()
			operation.SetSRAmount()
		}

		operation.SetCheckFee()
		operation.SetVerification()

		if operation.CheckFee != 0 {
			check_fee_counter++
		}
	}

	logs.Add(logs.INFO, fmt.Sprintf("Расчёт комиссии: %v [check fee: %d]", time.Since(start_time), check_fee_counter))

}

func FindRateForOperation(o *Operation) float64 {

	for _, r := range storage.Rates {

		if r.Transaction_completed_at.Before(o.Transaction_completed_at) &&
			r.Operation_type == o.Operation_type &&
			r.Country == o.Country &&
			r.Payment_type == o.Payment_type &&
			r.Merchant_name == o.Merchant_name &&
			r.Channel_currency == o.Channel_currency &&
			r.Provider_currency == o.Tariff.CurrencyBP {
			return r.Rate
		}
	}

	return 0

}
