package rr_provider

func FindRRForOperation(op Operation) *Tariff {

	operation_date := op.GetTime("Transaction_completed_at")
	merchant_account_id := op.GetInt("Merchant_account_id")
	provider_id := op.GetInt("Provider_id")
	operation_type := op.GetString("Operation_type")
	balance_guid := op.GetString("Provider_balance_guid")

	if operation_type != "sale" {
		return nil
	}

	for _, rr := range data {

		if rr.DateStart.IsZero() {
			continue
		}

		if rr.Merchant_account_id == merchant_account_id &&
			rr.DateStart.Before(operation_date) &&
			(rr.DateFinish.After(operation_date) || rr.DateFinish.IsZero()) &&
			rr.Provider_id == provider_id &&
			rr.Balance_guid == balance_guid {

			return rr
		}

	}

	return nil
}
