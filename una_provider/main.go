package una_provider

func FindUNAForOperation(op Operation) *Tariff {

	operation_date := op.GetTime("Transaction_completed_at")
	provider_id := op.GetInt("Provider_id")
	operation_group := op.GetString("Operation_group")
	balance_guid := op.GetString("Provider_balance_guid")

	if operation_group != "IN" {
		return nil
	}

	for _, una := range data {

		if una.DateStart.IsZero() {
			continue
		}

		if una.DateStart.Before(operation_date) &&
			(una.DateFinish.After(operation_date) || una.DateFinish.IsZero()) &&
			una.Provider_id == provider_id &&
			una.Balance_guid == balance_guid {

			return una
		}

	}

	return nil
}
