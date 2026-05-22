package tariff_compensation

import (
	"sort"
	"time"

	"github.com/jmoiron/sqlx"
)

var data []*Tariff

func Read_Sources(db *sqlx.DB, processing_merchant bool) {

	Read_PSQL_Tariffs(db, processing_merchant)

}

func SortTariffs() {
	sort.Slice(
		data,
		func(i int, j int) bool {
			return data[i].DateStart.After(data[j].DateStart)
		},
	)
}

// в импорте мерчанта отбор по merchant_id, provider_id (используются оба варианта is_referal)
// в импорте провайдера отбор по provider_id (используется только is_referal = false)
func FindTariffForOperation(op Operation, processing_merchant, is_referal bool) *Tariff {

	var operation_date time.Time
	if processing_merchant && op.GetBool("IsPerevodix") {
		operation_date = op.GetTime("Operation_created_at")
	} else {
		operation_date = op.GetTime("Transaction_completed_at")
	}

	provider_balance_guid := op.GetString("Provider_balance_guid")
	if provider_balance_guid == "" {
		return nil
	}

	merchant_id := op.GetInt("Merchant_id")
	provider_id := op.GetInt("Provider_id")
	channel_amount := op.GetFloat("Channel_amount")
	operation_group := op.GetString("Operation_group")
	balance_currency := op.GetString("Balance_currency")

	for _, t := range data {

		if t.DateStart.IsZero() {
			continue
		}

		if t.Is_referal == is_referal &&
			(!processing_merchant || t.Merchant_id == merchant_id) &&
			t.DateStart.Before(operation_date) && // добавил 04.12.25
			(t.DateFinish.After(operation_date) || t.DateFinish.IsZero()) &&
			(provider_balance_guid == t.Provider_balance_guid || t.Provider_balance_guid == "") &&
			(provider_id == t.Provider_id || t.Provider_id == 0) &&
			(balance_currency == t.Currency.Name || t.Currency.Name == "") &&
			t.Operation_group == operation_group {

			// проверяем наличие диапазона
			if t.RangeMIN != 0 || t.RangeMAX != 0 {

				// определелям попадание в диапазон тарифа если он заполнен
				if channel_amount > t.RangeMIN &&
					channel_amount <= t.RangeMAX {
					return t
				}

			} else {
				return t
			}

		}
	}

	return nil
}
