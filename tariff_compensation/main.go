package tariff_compensation

import (
	"app/querrys"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"
)

var data []*Tariff

func Read_Sources(db *sqlx.DB, registry_done chan querrys.Args) {

	Read_PSQL_Tariffs(db, registry_done)

}

func SortTariffs() {
	sort.Slice(
		data,
		func(i int, j int) bool {
			return data[i].DateStart.After(data[j].DateStart)
		},
	)
}

func FindTariffForOperation(op Operation, is_referal bool) *Tariff {

	var operation_date time.Time
	if op.GetBool("IsPerevodix") {
		operation_date = op.GetTime("Operation_created_at")
	} else {
		operation_date = op.GetTime("Transaction_completed_at")
	}

	provider_balance_guid := op.GetString("Provider_balance_guid")
	if provider_balance_guid == "" {
		return nil
	}

	opeation_group := op.GetString("Operation_group")

	// is referal = true
	// op group
	// comis type = turnover

	for _, t := range data {

		if t.DateStart.IsZero() {
			continue
		}

		if t.ComissionType != "turnover" {
			return nil
		}

		if t.Is_referal == is_referal &&
			provider_balance_guid == t.Provider_balance_guid &&
			t.Opeation_group == opeation_group &&
			(t.DateFinish.After(operation_date) || t.DateFinish.IsZero()) {

			// проверяем наличие диапазона
			if t.RangeMIN != 0 || t.RangeMAX != 0 {

				// определелям попадание в диапазон тарифа если он заполнен
				channel_amount := op.GetFloat("Channel_amount")
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
