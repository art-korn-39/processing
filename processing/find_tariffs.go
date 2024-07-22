package processing

import (
	"app/config"
	"app/logs"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func SelectTariffsInRegistry() {

	start_time := time.Now()

	var wg sync.WaitGroup

	channel_indexes := make(chan int, 10000)

	var countWithoutTariff int64

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for index := range channel_indexes {
				operation := storage.Registry[index]
				operation.Crypto_network = storage.Crypto[operation.Operation_id].Network
				operation.Tariff = FindTariffForOperation(operation)
				if operation.Tariff == nil {
					atomic.AddInt64(&countWithoutTariff, 1)
				}
			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Подбор тарифов: %v [без тарифов: %d]", time.Since(start_time), countWithoutTariff))

}

func FindTariffForOperation(op *Operation) *Tariff {

	var operation_date time.Time
	if op.IsPerevodix {
		operation_date = op.Operation_created_at
	} else {
		operation_date = op.Transaction_completed_at
	}

	for _, t := range storage.Tariffs {

		if t.Merchant_account_id == op.Merchant_account_id {

			if t.DateStart.Before(operation_date) &&
				t.Operation_type == op.Operation_type {

				if t.IsCrypto && op.Crypto_network != t.Convertation {
					continue
				}

				// проверяем наличие диапазона
				if t.RangeMIN != 0 || t.RangeMAX != 0 {

					// определелям попадание в диапазон тарифа если он заполнен
					if op.Channel_amount > t.RangeMIN &&
						op.Channel_amount <= t.RangeMAX {
						return &t
					}

				} else {
					return &t
				}

			}
		}
	}

	return nil
}
