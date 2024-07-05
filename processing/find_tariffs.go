package processing

import (
	"app/config"
	"app/logs"
	"fmt"
	"sync"
	"time"
)

func SelectTariffsInRegistry() {

	if config.Get().Async {
		SelectTariffsInRegistry_async()
		return
	}

	start_time := time.Now()

	var cnt int
	for _, operation := range storage.Registry {
		operation.Crypto_network = storage.Crypto[operation.Operation_id]
		operation.Tariff = FindTariffForOperation(operation)
		if operation.Tariff == nil {
			cnt++
		}
	}

	logs.Add(logs.INFO, fmt.Sprintf("Подбор тарифов: %v [без тарифов: %d]", time.Since(start_time), cnt))

}

func SelectTariffsInRegistry_async() {

	start_time := time.Now()

	var wg1 sync.WaitGroup

	channel_indexes := make(chan int, 10000)

	var cnt int

	wg1.Add(NUM_GORUTINES)
	for i := 1; i <= NUM_GORUTINES; i++ {
		go func() {
			defer wg1.Done()
			for index := range channel_indexes {
				operation := storage.Registry[index]
				operation.Crypto_network = storage.Crypto[operation.Operation_id]
				operation.Tariff = FindTariffForOperation(operation)
				if operation.Tariff == nil {
					mu.Lock()
					cnt++
					mu.Unlock()
				}
			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg1.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Подбор тарифов: %v [без тарифов: %d]", time.Since(start_time), cnt))

}

func FindTariffForOperation(op *Operation) *Tariff {

	for _, t := range storage.Tariffs {

		if t.DateStart.Before(op.Transaction_completed_at) &&
			t.Operation_type == op.Operation_type {

			// dragonpay - ???
			if t.Merchant_account_id == op.Merchant_account_id {

				if t.Schema == "Crypto" && op.Crypto_network != t.Convertation {
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
