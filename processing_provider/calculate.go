package processing_provider

import (
	"app/config"
	"app/logs"
	"app/merchants"
	"app/provider_balances"
	"app/provider_registry"
	"app/tariff_provider"
	"app/util"
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

				operation.Tariff = tariff_provider.FindTariffForOperation(operation, "Balance_guid")
				if operation.Tariff == nil {
					atomic.AddInt64(&countWithoutTariff, 1)
				}

				operation.Extra_tariff = tariff_provider.FindTariffForOperation(operation, "Extra_balance_guid")
			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Подбор тарифов: %v [без тарифов: %s]", time.Since(start_time), util.FormatInt(countWithoutTariff)))

}

func CalculateCommission() {

	start_time := time.Now()

	channel_indexes := make(chan int, 1000)

	var wg sync.WaitGroup

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()

			for index := range channel_indexes {

				operation := storage.Registry[index]

				operation.mu.Lock()

				operation.SetCountry()

				operation.SetBalanceAmount()
				operation.SetBRAmount()
				operation.SetExtraBRAmount()

				operation.SetCheckFee()
				operation.SetVerification()

				operation.mu.Unlock()

			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Расчёт комиссии: %v", time.Since(start_time)))

}

func SetProviderOperations() {

	start_time := time.Now()

	var countWithout int

	for _, o := range storage.Registry {
		ProviderOperation, ok := provider_registry.GetOperation(o.Operation_id, o.Document_date, o.Channel_amount)
		if ok {
			o.ProviderOperation = ProviderOperation
		} else {
			countWithout++
		}
	}

	logs.Add(logs.INFO, fmt.Sprintf("Подбор операций из реестра провайдера: %v [не найдено: %d]", time.Since(start_time), countWithout))

}

func SetBalanceCurrencyInOperations() {

	for _, operation := range storage.Registry {
		operation.SetBalanceCurrency()
	}

}

func SetBalanceInOperations() {

	start_time := time.Now()

	var countWithout int

	for _, operation := range storage.Registry {

		currency := operation.Channel_currency.Name
		if operation.ProviderOperation != nil {
			currency = operation.ProviderOperation.Provider_currency.Name
		}

		balance, ok := provider_balances.GetBalance(operation, currency)
		if ok {
			operation.ProviderBalance = balance
		} else {
			countWithout++
		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Подбор балансов к операциям: %v [без баланса: %d]", time.Since(start_time), countWithout))

}

func SetMerchantInOperations() {

	start_time := time.Now()

	var countWithout int

	for _, operation := range storage.Registry {
		merchant, ok := merchants.GetByProjectID(operation.Project_id)
		if ok {
			operation.Merchant = merchant
		} else {
			countWithout++
		}
	}

	logs.Add(logs.INFO, fmt.Sprintf("Подбор мерчантов к операциям: %v [не найдено: %d]", time.Since(start_time), countWithout))

}
