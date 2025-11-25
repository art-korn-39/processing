package processing_provider

import (
	"app/config"
	"app/dragonpay"
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

func SetCountries() {

	for _, o := range storage.Registry {
		o.SetCountry()
	}

}

func SetProviders() {

	start_time := time.Now()

	var countWithout int

	for _, o := range storage.Registry {
		ProviderOperation, ok := provider_registry.GetOperation(o.Operation_id, o.Document_date, o.Channel_amount)
		if ok {
			o.ProviderOperation = ProviderOperation
			if ProviderOperation.Team != "" {
				o.IsTradex = true
			}
		} else {
			countWithout++
		}
	}

	logs.Add(logs.INFO, fmt.Sprintf("Подбор операций из реестра провайдера: %v [не найдено: %s]", time.Since(start_time), util.FormatInt(countWithout)))

}

func SetBalances() {

	start_time := time.Now()

	var countWithout int

	for _, operation := range storage.Registry {

		// для операции с конвертом = "реестр" должна быть операция в "Provider_registry"
		currency := operation.Channel_currency.Name
		if operation.ProviderOperation != nil {
			currency = operation.ProviderOperation.Provider_currency.Name
		} else if operation.Provider_amount > 0 {
			currency = ""
		}

		var balance *provider_balances.Balance
		var ok bool
		if operation.IsTradex && operation.ProviderOperation != nil {
			balance, ok = provider_balances.GetBalanceByNickname(operation.ProviderOperation.Balance)
		} else {
			balance, ok = provider_balances.GetBalance(operation, currency)
		}

		if ok {
			operation.ProviderBalance = balance
		} else {

			// это для "без конвертации" и валюты канала = USD
			if operation.Channel_currency.Name == "USD" {
				balance, ok = provider_balances.GetBalance(operation, "USDT")
				if ok && balance.Convertation_id == CNV_NO_CONVERT {
					operation.ProviderBalance = balance
				}
			}

			// для колбэка условие, если баланс так и не нашли
			if operation.ProviderBalance == nil && operation.Provider_amount == 0 {
				balance, ok = provider_balances.GetBalance(operation, "")
				if ok && balance.Convertation_id == CNV_CALLBACK {
					operation.ProviderBalance = balance
				}
			}
		}

		if operation.ProviderBalance == nil {
			countWithout++
		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Подбор балансов к операциям: %v [без баланса: %d]", time.Since(start_time), countWithout))

}

func SetBalanceCurrencies() {

	for _, o := range storage.Registry {

		if o.ProviderOperation != nil {
			o.Balance_currency = o.ProviderOperation.Provider_currency

		} else if o.ProviderBalance != nil &&
			(o.ProviderBalance.Convertation_id == CNV_CALLBACK ||
				o.ProviderBalance.Convertation_id == CNV_REESTR) {

			o.Balance_currency = o.ProviderBalance.Balance_currency

		} else if o.ProviderBalance != nil &&
			o.ProviderBalance.Convertation_id == CNV_NO_CONVERT &&
			o.Channel_currency.Name == "USD" {

			o.Balance_currency = o.ProviderBalance.Balance_currency

		} else {
			o.Balance_currency = o.Channel_currency
		}

	}

}

func SelectTariffs() {

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

				if operation.IsPerevodix {
					continue
				}

				if operation.IsDragonPay {
					operation.DragonpayOperation = dragonpay.GetOperation(operation.Operation_id)
					if operation.DragonpayOperation != nil {
						operation.Payment_type, operation.Payment_type_id = dragonpay.GetPaymentType(operation.DragonpayOperation.Endpoint_id)
					} else {
						operation.Payment_type, operation.Payment_type_id = dragonpay.GetPaymentType(operation.Endpoint_id)
					}
				}

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

func SetMerchants() {

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

func SetProvider1C() {

	//start_time := time.Now()

	//var countWithout int

	for _, operation := range storage.Registry {
		operation.SetProvider1c()
	}

	//logs.Add(logs.INFO, fmt.Sprintf("Подбор мерчантов к операциям: %v [не найдено: %d]", time.Since(start_time), countWithout))

}
