package processing_provider

import (
	"app/config"
	"app/dragonpay"
	"app/logs"
	"app/merchants"
	"app/provider_balances"
	"app/provider_registry"
	"app/providers"
	"app/rr_provider"
	"app/tariff_compensation"
	"app/tariff_provider"
	"app/teams_tradex"
	"app/test_merchant_accounts"
	"app/util"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func FillRefFieldsInRegistry() {

	start_time := time.Now()

	channel_indexes := make(chan int, 1000)

	var wg sync.WaitGroup

	var countWithoutTariff int64

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()

			for index := range channel_indexes {

				op := storage.Registry[index]

				op.mu.Lock()

				// тестовый трафик
				if test_merchant_accounts.Skip(op.Document_date, op.Merchant_account_id, op.Merchant_id, op.Operation_type) {
					op.IsTestId = 1
					op.IsTestType = "live test"
				}

				// заполнение balance_id из clickhouse
				if op.IsTestId == 0 {
					op.SetBalanceID()
				}

				// страна
				op.SetCountry()

				// это tradex
				op.IsTradex = providers.Is_tradex(op.Provider_id)

				if op.Provider_id == 35802 && op.Real_provider != "ps-tradex" {
					op.IsTradex = false
				}

				// операция провайдера
				op.ProviderOperation, _ = provider_registry.GetOperation(op.Operation_id, op.Document_date, op.Channel_amount)

				// баланс провайдера
				if op.IsTradex && op.ProviderOperation != nil {
					team_name := op.ProviderOperation.Team
					team_ref, ok := teams_tradex.GetTeamByName(team_name)
					if ok {
						op.ProviderBalance, _ = provider_balances.GetBalanceByGUID(team_ref.Balance_guid)
					}
				} else {
					op.ProviderBalance, _ = provider_balances.GetBalance(op, "")
				}

				// валюта баланса
				op.SetBalanceCurrency()

				if !op.IsPerevodix {

					// dragonpay: payment_type
					if op.IsDragonPay {
						op.DragonpayOperation = dragonpay.GetOperation(op.Operation_id)
						if op.DragonpayOperation == nil {
							op.DragonpayOperation = dragonpay.PSQL_get_operation(storage.Postgres, op.Operation_id)
						}
						if op.DragonpayOperation != nil {
							op.Payment_type, op.Payment_type_id = dragonpay.GetPaymentType(op.DragonpayOperation.Endpoint_id)
						} else {
							op.Payment_type, op.Payment_type_id = dragonpay.GetPaymentType(op.Endpoint_id)
						}
					}

					// тариф
					op.Tariff = tariff_provider.FindTariffForOperation(op, "Provider_balance_guid")
					if op.Tariff == nil {
						atomic.AddInt64(&countWithoutTariff, 1)
					}

					// дополнительный тариф
					op.Extra_tariff = tariff_provider.FindTariffForOperation(op, "Extra_balance_guid")

				}

				// мерчант
				op.Merchant, _ = merchants.GetByProjectID(op.Project_id)

				// поставщик 1С
				op.SetProvider1c()

				// РР провайдера
				op.RR_provider = rr_provider.FindRRForOperation(op)

				// подбор тарифа компенсации
				op.Tariff_compensation = tariff_compensation.FindTariffForOperation(op, false, false)

				op.mu.Unlock()

			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Заполнение ссылок в операциях: %v [без тарифов: %s]", util.FormatDuration(time.Since(start_time)), util.FormatInt(countWithoutTariff)))

}

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

func SelectTariffs_() {

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

				operation.Tariff = tariff_provider.FindTariffForOperation(operation, "Provider_balance_guid")
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
