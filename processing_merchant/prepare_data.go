package processing_merchant

import (
	"app/config"
	"app/countries"
	"app/crypto"
	"app/dragonpay"
	"app/holds"
	"app/logs"
	"app/provider_balances"
	"app/provider_registry"
	"app/providers"
	"app/rr_merchant"
	"app/tariff_compensation"
	"app/tariff_merchant"
	"app/teams_tradex"
	"app/test_merchant_accounts"
	"app/util"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func PrepareData() {

	// var wg sync.WaitGroup

	// wg.Add(2)

	// // 2. Тарифы
	// go func() {
	// 	defer wg.Done()

	// Сортировка тарифов
	tariff_merchant.SortTariffs()

	// Сортировка холдов
	holds.Sort()

	// Заполнение ссылочных полей
	FillRefFieldsInRegistry()

	// Подбор тарифов к операциям
	// SelectTariffsInRegistry()
	//}()

	// // 2. Курсы валют
	// go func() {
	// 	defer wg.Done()

	// Группировка курсов валют
	provider_registry.GroupRates()

	// Сортировка курсов валют
	provider_registry.SortRates()

	// }()

	// wg.Wait()

}

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

				// dragonpay: payment_type, provider1c
				if op.IsDragonPay {
					op.DragonpayOperation = dragonpay.GetOperation(op.Operation_id)
					if op.DragonpayOperation == nil {
						op.DragonpayOperation = dragonpay.PSQL_get_operation(storage.Postgres, op.Operation_id)
					}
					endpoint_id := op.Endpoint_id
					if op.DragonpayOperation != nil {
						endpoint_id = op.DragonpayOperation.Endpoint_id
					}
					op.Payment_type, op.Payment_type_id = dragonpay.GetPaymentType(endpoint_id)
					op.Provider1c = dragonpay.GetProvider1C(endpoint_id)
				}

				// крипто
				if op.IsCrypto {
					op.CryptoOperation = crypto.GetOperation(op.Operation_id)
					if op.CryptoOperation == nil {
						op.CryptoOperation = crypto.PSQL_get_operation(storage.Postgres, op.Operation_id)
					}
				}

				// операция провайдера
				op.ProviderOperation, _ = provider_registry.GetOperation(op.Operation_id, op.Document_date, op.Channel_amount)

				// это tradex
				op.IsTradex = providers.Is_tradex(op.Provider_id)

				if op.Provider_id == 35802 && op.Real_provider != "ps-tradex" {
					op.IsTradex = false
				}

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

				// страна
				op.Country = countries.GetCountry(op.Country_code2, op.Channel_currency.Name)

				// связанная операция из таблицы detailed_provider
				op.Detailed_provider = data_detailed_provider[op.Operation_id]

				// подбор тарифов
				if op.IsTestId == 0 {

					op.Tariff = tariff_merchant.FindTariffForOperation(op)
					if op.Tariff == nil {
						atomic.AddInt64(&countWithoutTariff, 1)
					}

					if op.IsDragonPay {
						op.ClassicTariffDragonPay = true
						op.Tariff_dragonpay_mid = tariff_merchant.FindTariffForOperation(op)
					}

				}

				// валюта баланса
				op.SetBalanceCurrency()

				// поставщик 1С
				op.SetProvider1c()

				// подбор тарифа реферала
				op.Tariff_referal = tariff_compensation.FindTariffForOperation(op, true, true)

				// холд
				op.Hold, _ = holds.FindHoldForOperation(op.Balance_currency, op.Transaction_completed_at)

				// РР мерчанта
				op.RR_merchant = rr_merchant.FindRRForOperation(op)

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
