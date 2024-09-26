package processing_merchant

import (
	"app/config"
	"app/crypto"
	"app/holds"
	"app/logs"
	"app/provider"
	"app/tariff_merchant"
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
				operation.Crypto_network = crypto.Registry[operation.Operation_id].Network
				operation.Tariff = tariff_merchant.FindTariffForOperation(operation)
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

	logs.Add(logs.INFO, fmt.Sprintf("Подбор тарифов: %v [без тарифов: %s]", time.Since(start_time), util.FormatInt(countWithoutTariff)))

}

func CalculateCommission() {

	start_time := time.Now()

	channel_indexes := make(chan int, 1000)

	var wg sync.WaitGroup

	var check_fee_counter int64

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()

			for index := range channel_indexes {

				operation := storage.Registry[index]

				operation.mu.Lock()

				if operation.Tariff != nil {
					operation.SetBalanceAmount()
					operation.SetSRAmount()
					operation.SetRR()
					operation.SetDK()
					operation.SetProvider1c()
				}

				operation.SetCheckFee()
				operation.SetVerification()

				operation.mu.Unlock()

				if operation.CheckFee != 0 {
					atomic.AddInt64(&check_fee_counter, 1)
				}
			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Расчёт комиссии: %v [check fee: %s]", time.Since(start_time), util.FormatInt(check_fee_counter)))

}

func FindRateForOperation(o *Operation) float64 {

	for _, r := range provider.Rates {

		if r.Transaction_completed_at.Before(o.Transaction_completed_at) &&
			r.Operation_type == o.Operation_type &&
			r.Country == o.Country &&
			r.Payment_type == o.Payment_type &&
			r.Merchant_name == o.Merchant_name &&
			r.Channel_currency == o.Channel_currency &&
			r.Provider_currency == o.Tariff.CurrencyBP {
			return r.Rate
		}
	}

	return 0

}

func HandleHolds() {

	if len(holds.Data) == 0 {
		return
	}

	for i := range storage.Registry {
		operation := storage.Registry[i]

		hold, ok := FindHoldForOperation(operation)
		if !ok {
			continue
		}

		operation.mu.Lock()

		operation.Hold = hold
		operation.SetHold()

		operation.mu.Unlock()

	}

}

func FindHoldForOperation(op *Operation) (*holds.Hold, bool) {

	for _, h := range holds.Data {

		if h.Currency == op.Balance_currency && h.DateStart.Before(op.Transaction_completed_at) {
			return &h, true
		}

	}

	return nil, false
}