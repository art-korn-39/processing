package processing

import (
	"app/config"
	"app/logs"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

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
				if operation.Tariff != nil {
					operation.SetBalanceAmount()
					operation.SetSRAmount()
				}

				operation.SetCheckFee()
				operation.SetVerification()

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

	logs.Add(logs.INFO, fmt.Sprintf("Расчёт комиссии: %v [check fee: %d]", time.Since(start_time), check_fee_counter))

}

func FindRateForOperation(o *Operation) float64 {

	for _, r := range storage.Rates {

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
