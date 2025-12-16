package processing_merchant

import (
	"app/config"
	"app/logs"
	"app/util"
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

				if operation.IsTestId > 0 {
					continue
				}

				operation.mu.Lock()

				if operation.Tariff != nil {
					operation.SetBalanceAmount()
					operation.SetSRAmount()
					operation.SetDK()
				}

				operation.SetRR()
				operation.SetSRReferal()
				operation.SetHoldAmount()

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

	logs.Add(logs.INFO, fmt.Sprintf("Расчёт комиссии: %v [check fee: %s]", util.FormatDuration(time.Since(start_time)), util.FormatInt(check_fee_counter)))

}

// func HandleHolds() {

// 	// if len(holds.Data) == 0 {
// 	// 	return
// 	// }

// 	for i := range storage.Registry {
// 		operation := storage.Registry[i]

// 		hold, ok := holds.FindHoldForOperation(operation.Balance_currency, operation.Transaction_completed_at)
// 		if !ok {
// 			continue
// 		}

// 		operation.mu.Lock()

// 		operation.Hold = hold
// 		operation.SetHold()

// 		operation.mu.Unlock()

// 	}

// }

// func SelectTariffsInRegistry() {

// 	start_time := time.Now()

// 	var wg sync.WaitGroup

// 	channel_indexes := make(chan int, 10000)

// 	var countWithoutTariff int64

// 	wg.Add(config.NumCPU)
// 	for i := 1; i <= config.NumCPU; i++ {
// 		go func() {
// 			defer wg.Done()
// 			for index := range channel_indexes {
// 				operation := storage.Registry[index]

// 				if operation.IsTestId > 0 {
// 					continue
// 				}

// 				operation.Tariff = tariff_merchant.FindTariffForOperation(operation)
// 				if operation.Tariff == nil {
// 					atomic.AddInt64(&countWithoutTariff, 1)
// 				}

// 				if operation.IsDragonPay {
// 					operation.ClassicTariffDragonPay = true
// 					operation.Tariff_dragonpay_mid = tariff_merchant.FindTariffForOperation(operation)
// 				}

// 			}
// 		}()
// 	}

// 	for i := range storage.Registry {
// 		channel_indexes <- i
// 	}
// 	close(channel_indexes)

// 	wg.Wait()

// 	logs.Add(logs.INFO, fmt.Sprintf("Подбор тарифов: %v [без тарифов: %s]", time.Since(start_time), util.FormatInt(countWithoutTariff)))

// }
