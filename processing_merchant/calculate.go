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

				if operation.IsTestId > IST_LIVE || operation.SkipDecline() || operation.Dupclicate {
					continue
				}

				operation.mu.Lock()

				operation.SetBalanceAmount()

				if operation.Tariff != nil {
					operation.SetSRAmount()
					operation.SetDK()
				}

				operation.SetRR()
				operation.SetUNA()
				operation.SetSRReferal()
				operation.SetSRCompensation()
				operation.SetHoldAmount()

				operation.SetCheckFee()
				operation.SetVerification()

				operation.SetCorrection()
				operation.SetDeclineAmount()

				operation.mu.Unlock()

				addDuplicate(operation)

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

func addDuplicate(o *Operation) {

	if o.CorrectionTypeId != 2 {
		return
	}

	if o.Detailed_merchant == nil {
		return
	}

	copy := *o
	copy.mu = &sync.Mutex{}

	copy.Balance_amount = -o.Detailed_merchant.Balance_amount
	copy.Channel_amount = -o.Detailed_merchant.Channel_amount
	copy.Fee_amount = -o.Detailed_merchant.Fee_amount
	copy.SR_balance_currency = -o.Detailed_merchant.SR_balance_currency
	copy.SR_channel_currency = -o.Detailed_merchant.SR_channel_currency

	copy.CorrectionType = "reversal"
	copy.CorrectionTypeId = 4

	storage.Registry = append(storage.Registry, &copy)

}
