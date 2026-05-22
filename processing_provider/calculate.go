package processing_provider

import (
	"app/config"
	"app/logs"
	"app/util"
	"fmt"
	"sync"
	"time"
)

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

				if operation.SkipDecline() || operation.Dupclicate {
					continue
				}

				operation.mu.Lock()

				operation.SetBalanceAmount()
				operation.SetRate()
				operation.SetBRAmount()
				operation.SetExtraBRAmount()
				operation.SetRR()
				operation.SetUNA()
				operation.SetBRCompensation()

				operation.SetVerification()
				operation.SetVerificationTradex()

				operation.SetCorrection()
				operation.SetDeclineAmount()

				operation.mu.Unlock()

				addDuplicate(operation)

			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Расчёт комиссии: %v", util.FormatDuration(time.Since(start_time))))

}

func addDuplicate(o *Operation) {

	if o.CorrectionTypeId != 2 {
		return
	}

	if o.Detailed_provider == nil {
		return
	}

	copy := *o
	copy.mu = &sync.Mutex{}

	copy.Balance_amount = -o.Detailed_provider.Balance_amount
	copy.Channel_amount = -o.Detailed_provider.Channel_amount
	copy.BR_balance_currency = -o.Detailed_provider.BR_balance_currency

	copy.CorrectionType = "reversal"
	copy.CorrectionTypeId = 4

	storage.Registry = append(storage.Registry, &copy)

}
