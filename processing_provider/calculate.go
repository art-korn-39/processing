package processing_provider

import (
	"app/config"
	"app/logs"
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

				operation.mu.Lock()

				//operation.SetCountry()

				operation.SetBalanceAmount()
				operation.SetBRAmount()
				operation.SetExtraBRAmount()

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
