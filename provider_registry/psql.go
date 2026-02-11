package provider_registry

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func PSQL_read_registry_by_merchant(db *sqlx.DB, registry_done <-chan querrys.Args) {

	if db == nil {
		return
	}

	// MERCHANT_NAME + DATE
	Args := <-registry_done

	start_time := time.Now()

	usePeriodOnly := len(Args.Merchant_id) == 0

	var stat string
	var args []any
	if usePeriodOnly {
		stat = querrys.Stat_Select_provider_registry_period_only()
		args = []any{Args.DateFrom, Args.DateTo}
	} else {
		stat = querrys.Stat_Select_provider_registry_by_merchant_id()
		args = []any{pq.Array(Args.Merchant_id), Args.DateFrom, Args.DateTo}
	}

	err := db.Select(&rates, stat, args...)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for i := range rates {
		operation := &rates[i]

		operation.StartingFill(1)

		registry.Set(*operation)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(rates))))

}

func PSQL_read_registry_by_merchant_async(db *sqlx.DB, registry_done <-chan querrys.Args) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	if db == nil {
		return
	}

	// MERCHANT_NAME + DATE
	Args := <-registry_done

	start_time := time.Now()

	channel_dates := util.GetChannelOfDays(Args.DateFrom, Args.DateTo, 3*24*time.Hour)

	usePeriodOnly := len(Args.Merchant_id) == 0

	var stat string
	if usePeriodOnly {
		stat = querrys.Stat_Select_provider_registry_period_only()
	} else {
		stat = querrys.Stat_Select_provider_registry_by_merchant_id()
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {

				var args []any
				if usePeriodOnly {
					args = []any{period.StartDay, period.EndDay}
				} else {
					args = []any{pq.Array(Args.Merchant_id), period.StartDay, period.EndDay}
				}

				res := make([]Operation, 0, 10000)

				err := db.Select(&res, stat, args...)
				if err != nil {
					logs.Add(logs.INFO, err)
					return
				}

				mu.Lock()
				rates = append(rates, res...)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	for i := range rates {
		operation := &rates[i]

		operation.StartingFill(1)

		registry.Set(*operation)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(rates))))

}

func PSQL_read_registry_by_provider_async(db *sqlx.DB, registry_done <-chan querrys.Args) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	if db == nil {
		return
	}

	// MERCHANT_NAME + DATE
	Args := <-registry_done

	start_time := time.Now()

	channel_dates := util.GetChannelOfDays(Args.DateFrom, Args.DateTo, 3*24*time.Hour)

	usePeriodOnly := len(Args.Provider_id) == 0
	//useMerchantID := len(Args.Merchant_id) > 0

	var stat string
	if usePeriodOnly {
		stat = querrys.Stat_Select_provider_registry_period_only()
	} else {
		// if useMerchantID {
		// 	stat = querrys.Stat_Select_provider_registry()
		// } else {
		stat = querrys.Stat_Select_provider_registry_by_provider_id()
		// }
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {

				var args []any
				if usePeriodOnly {
					args = []any{period.StartDay, period.EndDay}
				} else {
					args = []any{pq.Array(Args.Provider_id), period.StartDay, period.EndDay}
				}

				res := make([]Operation, 0, 10000)

				err := db.Select(&res, stat, args...)
				if err != nil {
					logs.Add(logs.INFO, err)
					return
				}

				mu.Lock()
				rates = append(rates, res...)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	for i := range rates {
		operation := &rates[i]

		operation.StartingFill(1)

		registry.Set(*operation)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(rates))))

}
