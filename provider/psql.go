package provider

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

func PSQL_read_registry(db *sqlx.DB, registry_done <-chan querrys.Args) {

	if db == nil {
		return
	}

	// MERCHANT_NAME + DATE
	Args := <-registry_done

	if len(Args.Merhcant) == 0 {
		logs.Add(logs.INFO, `пустой массив "merchant_name" для чтения операций провайдера`)
		return
	}

	start_time := time.Now()

	args := []any{pq.Array(Args.Merhcant), Args.DateFrom, Args.DateTo}

	stat := querrys.Stat_Select_provider_registry()

	err := db.Select(&Rates, stat, args...)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for i := range Rates {
		operation := &Rates[i]

		operation.StartingFill(true)

		Registry.Set(*operation)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Rates))))

}

func PSQL_read_registry_async(db *sqlx.DB, registry_done <-chan querrys.Args) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	if db == nil {
		return
	}

	// MERCHANT_NAME + DATE
	Args := <-registry_done

	if len(Args.Merhcant) == 0 {
		logs.Add(logs.INFO, `пустой массив "merchant_name" для чтения операций провайдера`)
		return
	}

	start_time := time.Now()

	channel_dates := util.GetChannelOfDays(Args.DateFrom, Args.DateTo, 24*time.Hour)

	stat := querrys.Stat_Select_provider_registry()

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {

				args := []any{pq.Array(Args.Merhcant), period.StartDay, period.EndDay}

				res := make([]Operation, 0, 10000)

				err := db.Select(&res, stat, args...)
				if err != nil {
					logs.Add(logs.INFO, err)
					return
				}

				mu.Lock()
				Rates = append(Rates, res...)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	for i := range Rates {
		operation := &Rates[i]

		operation.StartingFill(false)

		Registry.Set(*operation)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Rates))))

}

func PSQL_read_registry_async_querry(db *sqlx.DB, registry_done <-chan querrys.Args) {

	var wg sync.WaitGroup
	var mu sync.Mutex

	if db == nil {
		return
	}

	// MERCHANT_NAME + DATE
	Args := <-registry_done

	if len(Args.Merhcant) == 0 {
		logs.Add(logs.INFO, `пустой массив "merchant_name" для чтения операций провайдера`)
		return
	}

	start_time := time.Now()

	channel_dates := util.GetChannelOfDays(Args.DateFrom, Args.DateTo, 24*time.Hour)

	stat := querrys.Stat_Select_provider_registry()

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for period := range channel_dates {

				args := []any{pq.Array(Args.Merhcant), period.StartDay, period.EndDay}

				res := make([]Operation, 0, 10000)

				rows, err := db.Queryx(stat, args...)
				if err != nil {
					logs.Add(logs.FATAL, err)
					return
				}

				for rows.Next() {

					var r Operation
					if err := rows.StructScan(&r); err != nil {
						logs.Add(logs.FATAL, err)
						return
					}

					r.StartingFill(false)

					mu.Lock()
					Registry.Set(r)
					mu.Unlock()

					res = append(res, r)

				}

				mu.Lock()
				Rates = append(Rates, res...)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение реестра провайдера из Postgres async Q: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Rates))))

}
