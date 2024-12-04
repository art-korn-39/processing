package conversion_raw

import (
	"app/config"
	"app/logs"
	"app/provider_registry"
	"app/querrys"
	"app/util"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

func writeResult(cfg config.Config, db *sqlx.DB) {

	if !cfg.Detailed.Usage {
		return
	}

	if cfg.Detailed.Storage == config.PSQL {
		writeIntoDB(db)
	} else {
		ext := filepath.Ext(cfg.Detailed.Filename)
		if ext == ".csv" {
			writeIntoCSV(cfg.Detailed.Filename)
		} else if ext == ".xlsx" {
			writeIntoXLSX(cfg.Detailed.Filename)
		}
	}

}

func writeIntoDB(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	chan_operations := make(chan []provider_registry.Operation, 1000)
	const batch_len = 10
	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_provider_registry_test()
	_, err := db.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for v := range chan_operations {

				_, err := db.NamedExec(stat, v)

				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД: ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]provider_registry.Operation, 0, batch_len)
	for _, v := range registry {
		// if v.Id == 0 {
		// 	continue
		// }
		batch = append(batch, *v)
		if (i+1)%batch_len == 0 {
			chan_operations <- batch
			batch = make([]provider_registry.Operation, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		chan_operations <- batch
	}

	close(chan_operations)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка операций в provider_registry: %v [%s строк]", time.Since(start_time), util.FormatInt(len(registry))))

}
