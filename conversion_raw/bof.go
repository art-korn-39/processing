package conversion_raw

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

type Bof_operation struct {
	Operation_id          string    `db:"operation_id"`
	Provider_payment_id   string    `db:"provider_payment_id"`
	Created_at            time.Time `db:"created_at"`
	Provider_name         string    `db:"provider_name"`
	Merchant_name         string    `db:"merchant_name"`
	Merchant_account_name string    `db:"merchant_account_name"`
	Operation_type_id     int       `db:"operation_type_id"`
	Operation_type        string
	Payment_type          string `db:"payment_type"`
	Country_code2         string `db:"country"`
	Status                string `db:"status"`
}

func (op *Bof_operation) fill() {

	if op.Operation_type_id == 3 {
		op.Operation_type = "sale"
	} else if op.Operation_type_id == 2 {
		op.Operation_type = "capture"
	} else if op.Operation_type_id == 6 {
		op.Operation_type = "recurring"
	} else if op.Operation_type_id == 5 {
		op.Operation_type = "refund"
	} else if op.Operation_type_id == 11 {
		op.Operation_type = "payout"
	}

}

func readBofOperations(db *sqlx.DB, key_column string) {

	start_time := time.Now()

	var mu sync.Mutex
	var wg sync.WaitGroup

	ch := getBatchIdChan(key_column)

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for b := range ch {
				statement := querrys.Stat_Select_reports_by_id()
				id_str := strings.Trim(strings.Join(b, "','"), "[]")
				statement = strings.ReplaceAll(statement, "$1", id_str)
				statement = strings.ReplaceAll(statement, "$2", key_column)
				result := []Bof_operation{}
				err := db.Select(&result, statement)
				if err != nil {
					logs.Add(logs.ERROR, err)
					continue
				}
				for _, v := range result {
					v.fill()
					mu.Lock()
					switch key_column {
					case OPID:
						bof_operations[v.Operation_id] = &v
					case PAYID:
						bof_operations[v.Provider_payment_id] = &v
					}

					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Чтение операций БОФ: %v [%s строк]", time.Since(start_time), util.FormatInt(len(bof_operations))))

}

func getBatchIdChan(key_column string) chan []string {

	batch_len := 10000
	ch := make(chan []string, 1000)

	go func() {
		var i int
		batch := make([]string, 0, batch_len)
		for _, v := range ext_registry {

			switch key_column {
			case "operation_id":
				batch = append(batch, v.operation_id)
			case "provider_payment_id":
				batch = append(batch, v.payment_id)
			}

			if (i+1)%batch_len == 0 {
				ch <- batch
				batch = make([]string, 0, batch_len)
			}
			i++
		}

		if len(batch) != 0 {
			ch <- batch
		}

		close(ch)
	}()

	return ch

}
