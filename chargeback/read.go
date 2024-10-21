package chargeback

import (
	"app/logs"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func ReadChargebacks(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := `SELECT * FROM chargebacks`

	slice_chargebacks := []Chargeback{}

	err := db.Select(&slice_chargebacks, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	Chargebacks = map[string]*Chargeback{}

	for _, chargeback := range slice_chargebacks {
		Chargebacks[chargeback.ID] = &chargeback
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Чтение chargebacks из Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Chargebacks))))

}
