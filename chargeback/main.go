package chargeback

import (
	"app/config"
	"app/logs"
	"app/storage"
	"app/util"
	"fmt"
	"time"
)

var (
	chargebacks map[string]*Chargeback
	operations  map[string]*Operation
	dispute_map map[string]Dispute // operation - chargeback
)

func Start() {

	cfg := config.Get()

	storage, err := storage.New(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}
	defer storage.Close()

	token, err := auth(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// получение чарджбэков (все или за месяц)
	err = loadChargebacks(cfg, token)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// поместили загруженные чарджбэки в БД
	chargebacksInsertIntoDB(storage.Postgres)

	// получили все чарджбэки из БД (для кейса, когда только за месяц грузили)
	readChargebacks(storage.Postgres)

	// получили операции и мэтчи (все или за месяц)
	err = loadOperations(cfg, token)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// заполнение case_id из чарджей
	setChargebackInfoIntoOperations()

	operationsInsertIntoDB(storage.Postgres)

}

func setChargebackInfoIntoOperations() {
	start_time := time.Now()
	var countNoneInDisput int
	var countBadId int
	for _, op := range operations {
		dispute, ok := dispute_map[op.GUID]
		if ok {
			op.Chargeback_id = dispute.Chargeback_id
			op.State = dispute.State_name
			op.StateChangeDate = dispute.StateChangeDate
			chargeback, ok := chargebacks[dispute.Chargeback_id]
			if ok {
				op.Chargeback_case_id = chargeback.Case_ID
				op.Chargeback_status = chargeback.Status
				op.Chargeback_deadline = chargeback.Deadline
				op.Chargeback_code_reason = chargeback.Code_reason
			} else {
				countBadId++
			}
		} else {
			countNoneInDisput++
		}
	}
	logs.Add(logs.MAIN, fmt.Sprintf("Стыковка chargebacks и операций: %v [%s нет в т.Dispute, %s пустой ID]",
		time.Since(start_time),
		util.FormatInt(countNoneInDisput),
		util.FormatInt(countBadId)))
}
