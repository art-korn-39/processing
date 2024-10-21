package chargeback

import (
	"app/config"
	"app/logs"
	"app/storage"
	"app/util"
	"fmt"
	"time"
)

const (
	AUTH       = "/ServiceModel/AuthService.svc/Login"
	CHARGEBACK = "/0/OData/UsrChargeback"
	MATCH      = "/0/OData/PspOperationInReqestDispute"
	OPERATION  = "/0/OData/PspProcessingOperation"
)

var (
	Chargebacks map[string]*Chargeback
	Operations  map[string]*Operation
	Dispute     map[string]string // operation - chargeback
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
	err = load_chargebacks(cfg, token)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// поместили загруженные чарджбэки в БД
	chargebacks_insert_into_db(storage.Postgres)

	// получили все чарджбэки из БД (для кейса, когда только за месяц грузили)
	ReadChargebacks(storage.Postgres)

	// получили операции и мэтчи (все или за месяц)
	err = load_operations(cfg, token)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// заполнение case_id из чарджей
	setChargebackInfoIntoOperations()

	operations_insert_into_db(storage.Postgres)

}
func setChargebackInfoIntoOperations() {
	start_time := time.Now()
	var countNoneInDisput int
	var countBadId int
	for _, op := range Operations {
		chargeback_id, ok := Dispute[op.GUID]
		if ok {
			op.Chargeback_id = chargeback_id
			chargeback, ok := Chargebacks[chargeback_id]
			if ok {
				op.Chargeback_case_id = chargeback.Case_ID
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
