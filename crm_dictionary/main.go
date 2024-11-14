package crm_dictionary

import (
	"app/config"
	"app/logs"
	"app/storage"
)

const (
	PAYMENT_METHOD = "/0/OData/UsrProcessingPaymentMethod"
	PAYMENT_TYPE   = "/0/OData/UsrPaymentMethodTypes"
)

var payment_methods []Payment_method
var payment_types []Payment_type

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

	err = loadPaymentMethod(cfg, token)
	if err != nil {
		logs.Add(logs.ERROR, err)
	}

	err = loadPaymentType(cfg, token)
	if err != nil {
		logs.Add(logs.ERROR, err)
	}

	paymentMethodInsertIntoDB(storage.Postgres)
	paymentTypeInsertIntoDB(storage.Postgres)

}
