package crm_dictionary

import (
	"app/config"
	"app/logs"
	"app/storage"
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

	// err = loadPaymentMethod(cfg, token)
	// if err != nil {
	// 	logs.Add(logs.ERROR, err)
	// }
	// paymentMethodInsertIntoDB(storage.Postgres)

	// err = loadPaymentType(cfg, token)
	// if err != nil {
	// 	logs.Add(logs.ERROR, err)
	// }
	// paymentTypeInsertIntoDB(storage.Postgres)

	err = load_employees(cfg, token)
	if err != nil {
		logs.Add(logs.ERROR, err)
	}
	InsertIntoDB_employees(storage.Postgres)

	err = load_merchants(cfg, token)
	if err != nil {
		logs.Add(logs.ERROR, err)
	}
	InsertIntoDB_merchants(storage.Postgres)

	err = load_providers(cfg, token)
	if err != nil {
		logs.Add(logs.ERROR, err)
	}
	InsertIntoDB_providers(storage.Postgres)

}
