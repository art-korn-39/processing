package crm_provider_losses

import (
	"app/config"
	"app/logs"
	"app/storage"
)

var (
	operations []*Operation
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

	// получили операции и мэтчи (все или за месяц)
	err = loadOperations(cfg, token)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	operationsInsertIntoDB(storage.Postgres)

}
