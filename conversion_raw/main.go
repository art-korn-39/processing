package conversion_raw

import (
	"app/config"
	"app/logs"
	"app/provider_registry"
	"app/storage"
	"fmt"
	"time"
)

var (
	registry       map[int]*provider_registry.Operation
	ext_registry   []*ext_operation
	bof_operations map[string]*Bof_operation
	all_settings   map[string]Setting
	handbook       map[string]string
)

func init() {
	registry = map[int]*provider_registry.Operation{}
	all_settings = map[string]Setting{}
	bof_operations = map[string]*Bof_operation{}
	handbook = map[string]string{}
}

func Start() {

	cfg := config.Get()

	storage, err := storage.New(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
		return
	}
	defer storage.Close()

	readSettings(storage.Postgres, cfg.Settings.Guid)

	filename := cfg.Provider_registry.Filename

	logs.Add(logs.INFO, "Выполняется чтение...")

	map_fields, setting, err := readFile(filename)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	readBofOperations(storage.Clickhouse, setting.Key_column)

	err = handleRecords(map_fields, setting)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	writeIntoDB(storage.Postgres)
}

func handleRecords(map_fields map[string]int, setting Setting) error {

	start_time := time.Now()

	cntWithoutBof := 0

	sliceCalculatedFields := setting.getCalculatedFields()

	for _, v := range ext_registry {

		var ok bool
		switch setting.Key_column {
		case OPID:
			v.bof_operation, ok = bof_operations[v.operation_id]
		case PAYID:
			v.bof_operation, ok = bof_operations[v.payment_id]
		}
		if !ok {
			cntWithoutBof++
			continue
		}

		provider_operation, err := v.createProviderOperation(map_fields, setting)
		if err != nil {
			return fmt.Errorf("ошибка при парсинге полей: %s", err)
		}

		setCalculatedFields(provider_operation, sliceCalculatedFields)

		registry[provider_operation.Id] = provider_operation

	}

	logs.Add(logs.INFO, fmt.Sprintf("Конвертация строковых полей в структуру БД: %v [без БОФ: %d]", time.Since(start_time), cntWithoutBof))

	return nil

}

func setCalculatedFields(op *provider_registry.Operation, calculatedFields []string) {

	for _, v := range calculatedFields {
		switch v {
		case "rate":
			if op.Amount != 0 {
				op.Rate = op.Channel_amount / op.Amount
				if op.Provider_currency.Name == "EUR" && op.Rate != 0 {
					op.Rate = 1 / op.Rate
				}
			}
		}
	}

}
