package conversion_raw

import (
	"app/config"
	"app/logs"
	"app/provider_balances"
	pr "app/provider_registry"
	"app/storage"
	"fmt"
	"time"
)

var (
	final_registry map[int]*pr.Operation
	ext_registry   []*raw_operation
	bof_registry   map[string]*Bof_operation

	is_kgx_tradex bool
	all_settings  map[string]Setting
	teams         map[string]string
	providers     []provider_params
	balances      map[Bof_operation]string
)

func init() {
	final_registry = map[int]*pr.Operation{}
	bof_registry = map[string]*Bof_operation{}

	all_settings = map[string]Setting{}
	teams = map[string]string{}
	providers = []provider_params{}
	balances = map[Bof_operation]string{}
}

func Start() {

	cfg := config.Get()

	storage, err := storage.New(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}
	defer storage.Close()

	// чтение настроек маппинга
	readSettings(storage.Postgres, cfg.Settings.Guid)
	if len(all_settings) == 0 {
		logs.Add(logs.FATAL, "По указанному провайдеру не найдены настройки конвертации.")
	}

	is_kgx_tradex = cfg.Settings.KGX_Tradex
	filename := cfg.Provider_registry.Filename

	logs.Add(logs.INFO, "Выполняется чтение...")

	// чтение файла реестра провайдера
	map_fields, setting, err := readFile(filename)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// чтение операций БОФ
	err = readBofOperations(cfg, storage.Clickhouse, setting.Key_column)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// чтение дополнительного маппинга
	if setting.external_usage {
		if is_kgx_tradex {
			err = readHandbook(cfg.Settings.Handbook)
			if err != nil {
				logs.Add(logs.FATAL, err)
			}
		}
		provider_balances.Read(storage.Postgres)
	}

	// сборка операции провайдера
	err = handleRecords(map_fields, setting)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// запись результата
	writeResult(cfg, storage.Postgres)
}

func handleRecords(map_fields map[string]int, setting Setting) error {

	start_time := time.Now()

	cntWithoutBof := 0

	sliceCalculatedFields := setting.getCalculatedFields()

	for _, v := range ext_registry {

		var ok bool
		switch setting.Key_column {
		case OPID:
			v.bof_operation, ok = bof_registry[v.operation_id]
		case PAYID:
			v.bof_operation, ok = bof_registry[v.payment_id]
		}
		if !ok {
			cntWithoutBof++
			//continue
		}

		provider_operation, err := v.createProviderOperation(map_fields, setting)
		if err != nil {
			return fmt.Errorf("ошибка при парсинге полей: %s", err)
		}

		setCalculatedFields(provider_operation, sliceCalculatedFields)

		if ok {
			final_registry[provider_operation.Id] = provider_operation
		}

	}

	logs.Add(logs.INFO, fmt.Sprintf("Конвертация строковых полей в структуру БД: %v [без БОФ: %d]", time.Since(start_time), cntWithoutBof))

	return nil

}

func setCalculatedFields(op *pr.Operation, calculatedFields []string) {

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
