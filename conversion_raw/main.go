package conversion_raw

import (
	"app/config"
	"app/logs"
	"app/provider_balances"
	pr "app/provider_registry"
	"app/storage"
	"fmt"
	"path/filepath"
	"time"
)

var (
	final_registry map[int]*pr.Operation
	ext_registry   []*base_operation
	bof_registry   map[string]*Bof_operation

	is_kgx_tradex bool
	all_settings  map[string]*Setting
	used_settings map[string]*Setting
	teams         map[string]team_line
	providers     []provider_params
	balances      map[Bof_operation]string
)

func init() {
	final_registry = map[int]*pr.Operation{}
	ext_registry = make([]*base_operation, 0, 100000)
	bof_registry = map[string]*Bof_operation{}

	all_settings = map[string]*Setting{}
	used_settings = map[string]*Setting{}
	teams = map[string]team_line{}
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

	// чтение файла/папки реестра провайдера
	if filepath.Ext(filename) == "" {
		readFolder(filename)
		if len(ext_registry) == 0 {
			return
		}
	} else {
		err = readFile(filename)
		if err != nil {
			logs.Add(logs.FATAL, err)
		}
	}

	// проверить, что во всех настройках используется одна key_column для поиска в БОФ
	// + определить необходимость использования внешних источников
	key_column, external_usage, err := checkUsedSettings()
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// чтение операций БОФ
	err = readBofOperations(cfg, storage.Clickhouse, key_column)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// чтение дополнительного маппинга
	if external_usage {
		if is_kgx_tradex {
			err = readHandbook(cfg.Settings.Handbook)
			if err != nil {
				logs.Add(logs.FATAL, err)
			}
		}
		provider_balances.Read(storage.Postgres)
	}

	// сборка операции провайдера
	err = handleRecords()
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	// запись результата
	writeResult(cfg, storage.Postgres)
}

func handleRecords() error {

	start_time := time.Now()

	cntWithoutBof := 0

	for _, v := range ext_registry {

		var ok bool
		switch v.setting.Key_column {
		case OPID:
			v.bof_operation, ok = bof_registry[v.operation_id]
		case PAYID:
			v.bof_operation, ok = bof_registry[v.payment_id]
		}
		if !ok {
			cntWithoutBof++
		}

		provider_operation, err := v.createProviderOperation()
		if err != nil {
			return fmt.Errorf("ошибка при парсинге полей: %s", err)
		}

		sliceCalculatedFields := v.setting.getCalculatedFields()

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
