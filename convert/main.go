package convert

import (
	"app/balances_tradex"
	"app/config"
	"app/exchange_rates"
	"app/logs"
	"app/provider_balances"
	pr "app/provider_registry"
	"app/storage"
	"app/teams_tradex"
	"fmt"
	"os"
	"strconv"
	"time"
)

var (
	final_registry  map[int]*pr.Operation
	ext_registry    []*Base_operation
	bof_registry    map[string]*Bof_operation
	tradex_registry map[string]*Tradex_operation

	is_kgx_tradex   bool
	use_daily_rates bool
	main_setting    *Setting
	all_settings    map[string]*Setting
	used_settings   map[string]*Setting
	// teams           map[string]team_line
	// providers       []provider_params
	balances map[Bof_operation]string // кэш балансов полученных методом getBalance()
)

func init() {
	final_registry = map[int]*pr.Operation{}
	ext_registry = make([]*Base_operation, 0, 100000)
	bof_registry = map[string]*Bof_operation{}
	tradex_registry = map[string]*Tradex_operation{}

	all_settings = map[string]*Setting{}
	used_settings = map[string]*Setting{}
	// teams = map[string]team_line{}
	// providers = []provider_params{}
	balances = map[Bof_operation]string{}
}

func Start() {

	cfg := config.Get()

	storage, err := storage.New(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}
	defer storage.Close()

	// найти подходящие настройки маппинга, прочитать реестр провайдера
	ReadAndConvert(&cfg, storage)

	// запись результата
	writeResult(cfg, storage.Postgres)
}

func ReadAndConvert(cfg *config.Config, storage *storage.Storage) []*Base_operation {

	var err error

	// чтение настроек маппинга
	readSettings(storage.Postgres, cfg.Settings.Guid)
	if len(all_settings) == 0 {
		logs.Add(logs.FATAL, "По указанному провайдеру не найдены настройки конвертации.")
	}

	is_kgx_tradex = cfg.Settings.KGX_Tradex
	use_daily_rates = cfg.Settings.Daily_rates
	tradex_comission_file := cfg.Settings.Tradex_comission
	filename := cfg.Provider_registry.Filename

	logs.Add(logs.INFO, "Выполняется чтение...")

	// чтение файла/папки реестра провайдера
	if !use_daily_rates {

		info, _ := os.Stat(filename)
		if info.IsDir() {
			readFolder(filename)
			if len(ext_registry) == 0 {
				return nil
			}
		} else {
			err = readFile(filename)
			if err != nil {
				logs.Add(logs.FATAL, err)
			}
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
			// err = readHandbook(cfg.Settings.Handbook)
			// if err != nil {
			// 	logs.Add(logs.FATAL, err)
			// }
			teams_tradex.Read(storage.Postgres)
			balances_tradex.Read(storage.Postgres)

			if tradex_comission_file != "" {
				readTradexComission(tradex_comission_file)
			}
		}
		if use_daily_rates {
			exchange_rates.Read(storage.Postgres)
		}
		provider_balances.Read(storage.Postgres)
	}

	// сборка операции провайдера
	err = handleRecords()
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

	return ext_registry

}

func handleRecords() error {

	start_time := time.Now()

	cntWithoutBof := 0

	if use_daily_rates {
		for _, bof_op := range bof_registry {
			base_op, err := createBaseOperation(nil, nil, main_setting)
			if err != nil {
				logs.Add(logs.INFO, err)
				continue
			}
			base_op.Bof_operation = bof_op
			ext_registry = append(ext_registry, base_op)
		}
	} else {
		for _, base_operation := range ext_registry {
			var ok bool
			switch base_operation.Setting.Key_column {
			case OPID:
				base_operation.Bof_operation, ok = bof_registry[base_operation.operation_id]
			case PAYID:
				base_operation.Bof_operation, ok = bof_registry[base_operation.payment_id]
			}
			if !ok {
				cntWithoutBof++
			}
		}
	}

	for _, base_operation := range ext_registry {

		provider_operation, err := base_operation.createProviderOperation()
		if err != nil {
			logs.Add(logs.INFO, err)
			continue
		}

		if is_kgx_tradex {
			tradex_operation, ok := tradex_registry[base_operation.payment_id]
			if ok {
				provider_operation.BR_amount = tradex_operation.amount
			}
		}

		sliceCalculatedFields := base_operation.Setting.getCalculatedFields()

		setCalculatedFields(provider_operation, sliceCalculatedFields)

		setVerification(provider_operation)

		if base_operation.Bof_operation != nil {
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
				// if op.Provider_currency.Name == "EUR" && op.Rate != 0 {
				// 	op.Rate = 1 / op.Rate
				// }
			}
		}
	}

}

func setVerification(op *pr.Operation) {

	operation_id := strconv.Itoa(op.Id)
	payment_id := op.Provider_payment_id

	_, ok1 := bof_registry[operation_id]
	_, ok2 := bof_registry[payment_id]

	op.SetVerification(ok1 || ok2, use_daily_rates)

}
