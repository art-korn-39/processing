package sverka

import (
	"app/config"
	"app/convert"
	"app/logs"
	"app/storage"
	"sort"
	"strings"
	"time"
)

func Start() {

	cfg := config.Get()

	storage, err := storage.New(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}
	defer storage.Close()

	// реестр провайдера
	full_registry := convert.ReadAndConvert(&cfg, storage)

	sortRegistry(full_registry)

	provider, start, finish := getArguments(full_registry)

	// получаем операции из detailed provider по провайдеру за период
	detailed := PSQL_read_registry(storage.Postgres, provider, start, finish)

	// сравниваем br и channel_amount
	differ_table := compare(full_registry, detailed)

	writeResult(&cfg, differ_table)

}

func sortRegistry(data []*convert.Base_operation) {
	sort.Slice(
		data,
		func(i int, j int) bool {
			return data[i].Provider_operation.Transaction_completed_at.Before(data[j].Provider_operation.Transaction_completed_at)
		},
	)
}

func getArguments(data []*convert.Base_operation) (provider []string, start, finish time.Time) {

	for _, v := range data {
		if !v.Provider_operation.Transaction_completed_at.IsZero() {
			start = v.Provider_operation.Transaction_completed_at
			break
		}
	}

	if start.IsZero() {
		logs.Add(logs.FATAL, "Не удалось определить начало периода")
	}

	finish = data[len(data)-1].Provider_operation.Transaction_completed_at
	if finish.IsZero() {
		logs.Add(logs.FATAL, "Не удалось определить конец периода")
	}

	start = start.Add(-3 * 24 * time.Hour)
	finish = finish.Add(3 * 24 * time.Hour)

	provider_map := map[string]bool{}

	for _, v1 := range data {
		provider_map[v1.Provider_operation.Provider_name] = true
	}

	provider = make([]string, 0, len(provider_map))
	for k := range provider_map {
		provider = append(provider, strings.ToLower(k))
	}

	return

}
