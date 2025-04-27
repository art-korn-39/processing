package sverka

import (
	"app/config"
	"app/convert"
	"app/logs"
	"app/storage"
	"sort"
	"time"
)

func Start() {

	cfg := config.Get()

	storage, err := storage.New(cfg)
	if err != nil {
		logs.Add(logs.FATAL, err)
	}
	defer storage.Close()

	full_registry := convert.ReadAndConvert(&cfg, storage)

	sortRegistry(full_registry)

	provider, start, finish := getArguments(full_registry)

	detailed := PSQL_read_registry(storage.Postgres, provider, start, finish)

	differ_table := compare(full_registry, detailed)

	writeResult(&cfg, differ_table)

}

func sortRegistry(data []*convert.Base_operation) {
	sort.Slice(
		data,
		func(i int, j int) bool {
			return data[i].Provider_operation.Transaction_completed_at.After(data[j].Provider_operation.Transaction_completed_at)
		},
	)
}

func getArguments(data []*convert.Base_operation) (provider []string, start, finish time.Time) {

	start = data[0].Provider_operation.Transaction_completed_at
	finish = data[len(data)-1].Provider_operation.Transaction_completed_at

	provider_map := map[string]bool{}

	for _, v1 := range data {
		provider_map[v1.Provider_operation.Provider_name] = true
	}

	provider = make([]string, 0, len(provider_map))
	for k := range provider_map {
		provider = append(provider, k)
	}

	return

}
