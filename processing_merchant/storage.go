package processing_merchant

import (
	"app/config"
	"app/logs"
	stg "app/storage"

	_ "github.com/alexbrainman/odbc"
	"github.com/jmoiron/sqlx"
)

type Storage struct {
	Postgres   *sqlx.DB
	Clickhouse *sqlx.DB
	Registry   []*Operation
	//Tariffs  []Tariff
}

func (s *Storage) Close() {
	if s.Postgres != nil {
		s.Postgres.Close()
	}
	if s.Clickhouse != nil {
		s.Clickhouse.Close()
	}
}

func (s *Storage) Connect() (err error) {

	cfg := config.Get()

	if config.Get().Clickhouse.Usage {
		connect, err := stg.CH_Connect(cfg)
		if err != nil {
			return err
		}
		logs.Add(logs.INFO, "Successful connection to Clickhouse")
		s.Clickhouse = connect
	}

	if config.Get().PSQL.Usage {
		connect, err := stg.PSQL_connect(cfg)
		if err != nil {
			return err
		}
		logs.Add(logs.INFO, "Successful connection to Postgres")
		s.Postgres = connect
	}

	//storage.Crypto = make(map[int]CryptoOperation, 200000)
	//storage.Provider_operations = make(map[int]ProviderOperation, 1000000)

	return nil
}

func GetRegistryCount() int {
	return len(storage.Registry)
}

func GetWithoutTariffCount() int {
	count := 0
	for _, o := range storage.Registry {
		if o.Tariff == nil {
			count++
		}
	}
	return count
}

func GetCheckFeeCount() int {
	count := 0
	for _, o := range storage.Registry {
		if o.CheckFee != 0 {
			count++
		}
	}
	return count
}

func BofRegistryNotValid() bool {

	cfg := config.Get()
	if cfg.Registry.Storage != config.File {
		return false
	}

	i := 0
	for _, o := range storage.Registry {
		if i > 5 {
			break
		}

		if o.Operation_id%1000000 != 0 {
			return false
		}

		i++
	}

	return true

}
