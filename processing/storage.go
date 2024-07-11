package processing

import (
	"app/config"
	"app/logs"
	"fmt"

	_ "github.com/alexbrainman/odbc"
	"github.com/jmoiron/sqlx"
)

type Storage struct {
	Postgres   *sqlx.DB
	Clickhouse *sqlx.DB

	Registry            []*Operation
	Tariffs             []Tariff
	Crypto              map[int]CryptoOperation
	Rates               []ProviderOperation
	Provider_operations map[int]ProviderOperation
}

func CH_Connect() (*sqlx.DB, error) {

	connInfo := fmt.Sprintf("driver=ClickHouse ODBC Driver (Unicode);host=%s;port=%d;username=%s;password=%s;dbname=%s",
		config.Get().Clickhouse.Host, config.Get().Clickhouse.Port, config.Get().Clickhouse.User, config.Get().Clickhouse.Password, config.Get().Clickhouse.Name)

	connect, err := sqlx.Connect("odbc", connInfo)
	if err != nil {
		return nil, err
	}

	return connect, nil
}

// PostgreSQL only supports 65535 parameters
// max count 1 batch = 65535 / count columns
func PSQL_connect() (*sqlx.DB, error) {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		config.Get().PSQL.Host, config.Get().PSQL.Port, config.Get().PSQL.User, config.Get().PSQL.Password, config.Get().PSQL.Name)

	connect, err := sqlx.Connect("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	return connect, nil

}

func (s *Storage) Close() {
	if s.Postgres != nil {
		s.Postgres.Close()
	}
	if s.Clickhouse != nil {
		s.Clickhouse.Close()
	}
}

func (s *Storage) Init() (err error) {

	if config.Get().Clickhouse.Usage {
		connect, err := CH_Connect()
		if err != nil {
			return err
		}
		logs.Add(logs.INFO, "Successful connection to Clickhouse")
		s.Clickhouse = connect
	}

	if config.Get().PSQL.Usage {
		connect, err := PSQL_connect()
		if err != nil {
			return err
		}
		logs.Add(logs.INFO, "Successful connection to Postgres")
		s.Postgres = connect
	}

	storage.Crypto = make(map[int]CryptoOperation)
	storage.Provider_operations = make(map[int]ProviderOperation)

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
