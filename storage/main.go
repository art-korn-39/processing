package storage

import (
	"app/config"
	"fmt"

	"github.com/jmoiron/sqlx"
)

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
