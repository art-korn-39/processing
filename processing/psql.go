package processing

import (
	"app/config"
	"fmt"

	"github.com/jmoiron/sqlx"
)

const (
// PSQL_NUM_GORUTINES = 10
)

// PostgreSQL only supports 65535 parameters
// cnt 1 batch = 65535 / cnt cols

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
