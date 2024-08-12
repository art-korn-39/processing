package storage

import (
	"app/config"
	"app/logs"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jmoiron/sqlx"
)

type Storage struct {
	Postgres   *sqlx.DB
	Clickhouse *sqlx.DB
	AWS        *session.Session
}

func New(cfg config.Config) (s *Storage, err error) {

	s = &Storage{}

	if cfg.Clickhouse.Usage {
		connect, err := CH_Connect(cfg)
		if err != nil {
			return nil, err
		}
		logs.Add(logs.INFO, "Successful connection to Clickhouse")
		s.Clickhouse = connect
	}

	if cfg.PSQL.Usage {
		connect, err := PSQL_connect(cfg)
		if err != nil {
			return nil, err
		}
		logs.Add(logs.INFO, "Successful connection to Postgres")
		s.Postgres = connect
	}

	if cfg.AWS.Usage {
		connect, err := AWS_connect(cfg)
		if err != nil {
			return nil, err
		}
		logs.Add(logs.INFO, "Successful connection to Amazon S3")
		s.AWS = connect
	}

	return s, nil
}

func CH_Connect(cfg config.Config) (*sqlx.DB, error) {

	connInfo := fmt.Sprintf("driver=ClickHouse ODBC Driver (Unicode);host=%s;port=%d;username=%s;password=%s;dbname=%s",
		cfg.Clickhouse.Host, cfg.Clickhouse.Port, cfg.Clickhouse.User, cfg.Clickhouse.Password, cfg.Clickhouse.Name)

	connect, err := sqlx.Connect("odbc", connInfo)
	if err != nil {
		return nil, err
	}

	return connect, nil
}

// PostgreSQL only supports 65535 parameters
// max count 1 batch = 65535 / count columns
func PSQL_connect(cfg config.Config) (*sqlx.DB, error) {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		cfg.PSQL.Host, cfg.PSQL.Port, cfg.PSQL.User, cfg.PSQL.Password, cfg.PSQL.Name)

	connect, err := sqlx.Connect("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	return connect, nil

}

func AWS_connect(cfg config.Config) (*session.Session, error) {
	//b := true
	sess, err := session.NewSession(
		&aws.Config{
			Region:      aws.String(cfg.AWS.Region),
			Credentials: credentials.NewStaticCredentials(cfg.AWS.Key, cfg.AWS.Secret, ""),
		},
	)
	if err != nil {
		return nil, err
	}
	return sess, nil
}

func (s *Storage) Close() {
	if s.Postgres != nil {
		s.Postgres.Close()
	}
	if s.Clickhouse != nil {
		s.Clickhouse.Close()
	}
}
