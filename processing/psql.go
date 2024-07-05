package processing

import (
	"app/config"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	pq_driver "github.com/lib/pq"
)

const (
	PSQL_NUM_GORUTINES = 10
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

func PSQL_insert() {

	type Row struct {
		Id      int    `db:"id"`
		Name    string `db:"name"`
		Project string `db:"project"`
		Amount  int    `db:"amount"`
	}

	channel := make(chan []Row, 50)

	const COUNT = 1000
	const batch_len = 100

	start_time := time.Now()

	var wg2 sync.WaitGroup

	wg2.Add(PSQL_NUM_GORUTINES)
	for i := 1; i <= PSQL_NUM_GORUTINES; i++ {
		go func() {
			defer wg2.Done()
			for v := range channel {
				storage.Postgres.NamedExec(`INSERT INTO many_rows (id, name, project, amount)
				VALUES (:id, :name, :project, :amount)`, v)
			}
		}()
	}

	batch := make([]Row, 0, batch_len)
	for i := 1; i <= COUNT; i++ {
		r := Row{
			Id:      i,
			Name:    "sample name",
			Project: "project name",
			Amount:  COUNT % i,
		}
		batch = append(batch, r)
		if i%batch_len == 0 {
			channel <- batch
			batch = make([]Row, 0, batch_len)
		}
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg2.Wait()

	fmt.Println("Загрузка в БД: ", time.Since(start_time))

}

func PSQL_DeleteAndInsert() {

	type Row struct {
		Id      int    `db:"id"`
		Name    string `db:"name"`
		Project string `db:"project"`
		Amount  int    `db:"amount"`
		Str1    string `db:"str1"`
		Str2    string `db:"str2"`
		Str3    string `db:"str3"`
		Str4    string `db:"str4"`
		Int1    int    `db:"int1"`
		Int2    int    `db:"int2"`
		Int3    int    `db:"int3"`
		Int4    int    `db:"int4"`
		Int5    int    `db:"int5"`
		Int6    int    `db:"int6"`
		Int7    int    `db:"int7"`
		Int8    int    `db:"int8"`
	}

	var wg sync.WaitGroup
	channel := make(chan []Row, 1000)
	const COUNT = 1000000
	const batch_len = 1000

	//1M записей 4 значения:
	//pq: got 4000000 parameters but PostgreSQL only supports 65535 parameters
	//cnt 1 batch = 65535 / cnt cols

	st := time.Now()

	wg.Add(PSQL_NUM_GORUTINES)
	for i := 1; i <= PSQL_NUM_GORUTINES; i++ {
		go func() {
			defer wg.Done()
			for v := range channel {

				tx, _ := storage.Postgres.Beginx()

				sliceID := make([]int, 0, len(v))
				for _, row := range v {
					sliceID = append(sliceID, row.Id)
				}
				tx.Exec("delete from many_rows where id = ANY($1);", pq_driver.Array(sliceID))

				_, err := tx.NamedExec(`INSERT INTO many_rows (
					id, name, project, amount, 
					str1, str2, str3, str4,
					int1, int2, int3, int4, int5, int6, int7, int8)
				VALUES (:id, :name, :project, :amount, 
						:str1, :str2, :str3, :str4,
						:int1, :int2, :int3, :int4, :int5, :int6, :int7, :int8)`, v)

				if err != nil {
					tx.Rollback()
				} else {
					tx.Commit()
				}

			}
		}()
	}

	batch := make([]Row, 0, batch_len)
	for i := 1; i <= COUNT; i++ {
		r := Row{
			Id:      i,
			Name:    "sample name",
			Project: "project name",
			Amount:  COUNT % i,
			Str1:    "111",
			Str2:    "222",
			Str3:    "333",
			Str4:    "444",
			Int1:    10,
		}
		batch = append(batch, r)
		if i%batch_len == 0 {
			channel <- batch
			batch = make([]Row, 0, batch_len)
		}
	}

	if len(batch) != 0 {
		channel <- batch
	}
	close(channel)

	wg.Wait()

	fmt.Println("Загрузка в БД: ", time.Since(st))

}
