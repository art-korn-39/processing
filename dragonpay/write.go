package dragonpay

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func insertIntoDB(db *sqlx.DB) error {

	if db == nil {
		return fmt.Errorf("Не подключена БД")
	}

	start_time := time.Now()

	stat := querrys.Stat_Insert_dragonpay_handbook()
	_, err := db.PrepareNamed(stat)
	if err != nil {
		return err
	}

	for k, v := range handbook {
		_, err := db.Exec(stat, k, v)
		if err != nil {
			return fmt.Errorf("не удалось записать в БД (dragonpay handbook): %v", err)
			//logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (dragonpay handbook): ", err))
		}
	}

	stat = querrys.Stat_Insert_dragonpay()
	_, err = db.PrepareNamed(stat)
	if err != nil {
		return err
	}

	for _, v := range registry {
		_, err := db.NamedExec(stat, v)
		if err != nil {
			return fmt.Errorf("не удалось записать в БД (dragonpay): %v", err)
		}
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка dragonpay в Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(registry))))

	return nil

}
