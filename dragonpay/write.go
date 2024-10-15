package dragonpay

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func insert_into_db(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Insert_dragonpay_handbook()
	_, err := db.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for k, v := range Handbook {
		_, err := db.Exec(stat, k, v)
		if err != nil {
			logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (dragonpay handbook): ", err))
		}
	}

	stat = querrys.Stat_Insert_dragonpay()
	_, err = db.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, v := range Registry {
		_, err := db.NamedExec(stat, v)
		if err != nil {
			logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (dragonpay): ", err))
		}
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка dragonpay в Postgres: %v [%s строк]", time.Since(start_time), util.FormatInt(len(Registry))))

}
