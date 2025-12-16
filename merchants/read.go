package merchants

import (
	"app/logs"
	"app/querrys"
	"app/util"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	data map[int]*Merchant
)

func init() {
	data = map[int]*Merchant{}
}

func Read(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	stat := querrys.Stat_Select_merchants()

	slice_merchants := []*Merchant{}

	err := db.Select(&slice_merchants, stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	for _, merchant := range slice_merchants {

		data[merchant.Project_id] = merchant
	}

	logs.Add(logs.INFO, fmt.Sprintf("Чтение мерчантов: %v [%s строк]", util.FormatDuration(time.Since(start_time)), util.FormatInt(len(data))))

}

func GetByProjectID(project_id int) (*Merchant, bool) {
	m, ok := data[project_id]
	return m, ok
}
