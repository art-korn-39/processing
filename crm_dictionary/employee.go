package crm_dictionary

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/util"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	EMPLOYEES = "/0/OData/Employee"
)

var employees []Employee

type Employee struct {
	Id             string `json:"id" db:"id"`
	Name           string `json:"name" db:"name"`
	Email          string `db:"email"`
	ManagerName    string `db:"manager"`
	DepartmentName string `db:"department"`
	JobTitle       string `db:"job_title"`

	// вложенные структуры json файла
	// перекладываем их значения на верхний уровень
	Contact          map[string]string `json:"contact"`          //Name
	Manager          map[string]string `json:"manager"`          //Name
	OrgStructureUnit map[string]string `json:"orgstructureunit"` //Name
	Job              map[string]string `json:"job"`              //Name
}

func (emp *Employee) fill() {
	emp.Email = emp.Contact["Email"]
	emp.ManagerName = emp.Manager["Name"]
	emp.DepartmentName = emp.OrgStructureUnit["Name"]
	emp.JobTitle = emp.Job["Name"]
}

func load_employees(cfg config.Config, token string) error {

	start_time := time.Now()

	type Response struct {
		Value []Employee `json:"value"`
	}

	s0 := []string{"$select=Id,Name"}

	s1 := []string{
		"$expand=Contact($select=Email)",
		"OrgStructureUnit($select=Name)",
		"Manager($select=Name)",
		"Job($select=Name)",
	}
	s0 = append(s0, strings.Join(s1, ","))

	url_params := strings.Join(s0, "&")

	requestURL := cfg.CRM.URL + EMPLOYEES + "?" + url_params

	req, _ := http.NewRequest("GET", requestURL, nil)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cookie", ".ASPXAUTH="+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("wrong response status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var data Response
	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}

	employees = data.Value
	for i, v := range employees {
		v.fill()
		employees[i] = v
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Получение employees: %v [%s строк]", time.Since(start_time), util.FormatInt(len(employees))))

	return nil
}

func InsertIntoDB_employees(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Employee, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_crm_employees()
	_, err := db.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for v := range channel {

				_, err := db.NamedExec(stat, v)
				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (employee): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Employee, 0, batch_len)
	for _, v := range employees {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Employee, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка employees crm в Postgres: %v", time.Since(start_time)))
}
