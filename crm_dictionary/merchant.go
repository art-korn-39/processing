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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	MERCHANTS   = "/0/OData/Account"
	MAX_INTEGER = 2147483647
)

var merchants []Merchant

type Merchant struct {
	Id             string `json:"id" db:"id"`
	Name           string `json:"name" db:"name"`
	Bof_id         int    `db:"bof_id"`
	TypeName       string `db:"type"`
	StatusName     string `db:"status"`
	Fin_manager_id string `json:"UsrMerchantFinManagerId" db:"fin_manager_id"`
	Kam_id         string `json:"UsrMerchantKamId" db:"kam_id"`
	Kam_sub_id     string `json:"UsrMerchantKamSubstitteId" db:"kam_sub_id"`

	// вложенные структуры json файла
	// перекладываем их значения на верхний уровень
	Type       map[string]string `json:"type"`              //Name
	Status     map[string]string `json:"usrmerchantstatus"` //Name
	Bof_id_str string            `json:"pspmechantprocessingid"`
}

func (m *Merchant) getIDs() ([]int, error) {
	ids_str := strings.Trim(m.Bof_id_str, " ")
	if ids_str == "" {
		return []int{}, nil
	}

	s := strings.Split(ids_str, ",")
	if len(s) == 0 {
		return []int{}, nil
	}

	s = slices.Compact(s)

	result := []int{}
	for _, v := range s {
		num, err := strconv.Atoi(strings.Trim(v, " "))
		if err != nil {
			return []int{}, err
		} else {
			if num <= MAX_INTEGER {
				result = append(result, num)
			}
		}
	}

	return result, nil
}

func (m *Merchant) fill() {
	m.TypeName = m.Type["Name"]
	m.StatusName = m.Status["Name"]
}

func load_merchants(cfg config.Config, token string) error {

	start_time := time.Now()

	type Response struct {
		Value []Merchant `json:"value"`
	}

	s0 := []string{
		"$select=Id,Name,PspMechantProcessingId,UsrMerchantKamId,UsrMerchantKamSubstitteId,UsrMerchantFinManagerId",
		`filter=Type/Name%20eq%20%27Merchant%27%20or%20Type/Name%20eq%20%27Aggregator%27`,
		"$expand=Type($select=Name),UsrMerchantStatus($select=Name)",
	}

	url_params := strings.Join(s0, "&")

	requestURL := cfg.CRM.URL + MERCHANTS + "?" + url_params

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

	newValues := []Merchant{}
	merchants = data.Value
	for i, v := range merchants {

		ids, err := v.getIDs()
		if err != nil {
			logs.Add(logs.INFO, err)
			continue
		}

		v.fill()

		if len(ids) > 0 {
			for j, id := range ids {
				if j == 0 {
					v.Bof_id = id
				} else {
					new_val := v
					new_val.Bof_id = id
					newValues = append(newValues, new_val)
				}
			}
		}

		merchants[i] = v
	}

	merchants = append(merchants, newValues...)

	logs.Add(logs.MAIN, fmt.Sprintf("Получение merchants: %v [%s строк]", time.Since(start_time), util.FormatInt(len(merchants))))

	return nil
}

func InsertIntoDB_merchants(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Merchant, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	tx, _ := db.Beginx()

	tx.Exec(`DELETE from crm_merchants
	where bof_id IN (
	SELECT bof_id 
	FROM crm_merchants
	where bof_id != 0
	group by bof_id
	having sum(1) > 1)`)

	stat := querrys.Stat_Insert_crm_merchants()
	_, err := tx.PrepareNamed(stat)
	if err != nil {
		logs.Add(logs.INFO, err)
		return
	}

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for v := range channel {

				_, err := tx.NamedExec(stat, v)
				if err != nil {
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (merchant): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Merchant, 0, batch_len)
	for _, v := range merchants {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Merchant, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	tx.Commit()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка merchants crm в Postgres: %v", time.Since(start_time)))
}
