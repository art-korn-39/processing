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
	PROVIDERS = "/0/OData/PspPaymentProvider"
)

var providers []Provider

type Provider struct {
	Id           string `json:"id" db:"id"`
	Name         string `json:"name" db:"name"`
	Manager_id   string `db:"manager_id"`
	Manager_name string `db:"manager_name"`
	Owner_id     string `db:"owner_id"`
	Owner_name   string `db:"owner_name"`
	Status_str   string `db:"status"`

	// вложенные структуры json файла
	// перекладываем их значения на верхний уровень
	Manager map[string]string `json:"manager"` //Name
	Owner   map[string]string `json:"owner"`   //Name
	Status  map[string]string `json:"Status"`  //Name

}

func (p *Provider) fill() {
	p.Manager_id = p.Manager["Id"]
	p.Manager_name = p.Manager["Name"]
	p.Owner_id = p.Owner["Id"]
	p.Owner_name = p.Owner["Name"]
	p.Status_str = p.Status["Name"]
}

func load_providers(cfg config.Config, token string) error {

	start_time := time.Now()

	type Response struct {
		Value []Provider `json:"value"`
	}

	s0 := []string{
		"$select=Id,Name",
		"$expand=Manager($select=Id,Name),Owner($select=Id,Name),Status($select=Name)",
	}

	url_params := strings.Join(s0, "&")

	requestURL := cfg.CRM.URL + PROVIDERS + "?" + url_params

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

	providers = data.Value
	for i, v := range providers {
		v.fill()
		providers[i] = v
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Получение providers: %v [%s строк]", time.Since(start_time), util.FormatInt(len(providers))))

	return nil
}

func InsertIntoDB_providers(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Provider, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_crm_providers()
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
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (provider): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Provider, 0, batch_len)
	for _, v := range providers {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Provider, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка providers crm в Postgres: %v", time.Since(start_time)))
}
