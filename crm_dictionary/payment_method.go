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
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	PAYMENT_METHOD = "/0/OData/UsrProcessingPaymentMethod"
)

var payment_methods []Payment_method

// id,ParentId,UsrProcessingPaymentMethodId,Name
type Payment_method struct {
	Id        string `json:"Id" db:"id"`
	Parent_id string `json:"ParentId" db:"parent_id"`
	Bof_id    int    `json:"UsrProcessingPaymentMethodId" db:"bof_id"`
	Name      string `json:"Name" db:"name"`
}

func loadPaymentMethod(cfg config.Config, token string) error {

	start_time := time.Now()

	type Response struct {
		Value []Payment_method `json:"value"`
	}

	url_params := "$select=id,ParentId,UsrProcessingPaymentMethodId,Name"

	requestURL := cfg.CRM.URL + PAYMENT_METHOD + "?" + url_params

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

	payment_methods = data.Value

	logs.Add(logs.MAIN, fmt.Sprintf("Получение payment_method: %v [%s строк]", time.Since(start_time), util.FormatInt(len(payment_methods))))

	return nil
}

func paymentMethodInsertIntoDB(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Payment_method, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_crm_payment_method()
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
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (payment_method): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Payment_method, 0, batch_len)
	for _, v := range payment_methods {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Payment_method, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка payment_method в Postgres: %v", time.Since(start_time)))
}
