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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	PROVIDER_SOLUTIONS = "/0/OData/PspTechProviderInProviderSolution"
)

var provider_solutions []Provider_solution

type Provider_solution struct {
	Id                      string `db:"id"`
	Solution_name           string `db:"solution_name"`
	Provider_id             string `db:"provider_id"`
	Provider_name           string `db:"provider_name"`
	Provider_id_bof         int    `db:"provider_id_bof"`
	Provider_name_bof       string `db:"provider_name_bof"`
	Payment_method_id_bof   int    `db:"payment_method_id_bof"`
	Payment_method_name_bof string `db:"payment_method_name_bof"`

	// вложенные структуры json файла
	// перекладываем их значения на верхний уровень
	Solution                 Solution                 `json:"solution"`
	OperationPaymentProvider OperationPaymentProvider `json:"OperationPaymentProvider"`
}

type Solution struct {
	Id       string   `json:"Id"`
	Name     string   `json:"Name"`
	Provider Provider `json:"Provider"`
}

type OperationPaymentProvider struct {
	Id             string         `json:"PspProviderId"`
	Name           string         `json:"UsrName"`
	Payment_method Payment_method `json:"UsrPaymentMethod"`
}

func (ps *Provider_solution) fill() {
	ps.Id = ps.Solution.Id
	ps.Solution_name = ps.Solution.Name
	ps.Provider_id = ps.Solution.Provider.Id
	ps.Provider_name = ps.Solution.Provider.Name
	ps.Provider_id_bof, _ = strconv.Atoi(ps.OperationPaymentProvider.Id)
	ps.Provider_name_bof = ps.OperationPaymentProvider.Name
	ps.Payment_method_id_bof = ps.OperationPaymentProvider.Payment_method.Bof_id
	ps.Payment_method_name_bof = ps.OperationPaymentProvider.Payment_method.Name
}

func load_provider_solutions(cfg config.Config, token string) error {

	start_time := time.Now()

	type Response struct {
		Value []Provider_solution `json:"value"`
	}

	s0 := []string{
		"$select=OperationPaymentProvider",
		"$expand=Solution($select=Id,name,Provider;$expand=Provider($select=Id,Name)),OperationPaymentProvider($select=PspProviderId,UsrName;$expand=UsrPaymentMethod($select=Name,UsrProcessingPaymentMethodId))",
	}

	url_params := strings.Join(s0, "&")

	requestURL := cfg.CRM.URL + PROVIDER_SOLUTIONS + "?" + url_params

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

	provider_solutions = data.Value
	for i, v := range provider_solutions {
		v.fill()
		provider_solutions[i] = v
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Получение provider solutions: %v [%s строк]", time.Since(start_time), util.FormatInt(len(provider_solutions))))

	return nil
}

func InsertIntoDB_provider_solutions(db *sqlx.DB) {

	if db == nil {
		return
	}

	start_time := time.Now()

	channel := make(chan []Provider_solution, 1000)

	const batch_len = 100

	var wg sync.WaitGroup

	stat := querrys.Stat_Insert_crm_provider_solutions()
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
					logs.Add(logs.ERROR, fmt.Sprint("не удалось записать в БД (provider_solutions): ", err))
				}

			}
		}()
	}

	var i int
	batch := make([]Provider_solution, 0, batch_len)
	for _, v := range provider_solutions {
		batch = append(batch, v)
		if (i+1)%batch_len == 0 {
			channel <- batch
			batch = make([]Provider_solution, 0, batch_len)
		}
		i++
	}

	if len(batch) != 0 {
		channel <- batch
	}

	close(channel)

	wg.Wait()

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка provider_solutions crm в Postgres: %v", time.Since(start_time)))
}
