package crm_provider_losses

import (
	"app/config"
	"app/logs"
	"app/util"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	OPERATION   = "/0/OData/PspLossInProvider"
	time_layout = "2006-01-02T15:04:05Z"
)

var (
	inception_date = time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC)
)

func loadOperations(cfg config.Config, token string) error {

	start_time := time.Now()

	slice_periods := []util.Period{
		{
			StartDay: time.Now().AddDate(0, -1, 0),
			EndDay:   time.Now(),
		},
	}

	if cfg.Full_loading {
		slice_periods = util.GetSliceOfDuration(inception_date, time.Now(), time.Hour*24*30)
	}

	operations = []*Operation{}

	for _, period := range slice_periods {
		err := getOperationsForPeriod(cfg, token, period.StartDay, period.EndDay)
		if err != nil {
			return err
		}
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Получение операций: %v [%s строк]", time.Since(start_time), util.FormatInt(len(operations))))

	return nil
}

func getOperationsForPeriod(cfg config.Config, token string, date_start, date_end time.Time) error {

	type Response struct {
		Value []Operation `json:"value"`
	}

	s0 := []string{
		// fmt.Sprint("$select=id,operationid,createdon,modifiedon,rrn,receiptdate,providerpaymentid,",
		// 	"accountnumber,amount,channelamount,amountusd,channelamountusd,amountanalyticcurrency,",
		// 	"channelamountanalyticcurrency"),
	}
	s1 := []string{
		"$expand=Status($select=name)",
		"LossType($select=name)",
		"LossPotentialStatus($select=name)",
		"Currency($select=Alpha3Code)",
	}
	s0 = append(s0, strings.Join(s1, ","))

	s0 = append(s0, "$filter=CreatedOn+ge+@date1+and+CreatedOn+le+@date2")
	s0 = append(s0, "@date1="+date_start.Format(time_layout))
	s0 = append(s0, "@date2="+date_end.Format(time_layout))

	url_params := strings.Join(s0, "&")

	requestURL := cfg.CRM.URL + OPERATION + "?" + url_params

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
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("wrong response status: %s, error: %s", resp.Status, getErrorFromBody(body))
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

	for _, v := range data.Value {
		v.fill()
		operations = append(operations, &v)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Загружены операции: %s [%s -> %s]",
		util.FormatInt(len(data.Value)),
		date_start.Format(time.DateOnly),
		date_end.Format(time.DateOnly)))

	return nil
}

func getErrorFromBody(body []byte) string {
	str := string(body)
	r, _ := regexp.Compile("<p>.*</p>")
	res := r.FindString(str)
	if res != "" {
		return res
	}

	r, _ = regexp.Compile(`internalexception\":{\"message.*PspLossInProvider`)
	res = r.FindString(str)
	if res != "" {
		return res
	}
	return ""
}
