package crm_chargeback

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
	CHARGEBACK = "/0/OData/UsrChargeback"
	MATCH      = "/0/OData/PspOperationInReqestDispute"
	OPERATION  = "/0/OData/PspProcessingOperation"

	time_layout = "2006-01-02T15:04:05Z"
)

var (
	_1dec22 = time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC)
	_1feb23 = time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)
	_3feb23 = time.Date(2023, 2, 3, 0, 0, 0, 0, time.UTC)
	_1dec23 = time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)

	_29dec23 = time.Date(2023, 12, 29, 0, 0, 0, 0, time.UTC)
	_30dec23 = time.Date(2023, 12, 30, 0, 0, 0, 0, time.UTC)

	_1jan24 = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_1feb25 = time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	_1mar25 = time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)

	_1feb26 = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
)

func loadChargebacks(cfg config.Config, token string) error {

	start_time := time.Now()

	type Response struct {
		Value []Chargeback `json:"value"`
	}

	s0 := []string{}
	s1 := []string{
		"$expand=UsrChargebackMerchant($select=Name,PspMechantProcessingId)",
		"UsrOperationPaymentProvider($select=UsrName,PspProviderId)",
		"UsrChargebackStatus($select=name)",
		"UsrChargebackCodeReason($select=name)",
		"UsrChargebackProcessingBrand($select=name)",
	}
	s0 = append(s0, strings.Join(s1, ","))
	if cfg.Full_loading {
		s0 = append(s0, "$filter=ModifiedOn+gt+@date")
		s0 = append(s0, "@date="+time.Now().AddDate(0, -12, 0).Format(time_layout))
	} else {
		s0 = append(s0, "$filter=ModifiedOn+gt+@date")
		s0 = append(s0, "@date="+time.Now().AddDate(0, -1, 0).Format(time_layout))
	}
	url_params := strings.Join(s0, "&")

	requestURL := cfg.CRM.URL + CHARGEBACK + "?" + url_params

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

	chargebacks = map[string]*Chargeback{}

	for _, v := range data.Value {
		v.fill()
		chargebacks[v.ID] = &v
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Получение chargebacks: %v [%s строк]", time.Since(start_time), util.FormatInt(len(chargebacks))))

	return nil
}

func loadOperations(cfg config.Config, token string) error {

	start_time := time.Now()

	slice_periods := []util.Period{
		{
			StartDay: time.Now().AddDate(0, -1, 0),
			EndDay:   time.Now(),
		},
	}

	if cfg.Full_loading {
		slice_periods = []util.Period{}
		// slice_periods = append(slice_periods, util.GetSliceOfDuration(_1dec22, _1feb23, time.Hour*24*31)...)
		// slice_periods = append(slice_periods, util.GetSliceOfDuration(_1feb23, _3feb23, time.Hour*24)...)
		// slice_periods = append(slice_periods, util.GetSliceOfDuration(_3feb23, _29dec23, time.Hour*24*30)...)

		// slice_periods = append(slice_periods, util.GetSliceOfDuration(_29dec23, _30dec23, time.Hour*1)...)

		// slice_periods = append(slice_periods, util.GetSliceOfDuration(_30dec23, _1feb25, time.Hour*24*30)...)
		// slice_periods = append(slice_periods, util.GetSliceOfDuration(_1feb25, _1mar25, time.Hour*24)...)
		//slice_periods = append(slice_periods, util.GetSliceOfDuration(_30dec23, time.Now(), time.Hour*24*3)...)

		slice_periods = append(slice_periods, util.GetSliceOfDuration(_1mar25, time.Now(), time.Hour*24*30)...)
	}

	operations = map[string]*Operation{}
	dispute_map = map[string]Dispute{}

	for _, period := range slice_periods {
		err := getOperationsForPeriod(cfg, token, period.StartDay, period.EndDay)
		if err != nil {
			return err
		}
		err = getDisputeForPeriod(cfg, token, period.StartDay, period.EndDay)
		if err != nil {
			return err
		}
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Получение операций: %v [%s строк]", time.Since(start_time), util.FormatInt(len(operations))))

	logs.Add(logs.MAIN, fmt.Sprintf("Загрузка dispute: %v [%s строк]", time.Since(start_time), util.FormatInt(len(dispute_map))))

	return nil
}

func getOperationsForPeriod(cfg config.Config, token string, date_start, date_end time.Time) error {

	type Response struct {
		Value []Operation `json:"value"`
	}

	s0 := []string{
		fmt.Sprint("$select=id,operationid,createdon,modifiedon,rrn,receiptdate,providerpaymentid,",
			"accountnumber,amount,channelamount,amountusd,channelamountusd,amountanalyticcurrency,",
			"channelamountanalyticcurrency"),
	}
	s1 := []string{
		"$expand=Merchant($select=Name,PspMechantProcessingId)",
		"PaymentMethodType($select=name,bofid)",
		"PspMerchantAccount($select=name,number)",
		"MerchantProcessingProject($select=UsrMerchantProcessingProjectName,PspMerchantProcessingProjectId)",
		"OperationPaymentProvider($select=UsrName,PspProviderId)",
		"ChannelCurrency($select=Alpha3Code)",
		"Type($select=name)",
		"TransactionStatus($select=name)",
	}
	s0 = append(s0, strings.Join(s1, ","))

	s0 = append(s0, "$filter=ModifiedOn+ge+@date1+and+ModifiedOn+le+@date2")
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
		return fmt.Errorf("[d1: %s | d2: %s] wrong response status: %s, error: %s", date_start.Format(time_layout), date_end.Format(time_layout), resp.Status, getErrorFromBody(body))
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
		operations[v.GUID] = &v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Загружены операции: %s [%s -> %s]",
		util.FormatInt(len(data.Value)),
		date_start.Format(time.DateOnly),
		date_end.Format(time.DateOnly)))

	return nil
}

func getDisputeForPeriod(cfg config.Config, token string, date_start, date_end time.Time) error {

	type Response struct {
		Value []Dispute `json:"value"`
	}

	s0 := []string{
		"$select=operationid,chargebackid,state,statechangedate,createdon,modifiedon",
	}

	s0 = append(s0, "$expand=state($select=Name)")
	s0 = append(s0, "$filter=CreatedOn+ge+@date1+and+CreatedOn+le+@date2")
	s0 = append(s0, "@date1="+date_start.Format(time_layout))
	s0 = append(s0, "@date2="+date_end.Format(time_layout))

	url_params := strings.Join(s0, "&")

	requestURL := cfg.CRM.URL + MATCH + "?" + url_params

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
		return fmt.Errorf("[d1: %s | d2: %s] wrong response status: %s, error: %s", date_start.Format(time_layout), date_end.Format(time_layout), resp.Status, getErrorFromBody(body))
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
		dispute_map[v.Operation_guid] = v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Загружены мэтчи: %s [%s -> %s]",
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

	r, _ = regexp.Compile(`internalexception\":{\"message.*PspProcessingOperation`)
	res = r.FindString(str)
	if res != "" {
		return res
	}

	return str
}
