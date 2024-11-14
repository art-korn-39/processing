package crm_dictionary

import (
	"app/config"
	"app/logs"
	"app/util"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

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

func loadPaymentType(cfg config.Config, token string) error {

	start_time := time.Now()

	type Response struct {
		Value []Payment_type `json:"value"`
	}

	url_params := "$select=id,name,BOFid,MethodId"

	requestURL := cfg.CRM.URL + PAYMENT_TYPE + "?" + url_params

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

	payment_types = data.Value
	for i, v := range payment_types {
		v.Bof_id, _ = strconv.Atoi(v.Bof_id_str)
		payment_types[i] = v
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Получение payment_type: %v [%s строк]", time.Since(start_time), util.FormatInt(len(payment_types))))

	return nil
}
