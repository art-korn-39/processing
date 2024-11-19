package chargeback

import (
	"app/config"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

const (
	AUTH = "/ServiceModel/AuthService.svc/Login"
)

func auth(cfg config.Config) (string, error) {

	body_struct := struct {
		UserName     string `json:"UserName"`
		UserPassword string `json:"UserPassword"`
	}{
		UserName:     cfg.CRM.Login,
		UserPassword: cfg.CRM.Password,
	}

	body_json, _ := json.Marshal(body_struct)

	url := cfg.CRM.URL + AUTH
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body_json))

	req.Header.Add("Content-Type", "application/json; charset=utf-8")
	req.Header.Add("ForceUseSession", "true")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ошибка аутентификации, статус: %s", resp.Status)
	}

	var token string
	cookies := resp.Cookies()
	for _, v := range cookies {
		if v.Name == ".ASPXAUTH" {
			token = v.Value
		}
	}

	if token == "" {
		return "", errors.New("не найден токен аутентификации")
	}

	return token, nil

}
