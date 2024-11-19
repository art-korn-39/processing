package chargeback

import (
	"strconv"
	"time"
)

type Operation struct {
	GUID                    string    `json:"id" db:"guid"`
	ID_str                  string    `json:"operationid"`
	ID                      int       `db:"id"`
	Created_on              time.Time `json:"createdon" db:"created_on"`
	Modified_on             time.Time `json:"modifiedon" db:"modified_on"`
	Rrn                     string    `json:"rrn" db:"rrn"`
	Receipt_date            time.Time `json:"receiptdate" db:"receipt_date"`
	Provider_payment_id_str string    `json:"providerpaymentid"`
	Provider_payment_id     int       `db:"provider_payment_id"`
	Account_number          string    `json:"accountnumber" db:"account_number"`

	Project_id   int    `db:"project_id"`
	Project_name string `db:"project_name"`

	Merchant_id   int    `db:"merchant_id"`
	Merchant_name string `db:"merchant_name"`

	Provider_id   int    `db:"provider_id"`
	Provider_name string `db:"provider_name"`

	Merchant_account_id   int    `db:"merchant_account_id"`
	Merchant_account_name string `db:"merchant_account_name"`

	Payment_type_id   int    `db:"payment_type_id"`
	Payment_type_name string `db:"payment_type_name"`

	Amount         float64 `json:"amount" db:"amount"`
	Channel_amount float64 `json:"channelamount" db:"channel_amount"`

	Amount_usd         float64 `json:"amountusd" db:"amount_usd"`
	Channel_amount_usd float64 `json:"channelamountusd" db:"channel_amount_usd"`

	Amount_rub         float64 `json:"amountanalyticcurrency" db:"amount_rub"`
	Channel_amount_rub float64 `json:"channelamountanalyticcurrency" db:"channel_amount_rub"`

	Type_name             string `db:"type"`
	Channel_currency_name string `db:"channel_currency"`
	Transaction_status    string `db:"transaction_status"`

	Chargeback_id          string    `db:"chargeback_id"`
	Chargeback_case_id     string    `db:"chargeback_case_id"`
	Chargeback_status      string    `db:"chargeback_status"`
	Chargeback_deadline    time.Time `db:"chargeback_deadline"`
	Chargeback_code_reason string    `db:"chargeback_code_reason"`

	// вложенные структуры json файла
	// перекладываем их значения на верхний уровень
	Merchant          map[string]string `json:"merchant"`                  //Name, PspMechantProcessingId
	Provider          map[string]string `json:"operationpaymentprovider"`  //UsrName, PspProviderId
	Merchant_account  map[string]string `json:"pspmerchantaccount"`        //Name, Number
	Payment_type      map[string]string `json:"paymentmethodtype"`         //Name, bofid
	Project           map[string]string `json:"merchantprocessingproject"` //UsrMerchantProcessingProjectName, PspMerchantProcessingProjectId
	Channel_currency  map[string]string `json:"channelcurrency"`           //Alpha3Code
	Type              map[string]string `json:"type"`                      //Name
	TransactionStatus map[string]string `json:"transactionstatus"`         //Name
}

func (op *Operation) fill() {
	op.ID, _ = strconv.Atoi(op.ID_str)
	op.Provider_payment_id, _ = strconv.Atoi(op.Provider_payment_id_str)
	op.Merchant_id, _ = strconv.Atoi(op.Merchant["PspMechantProcessingId"])
	op.Merchant_name = op.Merchant["Name"]
	op.Provider_id, _ = strconv.Atoi(op.Provider["PspProviderId"])
	op.Provider_name = op.Provider["UsrName"]
	op.Merchant_account_id, _ = strconv.Atoi(op.Merchant_account["Number"])
	op.Merchant_account_name = op.Merchant_account["Name"]
	op.Project_id, _ = strconv.Atoi(op.Project["PspMerchantProcessingProjectId"])
	op.Project_name = op.Project["UsrMerchantProcessingProjectName"]
	op.Payment_type_id, _ = strconv.Atoi(op.Payment_type["BOFid"])
	op.Payment_type_name = op.Payment_type["Name"]
	op.Type_name = op.Type["Name"]
	op.Channel_currency_name = op.Channel_currency["Alpha3Code"]
	op.Transaction_status = op.TransactionStatus["Name"]
}
