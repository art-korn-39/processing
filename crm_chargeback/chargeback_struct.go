package crm_chargeback

import (
	"app/util"
	"strconv"
	"strings"
	"time"
)

type Chargeback struct {
	ID             string    `json:"id" db:"id"`
	Name           string    `json:"usrname" db:"name"`
	Case_ID        string    `json:"usrcaseid" db:"case_id"`
	Created_on     time.Time `json:"createdon" db:"created_on"`
	Total_amount   float64   `json:"usrchargebacktotalamount" db:"total_amount"`
	Account_number string    `json:"usrchargebackaccountnumber" db:"account_number"`
	Deadline       time.Time `json:"usrchargebackdeadline" db:"deadline"`
	Receipt_date   time.Time `json:"pspreceiptdate" db:"receipt_date"`

	Status        string `db:"status"`
	Brand         string `db:"brand"`
	Code_reason   string `db:"code_reason"`
	Merchant_id   int    `db:"merchant_id"`
	Merchant_name string `db:"merchant_name"`
	Provider_id   int    `db:"provider_id"`
	Provider_name string `db:"provider_name"`

	// вложенные структуры json файла
	// перекладываем их значения на верхний уровень
	Merchant        map[string]string `json:"UsrChargebackMerchant"`
	Provider        map[string]string `json:"UsrOperationPaymentProvider"`
	Status_map      map[string]string `json:"UsrChargebackStatus"`
	Code_reason_map map[string]string `json:"UsrChargebackCodeReason"`
	Brand_map       map[string]string `json:"UsrChargebackProcessingBrand"`
}

func (c *Chargeback) fill() {

	c.Merchant_id = getID(c.Merchant["PspMechantProcessingId"])
	c.Merchant_name = c.Merchant["Name"]
	c.Provider_id = getID(c.Provider["PspProviderId"])
	c.Provider_name = c.Provider["UsrName"]
	c.Status = c.Status_map["Name"]
	c.Code_reason = c.Code_reason_map["Name"]
	c.Brand = c.Brand_map["Name"]
}

func getID(is_str string) int {

	MAX_INTEGER := 2147483647

	ids_str := strings.Trim(is_str, " ")
	if ids_str == "" {
		return 0
	}

	s := strings.Split(ids_str, ",")
	if len(s) == 0 {
		return 0
	}

	s = util.Compact(s)

	for _, v := range s {
		num, err := strconv.Atoi(strings.Trim(v, " "))
		if err != nil {
			return 0
		} else {
			if num <= MAX_INTEGER {
				return num
			}
		}
	}

	return 0

}
