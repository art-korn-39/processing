package processing

import (
	"app/config"
	"app/logs"
	"fmt"
	"time"
)

type SumFileds struct {
	count_operations    int
	channel_amount      float64
	balance_amount      float64
	fee_amount          float64
	SR_channel_currency float64
	SR_balance_currency float64
	checkFee            float64
	PP_amount           float64
}

func (sf *SumFileds) AddValues(o Operation) {
	sf.count_operations = sf.count_operations + o.Count_operations
	sf.channel_amount = sf.channel_amount + o.Channel_amount
	sf.balance_amount = sf.balance_amount + o.Balance_amount
	sf.fee_amount = sf.fee_amount + o.Fee_amount
	sf.SR_channel_currency = sf.SR_channel_currency + o.SR_channel_currency
	sf.SR_balance_currency = sf.SR_balance_currency + o.SR_balance_currency
	sf.checkFee = sf.checkFee + o.CheckFee
	sf.PP_amount = sf.PP_amount + o.PP_amount
}

type KeyFields_SummaryInfo struct {
	document_date         time.Time
	balance_id            int
	balance_name          string
	verification          string //?
	operation_type        string
	country               string
	payment_method_type   string
	merchant_name         string
	project_name          string
	merchant_account_name string
	subdivision1C         string
	provider1C            string
	ratedAccount          string
	channel_currency      Currency
	currencyBP            Currency
	percent               float64
	fix                   float64
	min                   float64
	max                   float64
	range_min             float64
	range_max             float64
	date_start            time.Time
	tariff_condition_id   int
	contract_id           int //???
	PP_rashold            time.Time
	Crypto_network        string
}

func NewKeyFields_SummaryInfo(o Operation) (KF KeyFields_SummaryInfo) {
	KF = KeyFields_SummaryInfo{
		document_date:         o.Document_date,
		balance_id:            o.Balance_id,
		verification:          o.Verification,
		operation_type:        o.Operation_type,
		country:               o.Country,
		payment_method_type:   o.Payment_method_type,
		merchant_name:         o.Merchant_name,
		project_name:          o.Project_name,
		merchant_account_name: o.Merchant_account_name,
		channel_currency:      o.Channel_currency,
	}

	if o.Tariff != nil {
		KF.balance_name = o.Tariff.Balance_name
		KF.subdivision1C = o.Tariff.Subdivision1C
		KF.ratedAccount = o.Tariff.RatedAccount
		KF.provider1C = o.Tariff.Provider1C
		KF.currencyBP = o.Tariff.CurrencyBP
		KF.percent = o.Tariff.Percent
		KF.fix = o.Tariff.Fix
		KF.min = o.Tariff.Min
		KF.max = o.Tariff.Max
		KF.range_min = o.Tariff.RangeMIN
		KF.range_max = o.Tariff.RangeMAX
		KF.date_start = o.Tariff.DateStart
		KF.tariff_condition_id = o.Tariff.id
		KF.PP_rashold = o.Document_date.Add(time.Duration(o.Tariff.PP_days * int(time.Hour) * 24))
		KF.Crypto_network = o.Crypto_network
		//KF.contract_id = o.Tariff.contract_id
	}

	return

}

func GroupRegistryToSummaryInfo() (group_Data map[KeyFields_SummaryInfo]SumFileds) {

	if !config.Get().SummaryInfo.Usage {
		return
	}

	start_time := time.Now()

	group_Data = map[KeyFields_SummaryInfo]SumFileds{}
	for _, operation := range storage.Registry {
		kf := NewKeyFields_SummaryInfo(*operation) // получили структуру с полями группировки
		sf := group_Data[kf]                       // получили текущие агрегатные данные по ним
		sf.AddValues(*operation)                   // увеличили агрегатные данные на значения тек. операции
		group_Data[kf] = sf                        // положили обратно в мапу
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в данные Excel: %v", time.Since(start_time)))

	return

}

type KeyFields_Rates struct {
	transaction_completed_at time.Time
	operation_type           string
	country                  string
	payment_method_type      string
	merchant_name            string
	channel_currency         Currency
	provider_currency        Currency
}

func NewKeyFields_Rates(r ProviderOperation) KeyFields_Rates {
	return KeyFields_Rates{
		transaction_completed_at: r.Transaction_completed_at,
		country:                  r.Country,
		payment_method_type:      r.Payment_method_type,
		merchant_name:            r.Merchant_name,
		operation_type:           r.Operation_type,
		channel_currency:         r.Channel_currency,
		provider_currency:        r.Provider_currency,
	}
}

type SumFileds_Rates struct {
	count_operations int
	rate             float64
}

func (sf *SumFileds_Rates) AddValues(r ProviderOperation) {
	sf.count_operations = sf.count_operations + 1
	sf.rate = sf.rate + r.Rate
}

func GroupRates() (group_Data map[KeyFields_Rates]SumFileds_Rates) {

	start_time := time.Now()

	group_Data = map[KeyFields_Rates]SumFileds_Rates{}
	for _, r := range storage.Rates {
		kf := NewKeyFields_Rates(r) // получили структуру с полями группировки
		sf := group_Data[kf]        // получили текущие агрегатные данные по ним
		sf.AddValues(r)             // увеличили агрегатные данные на значения тек. операции
		group_Data[kf] = sf         // положили обратно в мапу
	}

	// обратно собираем массив из операций провайдера
	storage.Rates = make([]ProviderOperation, 0, len(group_Data))
	for k, v := range group_Data {
		r := ProviderOperation{
			Transaction_completed_at: k.transaction_completed_at,
			Country:                  k.country,
			Payment_method_type:      k.payment_method_type,
			Merchant_name:            k.merchant_name,
			Operation_type:           k.operation_type,
			Channel_currency:         k.channel_currency,
			Provider_currency:        k.provider_currency,
			Rate:                     v.rate / float64(v.count_operations),
		}
		storage.Rates = append(storage.Rates, r)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка курсов валют: %v [%d строк]", time.Since(start_time), len(storage.Rates)))

	return

}
