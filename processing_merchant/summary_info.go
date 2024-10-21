package processing_merchant

import (
	"app/config"
	"app/currency"
	"app/logs"
	"app/tariff_merchant"
	"fmt"
	"time"
)

type SumFileds struct {
	count_operations       int
	channel_amount         float64
	balance_amount         float64
	fee_amount             float64
	SR_channel_currency    float64
	SR_balance_currency    float64
	checkFee               float64
	checkRates             float64
	RR_amount              float64
	hold_amount            float64
	CompensationRC         float64
	CompensationBC         float64
	BalanceRefund_turnover float64
	BalanceRefund_fee      float64
	Surcharge_amount       float64
}

func (sf *SumFileds) AddValues(o *Operation) {
	sf.count_operations = sf.count_operations + o.Count_operations
	sf.channel_amount = sf.channel_amount + o.Channel_amount
	sf.balance_amount = sf.balance_amount + o.Balance_amount
	sf.fee_amount = sf.fee_amount + o.Fee_amount
	sf.SR_channel_currency = sf.SR_channel_currency + o.SR_channel_currency
	sf.SR_balance_currency = sf.SR_balance_currency + o.SR_balance_currency
	sf.checkFee = sf.checkFee + o.CheckFee
	sf.checkRates = sf.checkRates + o.CheckRates
	sf.RR_amount = sf.RR_amount + o.RR_amount
	sf.hold_amount = sf.hold_amount + o.hold_amount
	sf.CompensationBC = sf.CompensationBC + o.CompensationBC
	sf.CompensationRC = sf.CompensationRC + o.CompensationRC
	sf.BalanceRefund_turnover = sf.BalanceRefund_turnover + o.Channel_amount - o.Actual_amount
	sf.Surcharge_amount = sf.Surcharge_amount + o.Surcharge_amount
}

func (sf *SumFileds) AddValuesFromSF(sf2 SumFileds) {
	sf.count_operations = sf.count_operations + sf2.count_operations
	sf.channel_amount = sf.channel_amount + sf2.channel_amount
	sf.balance_amount = sf.balance_amount + sf2.balance_amount
	sf.fee_amount = sf.fee_amount + sf2.fee_amount
	sf.SR_channel_currency = sf.SR_channel_currency + sf2.SR_channel_currency
	sf.SR_balance_currency = sf.SR_balance_currency + sf2.SR_balance_currency
	sf.checkFee = sf.checkFee + sf2.checkFee
	sf.checkRates = sf.checkRates + sf2.checkRates
	sf.RR_amount = sf.RR_amount + sf2.RR_amount
	sf.hold_amount = sf.hold_amount + sf2.hold_amount
	sf.CompensationBC = sf.CompensationBC + sf2.CompensationBC
	sf.CompensationRC = sf.CompensationRC + sf2.CompensationRC
	sf.BalanceRefund_turnover = sf.BalanceRefund_turnover + sf2.BalanceRefund_turnover
	sf.Surcharge_amount = sf.Surcharge_amount + sf2.Surcharge_amount
}

func (sf *SumFileds) SetBalanceRefund(convertation string, percent float64) {
	if convertation == "Частичные выплаты" {
		if sf.BalanceRefund_turnover != 0 {
			sf.BalanceRefund_fee = sf.BalanceRefund_turnover * percent
		}
	} else {
		sf.BalanceRefund_turnover = 0
		sf.BalanceRefund_fee = 0
	}
}

type KeyFields_SummaryInfo struct {
	document_date  time.Time
	balance_id     int
	balance_id_str string

	verification          string
	operation_type        string
	country               string
	payment_type          string
	merchant_name         string
	project_name          string
	merchant_account_name string
	balance_name          string
	merchant_account_id   int
	tariff_condition_id   int

	channel_currency currency.Currency
	balance_currency currency.Currency

	tariff     tariff_merchant.Tariff
	tariff_bof tariff_merchant.Tariff

	contract_id    int //???
	RR_date        time.Time
	hold_date      time.Time
	crypto_network string
	provider1c     string
}

func NewKeyFields_SummaryInfo(o *Operation) (KF KeyFields_SummaryInfo) {
	KF = KeyFields_SummaryInfo{
		document_date:         o.Document_date,
		balance_id:            o.Balance_id,
		balance_id_str:        fmt.Sprint(o.Balance_id),
		verification:          o.Verification,
		operation_type:        o.Operation_type,
		country:               o.Country,
		payment_type:          o.Payment_type,
		merchant_name:         o.Merchant_name,
		project_name:          o.Project_name,
		merchant_account_name: o.Merchant_account_name,
		merchant_account_id:   o.Merchant_account_id,
		tariff_condition_id:   o.Tariff_condition_id,
		channel_currency:      o.Channel_currency,
		balance_currency:      o.Balance_currency,
		crypto_network:        o.Crypto_network,
		RR_date:               o.RR_date,
		hold_date:             o.hold_date,
		provider1c:            o.Provider1c,
	}

	if o.Tariff != nil {
		KF.tariff = *o.Tariff
		KF.balance_id_str = fmt.Sprint(o.Balance_id, "_", o.Tariff.Balance_type)

		if o.Tariff.Convertation == "KGX" {
			if o.ProviderOperation != nil {
				KF.balance_name = o.Provider_name // тут уже лежит баланс из реестра провайдера
			}
		} else {
			KF.balance_name = o.Tariff.Balance_name
		}
	}

	if o.Tariff_bof != nil {
		KF.tariff_bof = *o.Tariff_bof
	}

	return

}

func GroupRegistryToSummaryInfo() (group_data map[KeyFields_SummaryInfo]SumFileds) {

	if !config.Get().SummaryInfo.Usage {
		return
	}

	start_time := time.Now()

	group_data = map[KeyFields_SummaryInfo]SumFileds{}
	for _, operation := range storage.Registry {
		kf := NewKeyFields_SummaryInfo(operation) // получили структуру с полями группировки
		sf := group_data[kf]                      // получили текущие агрегатные данные по ним
		sf.AddValues(operation)                   // увеличили агрегатные данные на значения тек. операции
		group_data[kf] = sf                       // положили обратно в мапу
	}

	for k, v := range group_data {
		v.checkRates = v.checkRates / float64(v.count_operations)
		v.SetBalanceRefund(k.tariff.Convertation, k.tariff.Percent)
		group_data[k] = v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в данные Excel: %v", time.Since(start_time)))

	return

}
