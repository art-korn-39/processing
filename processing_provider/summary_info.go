package processing_provider

import (
	"app/config"
	"app/currency"
	"app/dragonpay"
	"app/logs"
	"app/provider_balances"
	"app/tariff_provider"
	"app/teams_tradex"
	"app/util"
	"fmt"
	"time"
)

type SumFileds struct {
	count_operations          int
	balance_amount            float64
	BR_balance_currency       float64
	Extra_BR_balance_currency float64
	BR_compensation           float64
	channel_amount            float64
	surcharge_amount          float64
	RR_amount                 float64
}

func (sf *SumFileds) AddValues(o *Operation) {
	sf.count_operations = sf.count_operations + o.Count_operations
	sf.balance_amount = sf.balance_amount + o.Balance_amount
	sf.BR_balance_currency = sf.BR_balance_currency + o.BR_balance_currency
	sf.Extra_BR_balance_currency = sf.Extra_BR_balance_currency + o.Extra_BR_balance_currency
	sf.BR_compensation = sf.BR_compensation + o.BR_Compensation
	sf.channel_amount = sf.channel_amount + o.Channel_amount
	sf.surcharge_amount = sf.surcharge_amount + o.Surcharge_amount
	sf.RR_amount = sf.RR_amount + o.RR_amount
}

func (sf *SumFileds) RoundValues(balance_currency currency.Currency) {
	sf.balance_amount = util.Round(sf.balance_amount, balance_currency.GetAccuracy(2))                       //2
	sf.BR_balance_currency = util.Round(sf.BR_balance_currency, balance_currency.GetAccuracy(4))             //4
	sf.Extra_BR_balance_currency = util.Round(sf.Extra_BR_balance_currency, balance_currency.GetAccuracy(4)) //4
	sf.BR_compensation = util.Round(sf.BR_compensation, 2)
	sf.channel_amount = util.Round(sf.channel_amount, 2)
	sf.surcharge_amount = util.Round(sf.surcharge_amount, 2)
	sf.RR_amount = util.Round(sf.RR_amount, 2)
}

type KeyFields_SummaryInfo struct {
	balance      string
	organization string
	id_revise    string

	document_date time.Time
	provider_name string
	verification  string
	//verification_tradex   string
	operation_type        string
	operation_group       string
	payment_type          string
	merchant_account_name string
	merchant_name         string
	region                string
	channel_currency      currency.Currency
	balance_currency      currency.Currency
	tariff                tariff_provider.Tariff
	contractor_provider   string
	contractor_merchant   string
	project_name          string
	project_id            int
	project_id_tradex     int
	provider1c            string
	subdivision_name      string
	RR_date               time.Time
	isDragonpay           bool
	isTradex              bool
	isTestId              int
	team_tradex           string
}

func NewKeyFields_SummaryInfo(o *Operation) (KF KeyFields_SummaryInfo) {
	KF = KeyFields_SummaryInfo{
		document_date:         o.Document_date,
		provider_name:         o.Provider_name,
		verification:          o.Verification,
		operation_type:        o.Operation_type,
		operation_group:       o.Operation_group,
		payment_type:          o.Payment_type,
		merchant_name:         o.Merchant_name,
		merchant_account_name: o.Merchant_account_name,
		region:                o.Country.Region,
		channel_currency:      o.Channel_currency,
		balance_currency:      o.Balance_currency,
		RR_date:               o.RR_date,
		isDragonpay:           o.IsDragonPay,
		isTestId:              o.IsTestId,
		isTradex:              o.IsTradex,
		project_id_tradex:     o.Project_id,
		//verification_tradex:   o.VerificationTradex,
	}

	if o.Tariff != nil {
		KF.tariff = *o.Tariff
	}

	if o.ProviderBalance != nil {

		KF.id_revise = o.ProviderBalance.Balance_code
		KF.balance = o.ProviderBalance.Name
		KF.organization = o.ProviderBalance.Legal_entity
		KF.contractor_provider = util.TR(o.ProviderBalance.Nickname == "", o.ProviderBalance.Contractor, o.ProviderBalance.Nickname).(string)
		KF.subdivision_name = o.ProviderBalance.Subdivision_name
		//KF.balance_currency = o.ProviderBalance.Balance_currency

	} else if o.IsTradex && o.ProviderOperation != nil {

		team, ok := teams_tradex.GetTeamByName(o.ProviderOperation.Team)
		if ok {
			KF.balance = team.Balance_name
			providerBalance, ok := provider_balances.GetBalanceByGUID(team.Balance_guid)
			if ok {
				KF.id_revise = providerBalance.Balance_code
				KF.organization = providerBalance.Legal_entity
				KF.contractor_provider = util.TR(providerBalance.Nickname == "", providerBalance.Contractor, providerBalance.Nickname).(string)
			}
		}

	}

	if o.ProviderOperation != nil {
		KF.team_tradex = o.ProviderOperation.Team
	}

	if o.Merchant != nil {
		KF.contractor_merchant = o.Merchant.Contractor_name
	} else { // если пустой мерчант 1С, то запишем данные по проекту, чтобы проще отследить было
		KF.project_name = o.Project_name
		KF.project_id = o.Project_id
	}

	if o.IsDragonPay {
		if o.DragonpayOperation != nil {
			KF.provider1c = dragonpay.GetProvider1C(o.DragonpayOperation.Endpoint_id)
		} else {
			KF.provider1c = dragonpay.GetProvider1C(o.Endpoint_id)
		}
	} else {
		KF.provider1c = o.Provider1c
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
		//sf.RoundValues(operation.Balance_currency) // снова избавляемся от погрешностей
		group_data[kf] = sf // положили обратно в мапу
	}

	for k, v := range group_data {
		group_data[k] = v
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка в данные Excel: %v", util.FormatDuration(time.Since(start_time))))

	return

}
