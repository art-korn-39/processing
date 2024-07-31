package provider

import (
	"app/currency"
	"app/logs"
	"fmt"
	"sort"
	"time"
)

type rates []Operation

func (r rates) Sort() {
	sort.Slice(
		r,
		func(i int, j int) bool {
			return r[i].Transaction_completed_at.After(r[j].Transaction_completed_at)
		},
	)
}

type key_fields struct {
	transaction_completed_at time.Time
	operation_type           string
	country                  string
	payment_type             string
	merchant_name            string
	channel_currency         currency.Currency
	provider_currency        currency.Currency
}

func newKeyFields(r Operation) key_fields {
	return key_fields{
		transaction_completed_at: r.Transaction_completed_at,
		country:                  r.Country,
		payment_type:             r.Payment_type,
		merchant_name:            r.Merchant_name,
		operation_type:           r.Operation_type,
		channel_currency:         r.Channel_currency,
		provider_currency:        r.Provider_currency,
	}
}

type sum_fields struct {
	count_operations int
	rate             float64
}

func (sf *sum_fields) AddValues(r Operation) {
	sf.count_operations = sf.count_operations + 1
	sf.rate = sf.rate + r.Rate
}

func (r rates) Group() (res rates) {

	start_time := time.Now()

	group_Data := map[key_fields]sum_fields{}
	for _, r := range r {
		kf := newKeyFields(r) // получили структуру с полями группировки
		sf := group_Data[kf]  // получили текущие агрегатные данные по ним
		sf.AddValues(r)       // увеличили агрегатные данные на значения тек. операции
		group_Data[kf] = sf   // положили обратно в мапу
	}

	// обратно собираем массив из операций провайдера
	res = make([]Operation, 0, len(group_Data))
	for k, v := range group_Data {
		op := Operation{
			Transaction_completed_at: k.transaction_completed_at,
			Country:                  k.country,
			Payment_type:             k.payment_type,
			Merchant_name:            k.merchant_name,
			Operation_type:           k.operation_type,
			Channel_currency:         k.channel_currency,
			Provider_currency:        k.provider_currency,
			Rate:                     v.rate / float64(v.count_operations),
		}
		res = append(res, op)
	}

	logs.Add(logs.INFO, fmt.Sprintf("Группировка курсов валют: %v [%d строк]", time.Since(start_time), len(res)))

	return

}
