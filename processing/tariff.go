package processing

import (
	"app/config"
	"app/crypto"
	"app/currency"
	"app/logs"
	"app/util"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Tariff struct {
	Balance_name          string //`xlsx:"1"`
	Merchant              string //`xlsx:"5"`
	Merchant_account_name string //`xlsx:"6"`
	Merchant_account_id   int    //`xlsx:"7"`
	Balance_code          string //`xlsx:"8"`
	Provider              string //`xlsx:"9"`

	Schema         string //`xlsx:"18"`
	IsCrypto       bool
	Convertation   string //`xlsx:"19"`
	Operation_type string //`xlsx:"22"`

	RR_days    int     //`xlsx:"32"`
	RR_percent float64 //`xlsx:"33"`

	Subdivision1C string //`xlsx:"36"`
	Provider1C    string //`xlsx:"37"`
	RatedAccount  string //`xlsx:"38"`
	Balance_id    int    //`xlsx:"39"`
	Balance_type  string //`xlsx:"40"`
	id            int    //`xlsx:"46"`

	DateStartPS time.Time         //`xlsx:"12"`
	CurrencyBM  currency.Currency //`xlsx:"15"`
	CurrencyBP  currency.Currency //`xlsx:"16"`
	DateStart   time.Time         //`xlsx:"17"`
	RangeMIN    float64           //`xlsx:"20"`
	RangeMAX    float64           //`xlsx:"21"`
	Percent     float64           //`xlsx:"23"`
	Fix         float64           //`xlsx:"24"`
	Min         float64           //`xlsx:"25"`
	Max         float64           //`xlsx:"26"`

	DK_is_zero bool
	DK_percent float64
	DK_fix     float64
	DK_min     float64
	DK_max     float64

	Formula    string
	DK_formula string
	Range      string
}

func (t *Tariff) StartingFill() {

	if t.Schema == "Crypto" {
		t.IsCrypto = true
	}

	t.RangeMAX = util.TR(t.RangeMAX == 0, RANGE_MAX, t.RangeMAX).(float64) // ставим в конце, чтобы формуле не мешал

	// FORMULA
	s := make([]string, 0, 5)

	if t.Percent != 0 {
		s = append(s, fmt.Sprint("PCT ", util.BaseRound(t.Percent*100)))
	}
	if t.Fix != 0 {
		s = append(s, fmt.Sprint("FIX ", util.BaseRound(t.Fix)))
	}
	if t.Min != 0 {
		s = append(s, fmt.Sprint("MIN ", t.Min))
	}
	if t.Max != 0 {
		s = append(s, fmt.Sprint("MAX ", t.Max))
	}

	range_min := strconv.FormatFloat(t.RangeMIN, 'f', -1, 64)
	range_max := strconv.FormatFloat(t.RangeMAX, 'f', -1, 64)

	if util.BaseRound(t.RangeMIN) != 0 || util.BaseRound(t.RangeMAX) != RANGE_MAX {
		s = append(s, fmt.Sprintf("R.MIN %s R.MAX %s", range_min, range_max))
	}

	t.Formula = strings.Join(s, " ")

	// RANGE
	if util.BaseRound(t.RangeMAX) != RANGE_MAX {
		t.Range = fmt.Sprintf("%s - max", range_min)
	} else {
		t.Range = fmt.Sprintf("%s - %s", range_min, range_max)
	}

	// ДК
	if t.DK_percent+t.DK_fix+t.DK_min+t.DK_max > 0 {
		t.DK_formula = fmt.Sprintf("%s%%, +%s, min %s, max %s", fmt.Sprint(util.BaseRound(t.DK_percent*100)), fmt.Sprint(t.DK_fix), fmt.Sprint(t.DK_min), fmt.Sprint(t.DK_max))
	} else {
		t.DK_is_zero = true
		t.DK_formula = "без компенсации"
	}

}

func SortTariffs() {
	sort.Slice(
		storage.Tariffs,
		func(i int, j int) bool {
			return storage.Tariffs[i].DateStart.After(storage.Tariffs[j].DateStart)
		},
	)
}

func SelectTariffsInRegistry() {

	start_time := time.Now()

	var wg sync.WaitGroup

	channel_indexes := make(chan int, 10000)

	var countWithoutTariff int64

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()
			for index := range channel_indexes {
				operation := storage.Registry[index]
				operation.Crypto_network = crypto.Registry[operation.Operation_id].Network
				operation.Tariff = FindTariffForOperation(operation)
				if operation.Tariff == nil {
					atomic.AddInt64(&countWithoutTariff, 1)
				}
			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Подбор тарифов: %v [без тарифов: %d]", time.Since(start_time), countWithoutTariff))

}

func FindTariffForOperation(op *Operation) *Tariff {

	var operation_date time.Time
	if op.IsPerevodix {
		operation_date = op.Operation_created_at
	} else {
		operation_date = op.Transaction_completed_at
	}

	for _, t := range storage.Tariffs {

		if t.Merchant_account_id == op.Merchant_account_id {

			if t.DateStart.Before(operation_date) &&
				t.Operation_type == op.Operation_type {

				// тип сети будет колонка в тарифе и проверять на неё
				if t.IsCrypto && op.Crypto_network != t.Convertation {
					continue
				}

				// проверяем наличие диапазона
				if t.RangeMIN != 0 || t.RangeMAX != 0 {

					// определелям попадание в диапазон тарифа если он заполнен
					if op.Channel_amount > t.RangeMIN &&
						op.Channel_amount <= t.RangeMAX {
						return &t
					}

				} else {
					return &t
				}

			}
		}
	}

	return nil
}
