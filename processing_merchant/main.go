package processing_merchant

import (
	"app/config"
	"app/countries"
	"app/crypto"
	"app/dragonpay"
	"app/holds"
	"app/logs"
	"app/provider_registry"
	"app/querrys"
	"app/tariff_merchant"
	"app/test_merchant_accounts"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	Version = "1.5.5"
)

var (
	storage Storage
)

func Init() {

	var err error
	logs.Add(logs.INFO, fmt.Sprintf("Загружен файл конфигурации (ver %s)", Version))

	err = storage.Connect()
	if err != nil {
		logs.Add(logs.FATAL, err)
	}

}

func Start() {

	Init()
	defer storage.Close()

	st := time.Now()

	// 1. Загрузка источников
	ReadSources()

	logs.Add(logs.DEBUG, "ReadSources: ", time.Since(st))
	st = time.Now()

	// 2. Проверка файла реестра БОФ
	if BofRegistryNotValid() {
		logs.Add(logs.FATAL, strings.ToUpper("Некорректный файл из БОФ"))
	}

	// 3. Подготовка данных
	PrepareData()

	logs.Add(logs.DEBUG, "PrepareData: ", time.Since(st))
	st = time.Now()

	// 4. Комиссия и холды
	CalculateCommission()
	HandleHolds()

	logs.Add(logs.DEBUG, "CalculateCommission: ", time.Since(st))
	st = time.Now()

	// 5. Результат
	SaveResult()

	logs.Add(logs.DEBUG, "SaveResult: ", time.Since(st))

}

func ReadSources() {

	var wg sync.WaitGroup

	wg.Add(8)

	registry_done := make(chan querrys.Args, 3)
	go func() {
		defer wg.Done()
		Read_Registry(registry_done)
	}()

	go func() {
		defer wg.Done()
		provider_registry.Read_Registry(storage.Postgres, registry_done)
	}()

	go func() {
		defer wg.Done()
		tariff_merchant.Read_Sources(storage.Postgres, registry_done)
	}()

	go func() {
		defer wg.Done()
		PSQL_read_detailed_provider(storage.Postgres, registry_done)
	}()

	go func() {
		defer wg.Done()
		crypto.Read_Registry(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		dragonpay.Read_Registry(storage.Postgres, false)
	}()

	go func() {
		defer wg.Done()
		countries.Read_Data(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		test_merchant_accounts.Read(storage.Postgres)
	}()

	wg.Wait()
}

func PrepareData() {

	var wg sync.WaitGroup

	wg.Add(2)

	// 2. Тарифы
	go func() {
		defer wg.Done()

		IndicateIsTestInRegistry()

		// Сортировка
		tariff_merchant.SortTariffs()
		holds.Sort()

		// Подбор тарифов к операциям
		SelectTariffsInRegistry()
	}()

	// 2. Курсы валют
	go func() {
		defer wg.Done()

		// Группировка курсов валют
		provider_registry.GroupRates()

		// Сортировка курсов валют
		provider_registry.SortRates()
	}()

	wg.Wait()

}

func SaveResult() {

	var wg sync.WaitGroup

	wg.Add(2)

	// Итоговые данные
	// Делаем сначала, т.к. они id документа проставляют
	summary := GroupRegistryToSummaryMerchant()
	Write_Summary(summary)

	// Детализированные записи
	go func() {
		defer wg.Done()
		Write_Detailed()
	}()

	// Выгрузка в эксель
	go func() {
		defer wg.Done()
		summaryInfo := GroupRegistryToSummaryInfo()
		Write_SummaryInfo(summaryInfo)
	}()

	wg.Wait()

}

func IndicateIsTestInRegistry() {

	start_time := time.Now()

	channel_indexes := make(chan int, 1000)

	var wg sync.WaitGroup

	wg.Add(config.NumCPU)
	for i := 1; i <= config.NumCPU; i++ {
		go func() {
			defer wg.Done()

			for index := range channel_indexes {

				operation := storage.Registry[index]

				operation.mu.Lock()
				if test_merchant_accounts.Skip(operation.Document_date, operation.Merchant_account_id, operation.Operation_type) {
					operation.IsTestId = 1
					operation.IsTestType = "live test"
				}
				operation.mu.Unlock()

			}
		}()
	}

	for i := range storage.Registry {
		channel_indexes <- i
	}
	close(channel_indexes)

	wg.Wait()

	logs.Add(logs.INFO, fmt.Sprintf("Определение тестового трафика: %v", time.Since(start_time)))

}
