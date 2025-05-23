package processing_provider

import (
	"app/countries"
	"app/dragonpay"
	"app/logs"
	"app/merchants"
	"app/provider_balances"
	"app/provider_registry"
	"app/querrys"
	"app/tariff_provider"
	"fmt"
	"sync"
	"time"
)

const (
	Version = "1.3.0"
)

var (
	storage Storage
)

func Init() {

	logs.Add(logs.INFO, fmt.Sprintf("Загружен файл конфигурации (ver %s)", Version))

	err := storage.Connect()
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

	// 2. Подготовка данных
	PrepareData()

	logs.Add(logs.DEBUG, "PrepareData: ", time.Since(st))
	st = time.Now()

	// 3. Комиссия и холды
	HandleDataInOperations()

	logs.Add(logs.DEBUG, "CalculateCommission: ", time.Since(st))
	st = time.Now()

	// 4. Результат
	SaveResult()

	logs.Add(logs.DEBUG, "SaveResult: ", time.Since(st))

}

func ReadSources() {

	var wg sync.WaitGroup

	wg.Add(8)

	registry_done := make(chan querrys.Args, 1)
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
		tariff_provider.Read_Sources(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		countries.Read_Data(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		provider_balances.Read(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		merchants.Read(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		dragonpay.Read_Registry(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		Read_Detailed(storage.Postgres, registry_done)
	}()

	wg.Wait()
}

func PrepareData() {

	// Сортировка
	tariff_provider.SortTariffs()

	// Заполнение стран
	SetCountriesInOperations()

	// Подбор операций из реестра провайдера
	SetProviderOperations()

	// Подбор балансов к операциям
	SetBalanceInOperations()

	SetBalanceCurrencyInOperations()

	// Подбор тарифов к операциям
	SelectTariffsInRegistry()

	SetMerchantInOperations()

}

func HandleDataInOperations() {

	CalculateCommission()

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
