package processing_provider

import (
	"app/countries"
	"app/logs"
	"app/merchants"
	"app/provider_balances"
	"app/querrys"
	"app/tariff_provider"
	"fmt"
	"sync"
	"time"
)

const (
	Version = "1.1.0"
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

	wg.Add(5)

	registry_done := make(chan querrys.Args, 1)
	go func() {
		defer wg.Done()
		Read_Registry(registry_done)
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

	wg.Wait()
}

func PrepareData() {

	// var wg sync.WaitGroup

	// wg.Add(2)

	// Сортировка
	tariff_provider.SortTariffs()

	// Подбор тарифов к операциям
	SelectTariffsInRegistry()

	SetBalanceInOperations()

	SetMerchantInOperations()

	// wg.Wait()

}

func HandleDataInOperations() {

	CalculateCommission()

}

func SaveResult() {

	var wg sync.WaitGroup

	wg.Add(1)

	// Итоговые данные
	// Делаем сначала, т.к. они id документа проставляют
	// summary := GroupRegistryToSummaryProvider()

	summaryInfo := GroupRegistryToSummaryInfo()
	Write_SummaryInfo(summaryInfo)

	// Детализированные записи
	go func() {
		defer wg.Done()
		Write_Detailed()
	}()

	wg.Wait()

}
