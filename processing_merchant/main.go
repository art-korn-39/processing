package processing_merchant

import (
	"app/config"
	"app/crypto"
	"app/holds"
	"app/logs"
	"app/provider"
	"app/querrys"
	"app/tariff_merchant"
	"fmt"
	"sync"
	"time"
)

const (
	Version = "1.3.5"
)

var (
	storage Storage
)

func Init() {

	var err error

	err = config.Load()
	if err != nil {
		logs.Add(logs.FATAL, err)
	}
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

	wg.Add(4)

	registry_done := make(chan querrys.Args, 1)
	go func() {
		defer wg.Done()
		Read_Registry(registry_done)
	}()

	go func() {
		defer wg.Done()
		provider.Read_Registry(storage.Postgres, registry_done)
	}()

	go func() {
		defer wg.Done()
		tariff_merchant.Read_Sources()
	}()

	go func() {
		defer wg.Done()
		crypto.Read_Registry(storage.Postgres)
	}()

	wg.Wait()
}

func PrepareData() {

	var wg sync.WaitGroup

	wg.Add(2)

	// 2. Тарифы
	go func() {
		defer wg.Done()

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
		provider.Rates = provider.Rates.Group()

		// Сортировка курсов валют
		provider.Rates.Sort()
	}()

	wg.Wait()

}

func HandleDataInOperations() {

	CalculateCommission()
	HandleHolds()

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
