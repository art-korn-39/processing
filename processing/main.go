package processing

import (
	"app/config"
	"app/crypto"
	"app/holds"
	"app/logs"
	"app/provider"
	"app/querrys"
	"fmt"
	"sync"
	"time"
)

const (
	Version = "1.2.1"
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

	err = storage.Init()
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

	//return

	logs.Add(logs.DEBUG, "ReadSources: ", time.Since(st))
	st = time.Now()

	// 2. Подготовка данных
	PrepareData()

	logs.Add(logs.DEBUG, "PrepareData: ", time.Since(st))
	st = time.Now()

	// 3.
	HandleDataInOperations()

	logs.Add(logs.DEBUG, "CalculateCommission: ", time.Since(st))
	st = time.Now()

	// 5. Результат
	SaveResult()

	logs.Add(logs.DEBUG, "SaveResult: ", time.Since(st))

}

func ReadSources() {

	var wg sync.WaitGroup

	wg.Add(4)

	//wg.Add(1)

	registry_done := make(chan querrys.Args, 1)
	go func() {
		defer wg.Done()
		Read_Registry(registry_done)
	}()

	go func() {
		defer wg.Done()
		//Read_ProviderRegistry(registry_done)
		provider.Read_Registry(storage.Postgres, registry_done)
	}()

	go func() {
		defer wg.Done()
		Read_Tariffs()
	}()

	go func() {
		defer wg.Done()
		//Read_Crypto()
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
		SortTariffs()
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

	// var wg sync.WaitGroup

	// wg.Add(2)

	// // 3. Комиссия
	// go func() {
	// 	defer wg.Done()
	// 	CalculateCommission()
	// }()

	// // 4. Холды
	// go func() {
	// 	defer wg.Done()
	// 	HandleHolds()
	// }()

	// wg.Wait()

	CalculateCommission()
	HandleHolds()

}

func SaveResult() {

	var wg sync.WaitGroup

	wg.Add(3)

	// Детализированные записи
	go func() {
		defer wg.Done()
		Write_Detailed()
	}()

	// Итоговые данные
	go func() {
		defer wg.Done()
		summary := GroupRegistryToSummaryMerchant()
		Write_Summary(summary)
	}()

	// Выгрузка в эксель
	go func() {
		defer wg.Done()
		summaryInfo := GroupRegistryToSummaryInfo()
		Write_SummaryInfo(summaryInfo)
	}()

	wg.Wait()

}
