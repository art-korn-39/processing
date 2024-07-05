package processing

import (
	"app/config"
	"app/logs"
	"fmt"
	"sync"
	"time"
)

const (
	Version       = "0.6.9"
	NUM_GORUTINES = 5
)

var (
	storage Storage
	mu      sync.Mutex
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
	defer logs.Finish()

	Init()

	defer storage.Close()

	start_time := time.Now()

	// 1. Загрузка источников
	ReadSources()

	// 2. Подготовка данных
	PrepareData()

	// 3. Комиссия
	CalculateCommission()

	// 4. Результат
	SaveResult()

	logs.Add(logs.INFO, fmt.Sprintf("Общее время выполнения: %v", time.Since(start_time)))

}

func ReadSources() {

	var wg sync.WaitGroup

	wg.Add(4)

	registry_done := make(chan struct{})
	go func() {
		defer wg.Done()
		Read_Registry(registry_done)
	}()

	go func() {
		defer wg.Done()
		Read_ProviderRegistry(registry_done)
	}()

	go func() {
		defer wg.Done()
		Read_Tariffs()
	}()

	go func() {
		defer wg.Done()
		Read_Crypto()
	}()

	wg.Wait()
}

func PrepareData() {

	var wg sync.WaitGroup

	wg.Add(2)

	// 2. Тарифы
	go func() {
		defer wg.Done()
		// Сортировка тарифов
		SortTariffs()
		// Подбор тарифов к операциям
		SelectTariffsInRegistry()
	}()

	// 2. Курсы валют
	go func() {
		defer wg.Done()
		// Группировка курсов валют
		GroupRates()
		// Сортировка курсов валют
		SortRates()
	}()

	wg.Wait()

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
