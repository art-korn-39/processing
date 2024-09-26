package processing_provider

import (
	"app/config"
	"app/logs"
	"app/querrys"
	"app/tariff_merchant"
	"fmt"
	"sync"
	"time"
)

const (
	Version = "1.0.0"
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

	wg.Add(2)

	registry_done := make(chan querrys.Args, 1)
	go func() {
		defer wg.Done()
		Read_Registry(registry_done)
	}()

	go func() {
		defer wg.Done()
		tariff_merchant.Read_Sources()
	}()

	wg.Wait()
}

func PrepareData() {

	// var wg sync.WaitGroup

	// wg.Add(2)

	// // 2. Тарифы
	// go func() {
	// 	defer wg.Done()

	// Сортировка
	tariff_merchant.SortTariffs()

	// Подбор тарифов к операциям
	SelectTariffsInRegistry()
	//}()

	// // 2. Курсы валют
	// go func() {
	// 	defer wg.Done()

	// 	// Группировка курсов валют
	// 	provider.Rates = provider.Rates.Group()

	// 	// Сортировка курсов валют
	// 	provider.Rates.Sort()
	// }()

	// wg.Wait()

}

func HandleDataInOperations() {

	CalculateCommission()

}

func SaveResult() {

	// Итоговые данные
	// Делаем сначала, т.к. они id документа проставляют
	// summary := GroupRegistryToSummaryProvider()
	// Write_Summary(summary)

	// Выгрузка в эксель
	summaryInfo := GroupRegistryToSummaryInfo()
	Write_SummaryInfo(summaryInfo)

}
