package processing_merchant

import (
	"app/countries"
	"app/crypto"
	"app/dragonpay"
	"app/logs"
	"app/provider_balances"
	"app/provider_registry"
	"app/providers"
	"app/providers_1c"
	"app/querrys"
	"app/rr_merchant"
	"app/tariff_compensation"
	"app/tariff_merchant"
	"app/teams_tradex"
	"app/test_merchant_accounts"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	Version = "1.7.0"
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
		logs.Add(logs.FATAL, strings.ToUpper("Некорректный файл из БОФ (файл был перезаписан, id операций не актуальны)"))
	}

	// 3. Подготовка данных
	PrepareData()

	logs.Add(logs.DEBUG, "PrepareData: ", time.Since(st))
	st = time.Now()

	// 4. Расчёты
	CalculateCommission()

	logs.Add(logs.DEBUG, "CalculateCommission: ", time.Since(st))
	st = time.Now()

	// 5. Результат
	SaveResult()

	logs.Add(logs.DEBUG, "SaveResult: ", time.Since(st))

}

func ReadSources() {

	var wg sync.WaitGroup

	wg.Add(14)

	channel_readers := 5
	registry_done := make(chan querrys.Args, channel_readers)

	go func() {
		defer wg.Done()
		Read_Registry(registry_done, channel_readers)
	}()

	go func() {
		defer wg.Done()
		provider_registry.Read_Registry(storage.Postgres, registry_done, true)
	}()

	go func() {
		defer wg.Done()
		tariff_merchant.Read_Sources(storage.Postgres, registry_done)
	}()

	go func() {
		defer wg.Done()
		tariff_compensation.Read_Sources(storage.Postgres, true)
	}()

	go func() {
		defer wg.Done()
		PSQL_read_detailed_provider(storage.Postgres, registry_done)
	}()

	go func() {
		defer wg.Done()
		crypto.Read_Registry(storage.Postgres, registry_done)
	}()

	go func() {
		defer wg.Done()
		dragonpay.Read_Registry(storage.Postgres, false, registry_done)
	}()

	go func() {
		defer wg.Done()
		countries.Read_Data(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		test_merchant_accounts.Read(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		provider_balances.Read(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		providers_1c.Read(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		providers.Read(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		teams_tradex.Read(storage.Postgres)
	}()

	go func() {
		defer wg.Done()
		rr_merchant.Read(storage.Postgres)
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
