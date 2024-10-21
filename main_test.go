package main

import (
	"app/config"
	"app/logs"
	"app/processing_merchant"
	"app/processing_provider"
	"testing"
)

// проверка точности рассчета на эталоне
// reg - F; tariff - F; rate - F; crypto - F
func TestProceesing1(t *testing.T) {

	logs.Testing = true

	app := "processing_merchant"
	file_config := "test\\config1.conf"

	config.New(app, file_config)
	if err := config.Load(); err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	processing_merchant.Start()

	// Проверка результатов
	fact := processing_merchant.GetCheckFeeCount()
	expected := 19
	if fact != expected {
		t.Errorf("check fee: %d; expected: %d", fact, expected)
	}

	fact = processing_merchant.GetRegistryCount()
	expected = 129463
	if fact != expected {
		t.Errorf("len registry: %d; expected: %d", fact, expected)
	}

	fact = processing_merchant.GetWithoutTariffCount()
	expected = 0
	if fact != expected {
		t.Errorf("without tariff: %d; expected: %d", fact, expected)
	}

}

// проверка работы с БД на эталоне
// reg - F; tariff - F; rate - PQ; crypto - PQ
func TestProceesing2(t *testing.T) {

	logs.Testing = true

	app := "processing_merchant"
	file_config := "test\\config2.conf"

	config.New(app, file_config)
	if err := config.Load(); err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	processing_merchant.Start()

	// Проверка результатов
	fact := processing_merchant.GetCheckFeeCount()
	expected := 19
	if fact != expected {
		t.Errorf("check fee: %d; expected: %d", fact, expected)
	}

	fact = processing_merchant.GetRegistryCount()
	expected = 129463
	if fact != expected {
		t.Errorf("len registry: %d; expected: %d", fact, expected)
	}

	fact = processing_merchant.GetWithoutTariffCount()
	expected = 0
	if fact != expected {
		t.Errorf("without tariff: %d; expected: %d", fact, expected)
	}

}

// проверка строк без тарифов и конверта
// reg - CH; tariff - F; rate - F; crypto - PQ
func TestProceesing3(t *testing.T) {

	logs.Testing = true

	app := "processing_merchant"
	file_config := "test\\config3.conf"

	config.New(app, file_config)
	if err := config.Load(); err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	processing_merchant.Start()

}

// проверка выполнения
// reg - F; tariff - F
func TestProceesing4(t *testing.T) {

	logs.Testing = true

	app := "processing_provider"
	file_config := "test\\config4.conf"

	config.New(app, file_config)
	if err := config.Load(); err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	processing_provider.Start()

}
