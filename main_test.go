package main

import (
	"app/config"
	"app/logs"
	"app/processing"
	"testing"
)

// проверка точности рассчета на эталоне
// reg - F; tariff - F; rate - F; crypto - F
func TestProceesing1(t *testing.T) {

	logs.Testing = true

	app := "processing"
	file_config := "test\\config1.conf"

	config.New(app, file_config)

	processing.Start()

	// Проверка результатов
	fact := processing.GetCheckFeeCount()
	expected := 19
	if fact != expected {
		t.Errorf("check fee: %d; expected: %d", fact, expected)
	}

	fact = processing.GetRegistryCount()
	expected = 129463
	if fact != expected {
		t.Errorf("len registry: %d; expected: %d", fact, expected)
	}

	fact = processing.GetWithoutTariffCount()
	expected = 0
	if fact != expected {
		t.Errorf("without tariff: %d; expected: %d", fact, expected)
	}

}

// проверка работы с БД на эталоне
// reg - F; tariff - F; rate - PQ; crypto - PQ
func TestProceesing2(t *testing.T) {

	logs.Testing = true

	app := "processing"
	file_config := "test\\config2.conf"

	config.New(app, file_config)

	processing.Start()

	// Проверка результатов
	fact := processing.GetCheckFeeCount()
	expected := 19
	if fact != expected {
		t.Errorf("check fee: %d; expected: %d", fact, expected)
	}

	fact = processing.GetRegistryCount()
	expected = 129463
	if fact != expected {
		t.Errorf("len registry: %d; expected: %d", fact, expected)
	}

	fact = processing.GetWithoutTariffCount()
	expected = 0
	if fact != expected {
		t.Errorf("without tariff: %d; expected: %d", fact, expected)
	}

}

// проверка строк без тарифов и конверта
// reg - F; tariff - F; rate - F; crypto - PQ
func TestProceesing3(t *testing.T) {

	logs.Testing = true

	app := "processing"
	file_config := "test\\config3.conf"

	config.New(app, file_config)

	processing.Start()

}
