package main

import (
	"app/config"
	"app/logs"
	"app/processing"
	"testing"
)

// reg - F; tariff - F; rate - F; crypto - F
func TestProceesing1(t *testing.T) {

	logs.Testing = true

	app := "processing"
	async := true
	file_config := "test\\config1.conf"

	config.New(app, async, file_config)

	processing.Start()

	// Проверка результатов
	fact := processing.GetCheckFeeCount()
	expected := 21
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

// reg - F; tariff - F; rate - PQ; crypto - F
func TestProceesing2(t *testing.T) {

	logs.Testing = true

	app := "processing"
	async := true
	file_config := "test\\config2.conf"

	config.New(app, async, file_config)

	processing.Start()

	// Проверка результатов
	fact := processing.GetCheckFeeCount()
	expected := 21
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
