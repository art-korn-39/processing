package main

import (
	"app/aws"
	"app/config"
	"app/conversion"
	"app/convert"
	"app/crm_chargeback"
	"app/crm_dictionary"
	"app/crypto"
	"app/decline"
	"app/dragonpay"
	"app/logs"
	"app/origamix"
	"app/processing_merchant"
	"app/processing_provider"
	"app/sverka"
	"flag"
	"fmt"
	"time"
)

func main() {

	defer logs.Finish()

	start_time := time.Now()

	var app string
	var file_config string

	// processing_merchant | processing_provider | convert | sverka
	// conversion | decline | crypto | dragonpay | aws | crm_chargeback | crm_dictionary | origamix
	flag.StringVar(&app, "app", "processing_provider", "")
	flag.StringVar(&file_config, "file_config", "", "")
	flag.Parse()

	config.New(app, file_config)

	if err := config.Load(); err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	switch app {
	case "processing_merchant":
		processing_merchant.Start()
	case "processing_provider":
		processing_provider.Start()
	case "conversion":
		conversion.Start()
	case "decline":
		decline.Start()
	case "crypto":
		crypto.Start()
	case "dragonpay":
		dragonpay.Start()
	case "aws":
		aws.Start()
	case "crm_chargeback":
		crm_chargeback.Start()
	case "crm_dictionary":
		crm_dictionary.Start()
	case "convert":
		convert.Start()
	case "sverka":
		sverka.Start()
	case "origamix":
		origamix.Start()
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Общее время выполнения: %v", time.Since(start_time)))

}
