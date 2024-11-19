package main

import (
	"app/aws"
	"app/chargeback"
	"app/config"
	"app/conversion"
	"app/crm_dictionary"
	"app/crypto"
	"app/decline"
	"app/dragonpay"
	"app/logs"
	"app/processing_merchant"
	"app/processing_provider"
	"flag"
	"fmt"
	"time"
)

func main() {

	defer logs.Finish()

	start_time := time.Now()

	var app string
	var file_config string

	// processing_merchant | processing_provider | conversion | decline
	// crypto | dragonpay | aws | chargeback | crm_dictionary
	flag.StringVar(&app, "app", "chargeback", "")
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
	case "chargeback":
		chargeback.Start()
	case "crm_dictionary":
		crm_dictionary.Start()
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Общее время выполнения: %v", time.Since(start_time)))

}
