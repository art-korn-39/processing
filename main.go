package main

import (
	"app/aws"
	"app/config"
	"app/conversion"
	"app/convert"
	"app/crm_chargeback"
	"app/crm_dictionary"
	"app/crm_merchant_losses"
	"app/crm_provider_losses"
	"app/crypto"
	"app/decline"
	"app/dragonpay"
	"app/logs"
	"app/origamix"
	"app/processing_merchant"
	"app/processing_provider"
	"app/sverka"
	"app/util"
	"flag"
	"fmt"
	"time"
)

func main() {

	defer logs.Finish()

	start_time := time.Now()

	var app string
	var file_config string

	// processing_merchant  | processing_provider | convert 			| sverka
	// conversion 			| decline 			  | crypto  			| dragonpay 		  | aws | origamix
	// crm_chargeback 		| crm_dictionary 	  | crm_provider_losses | crm_merchant_losses |

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
	case "convert":
		convert.Start()
	case "sverka":
		sverka.Start()
	case "origamix":
		origamix.Start()
	case "crm_chargeback":
		crm_chargeback.Start()
	case "crm_dictionary":
		crm_dictionary.Start()
	case "crm_provider_losses":
		crm_provider_losses.Start()
	case "crm_merchant_losses":
		crm_merchant_losses.Start()
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Общее время выполнения: %v", util.FormatDuration(time.Since(start_time))))

}

// time.Time	 = 24B
// int/int64	 = 8B
// float64		 = 8B
// *pointer 	 = 8B
// slice		 = 24B  (*pointer, len, cap)
// boolean 		 = 1B 	(обычно 8B, из-за выравнивания)
// string 		 = 16B  (*pointer, len)
// "1" (ASCII)	 = 1B   (в куче)
// "л" (UTF-8) 	 = 2B   (в куче)
// "목" (UTF-8+) = 3-4B (в куче)

// GUID 1C = '64fa2fb9-785e-11f0-a37e-005056814ce3' = 36B (в куче и 16B заголовок)

// Передача по значению VS по указателю:

//Small  (32B): Value на 10-30% быстрее
//Medium (128B): Примерно одинаково (±5%)
//Large  (512B): Pointer на 20-50% быстрее
