package main

import (
	"app/aws"
	"app/config"
	"app/conversion"
	"app/crypto"
	"app/decline"
	"app/logs"
	"app/processing"
	"flag"
	"fmt"
	"time"
)

func main() {

	defer logs.Finish()

	start_time := time.Now()

	var app string
	var file_config string

	flag.StringVar(&app, "app", "conversion", "") // processing | conversion | decline | crypto | aws
	flag.StringVar(&file_config, "file_config", "", "")
	flag.Parse()

	config.New(app, file_config)

	if err := config.Load(); err != nil {
		logs.Add(logs.FATAL, err)
		return
	}

	switch app {
	case "processing":
		processing.Start()
	case "conversion":
		conversion.Start()
	case "decline":
		decline.Start()
	case "crypto":
		crypto.Start()
	case "aws":
		aws.Start()
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Общее время выполнения: %v", time.Since(start_time)))

}
