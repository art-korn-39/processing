package main

import (
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

	// a := 1000000000
	// b := float64(a)

	// fmt.Println(strconv.FormatFloat(b, 'f', -1, 64))
	// fmt.Println(fmt.Sprintf("%.f", b))

	// return

	defer logs.Finish()

	start_time := time.Now()

	var app string
	var file_config string

	flag.StringVar(&app, "app", "processing", "") // processing | conversion | decline | crypto
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
	}

	logs.Add(logs.MAIN, fmt.Sprintf("Общее время выполнения: %v", time.Since(start_time)))

}
