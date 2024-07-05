package main

import (
	"app/config"
	"app/conversion"
	"app/decline"
	"app/logs"
	"app/processing"
	"flag"
)

func main() {

	var app string
	var async bool
	var file_config string

	flag.StringVar(&app, "app", "decline", "") // processing | conversion | decline
	flag.BoolVar(&async, "async", false, "")
	flag.StringVar(&file_config, "file_config", "", "")
	flag.Parse()

	config.New(app, async, file_config)

	if err := config.Load(); err != nil {
		logs.Add(logs.FATAL, err)
	}

	switch app {
	case "processing":
		processing.Start()
	case "conversion":
		conversion.Start()
	case "decline":
		decline.Start()
	}

}
