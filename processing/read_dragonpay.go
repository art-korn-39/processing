package processing

import (
	"app/util"
	"fmt"
	"log"
	"time"

	"github.com/tealeg/xlsx"
)

func Read_XLSX_DragonPay() {

	start_time := time.Now()

	storage.Tariffs = make([]Tariff, 0, 1000)

	filename := "data/Dragonpay.xlsx"
	xlFile, err := xlsx.OpenFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	for _, sheet := range xlFile.Sheets {

		util.Unused(sheet)

	}

	fmt.Println("Чтение dragonpay: ", time.Since(start_time))

}
