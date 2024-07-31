package processing

// import (
// 	"github.com/tealeg/xlsx"
// )

// func add_page_copy1(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

// 	sheet, _ := f.AddSheet("Копируем")

// 	headers := []string{"Баланс", "IDBALANCE", "Дата", "Проверка", "operation_type", "issuer_country",
// 		"payment_method_type", "merchant_name", "project_name", "merchant_account_name", "Подразделение", "Рассчетный счет",
// 		"Поставщик 1С", "real_currency / channel_currency", "Валюта баланса", "Акт. тариф", "Акт. Фикс",
// 		"Акт. Мин", "Акт. Макс", "Range min", "Range max", "Старт тарифа", "tariff_condition_id", "contract_id",
// 		"PPрасхолд", "CryptoNetWork", "real_amount / channel_amount", "real_amount, fee", "Сумма в валюте баланса",
// 		"SR Balance Currency", "ChecFee", "Кол-во операций", "Сумма холда"}

// 	style := xlsx.NewStyle()
// 	style.Fill.FgColor = "5B9BD5"
// 	style.Fill.PatternType = "solid"
// 	style.ApplyFill = true
// 	style.Alignment.WrapText = true
// 	style.Alignment.Horizontal = "center"
// 	style.Alignment.Vertical = "center"
// 	style.ApplyAlignment = true
// 	style.Font.Bold = true
// 	style.Font.Color = "FFFFFF"

// 	row := sheet.AddRow()

// 	for _, v := range headers {
// 		cell := row.AddCell()
// 		cell.SetString(v)
// 		cell.SetStyle(style)
// 	}

// 	sheet.SetColWidth(0, 0, 30)   // баланс
// 	sheet.SetColWidth(1, 1, 11)   // idbalance
// 	sheet.SetColWidth(2, 2, 12)   // дата
// 	sheet.SetColWidth(3, 3, 16)   // проверка
// 	sheet.SetColWidth(4, 4, 15)   // operation_type
// 	sheet.SetColWidth(6, 6, 18)   // payment_method_type
// 	sheet.SetColWidth(9, 9, 35)   // merchant_account_name
// 	sheet.SetColWidth(10, 10, 16) // подразделение
// 	sheet.SetColWidth(11, 11, 25) // рассчетный счет
// 	sheet.SetColWidth(12, 12, 16) // поставщик 1С
// 	sheet.SetColWidth(13, 13, 14) // real_currency / channel_currency
// 	sheet.SetColWidth(21, 21, 12) // старт тарифа
// 	sheet.SetColWidth(22, 22, 16) // tariff_condition_id
// 	sheet.SetColWidth(23, 23, 12) // contract_id
// 	sheet.SetColWidth(24, 24, 12) // PPрасхолд
// 	sheet.SetColWidth(26, 32, 16)

// 	var cell *xlsx.Cell

// 	for k, v := range M {
// 		row := sheet.AddRow()
// 		row.AddCell().SetString(k.balance_name)
// 		row.AddCell().SetInt(k.balance_id)
// 		row.AddCell().SetDate(k.document_date)
// 		row.AddCell().SetString(k.verification)
// 		row.AddCell().SetString(k.operation_type)
// 		row.AddCell().SetString(k.country)
// 		row.AddCell().SetString(k.payment_type)
// 		row.AddCell().SetString(k.merchant_name)
// 		row.AddCell().SetString(k.project_name)
// 		row.AddCell().SetString(k.merchant_account_name)
// 		row.AddCell().SetString(k.subdivision1C)
// 		row.AddCell().SetString(k.ratedAccount)
// 		row.AddCell().SetString(k.provider1C)
// 		row.AddCell().SetString(k.channel_currency.Name)
// 		row.AddCell().SetString(k.currencyBP.Name)

// 		cell = row.AddCell()
// 		cell.SetFloat(k.percent)
// 		cell.SetFormat("0.00%")

// 		cell = row.AddCell()
// 		cell.SetFloat(k.fix)
// 		cell.SetFormat("0.00")

// 		row.AddCell().SetFloat(k.min)
// 		row.AddCell().SetFloat(k.max)
// 		row.AddCell().SetFloat(k.range_min)
// 		row.AddCell().SetFloat(k.range_max)

// 		if k.date_start.IsZero() {
// 			row.AddCell().SetString("")
// 		} else {
// 			row.AddCell().SetDate(k.date_start)
// 		}

// 		row.AddCell().SetInt(k.tariff_condition_id)
// 		row.AddCell().SetInt(k.contract_id)

// 		if k.RR_date.IsZero() {
// 			row.AddCell().SetString("")
// 		} else {
// 			row.AddCell().SetDate(k.RR_date)
// 		}

// 		row.AddCell().SetString(k.Crypto_network)

// 		cell = row.AddCell()
// 		cell.SetFloat(v.channel_amount)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell()
// 		cell.SetFloat(v.SR_channel_currency)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell()
// 		cell.SetFloat(v.balance_amount)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell()
// 		cell.SetFloat(v.SR_balance_currency)
// 		cell.SetFormat("0.00")

// 		row.AddCell().SetFloat(v.checkFee)
// 		row.AddCell().SetInt(v.count_operations)

// 		cell = row.AddCell()
// 		cell.SetFloat(v.RR_amount)
// 		cell.SetFormat("0.00")
// 	}

// }

// func add_page_copy2(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

// 	sheet, _ := f.AddSheet("Копируем2")

// 	headers := []string{"Баланс", "IDBALANCE", "Дата", "Проверка", "operation_type",
// 		"payment_method_type", "merchant_name", "merchant_account_name", "Подразделение", "Рассчетный счет",
// 		"Поставщик 1С", "real_currency / channel_currency", "Валюта баланса", "Акт. тариф формула", "Range",
// 		"Старт тарифа", "tariff_condition_id", "contract_id", "PPрасхолд", "ДатаРасхолдМ", "CryptoNetWork",
// 		"ДК тариф формула", "Компенсация BC", "Компенсация RC",
// 		"real_amount / channel_amount", "real_amount, fee", "Сумма в валюте баланса",
// 		"SR Balance Currency", "ChecFee", "Кол-во операций", "Сумма холда", "СуммаХолдаМ"}

// 	style := xlsx.NewStyle()
// 	style.Fill.FgColor = "5B9BD5"
// 	style.Fill.PatternType = "solid"
// 	style.ApplyFill = true
// 	style.Alignment.WrapText = true
// 	style.Alignment.Horizontal = "center"
// 	style.Alignment.Vertical = "center"
// 	style.ApplyAlignment = true
// 	style.Font.Bold = true
// 	style.Font.Color = "FFFFFF"

// 	row := sheet.AddRow()

// 	for _, v := range headers {
// 		cell := row.AddCell()
// 		cell.SetString(v)
// 		cell.SetStyle(style)
// 	}

// 	sheet.SetColWidth(0, 0, 30)   // баланс
// 	sheet.SetColWidth(1, 1, 11)   // idbalance
// 	sheet.SetColWidth(2, 2, 12)   // дата
// 	sheet.SetColWidth(3, 3, 16)   // проверка
// 	sheet.SetColWidth(4, 4, 15)   // operation_type
// 	sheet.SetColWidth(5, 5, 18)   // payment_method_type
// 	sheet.SetColWidth(7, 7, 35)   // merchant_account_name
// 	sheet.SetColWidth(8, 8, 16)   // подразделение
// 	sheet.SetColWidth(9, 9, 25)   // рассчетный счет
// 	sheet.SetColWidth(10, 10, 16) // поставщик 1С
// 	sheet.SetColWidth(11, 11, 14) // real_currency / channel_currency

// 	sheet.SetColWidth(15, 15, 12) // старт тарифа
// 	sheet.SetColWidth(16, 16, 16) // tariff_condition_id
// 	sheet.SetColWidth(17, 17, 12) // contract_id
// 	sheet.SetColWidth(18, 19, 12) // PPрасхолд
// 	sheet.SetColWidth(20, 31, 16)

// 	var cell *xlsx.Cell

// 	for k, v := range M {
// 		row := sheet.AddRow()
// 		row.AddCell().SetString(k.balance_name)          //0
// 		row.AddCell().SetInt(k.balance_id)               //1
// 		row.AddCell().SetDate(k.document_date)           //2
// 		row.AddCell().SetString(k.verification)          //3
// 		row.AddCell().SetString(k.operation_type)        //4
// 		row.AddCell().SetString(k.payment_type)          //5
// 		row.AddCell().SetString(k.merchant_name)         //6
// 		row.AddCell().SetString(k.merchant_account_name) //7
// 		row.AddCell().SetString(k.subdivision1C)         //8
// 		row.AddCell().SetString(k.ratedAccount)          //9
// 		row.AddCell().SetString(k.provider1C)            //10
// 		row.AddCell().SetString(k.channel_currency.Name) //11
// 		row.AddCell().SetString(k.currencyBP.Name)       //12 Валюта баланса
// 		row.AddCell().SetString(k.Formula)               //13 Формула
// 		row.AddCell().SetString(k.Range)                 //14 Range

// 		if k.date_start.IsZero() { //15
// 			row.AddCell().SetString("")
// 		} else {
// 			row.AddCell().SetDate(k.date_start)
// 		}

// 		row.AddCell().SetInt(k.tariff_condition_id) //16
// 		row.AddCell().SetInt(k.contract_id)         //17

// 		if k.RR_date.IsZero() { //18 PPрасхолд
// 			row.AddCell().SetString("")
// 		} else {
// 			row.AddCell().SetDate(k.RR_date)
// 		}

// 		if k.hold_date.IsZero() { //19 ДатаРасхолдМ
// 			row.AddCell().SetString("")
// 		} else {
// 			row.AddCell().SetDate(k.hold_date)
// 		}

// 		row.AddCell().SetString(k.Crypto_network) //20 CryptoNetWork
// 		row.AddCell().SetString(k.FormulaDK)      //21 ДК тариф формула

// 		cell = row.AddCell() //22 Компенсация BC
// 		cell.SetFloat(v.CompensationBC)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell() //23 Компенсация RC
// 		cell.SetFloat(v.CompensationRC)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell() //24
// 		cell.SetFloat(v.channel_amount)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell() //25
// 		cell.SetFloat(v.SR_channel_currency)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell() //26 Сумма в валюте баланса
// 		cell.SetFloat(v.balance_amount)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell() //27
// 		cell.SetFloat(v.SR_balance_currency)
// 		cell.SetFormat("0.00")

// 		row.AddCell().SetFloat(v.checkFee)       //28
// 		row.AddCell().SetInt(v.count_operations) //29

// 		cell = row.AddCell() //30
// 		cell.SetFloat(v.RR_amount)
// 		cell.SetFormat("0.00")

// 		cell = row.AddCell() //31 СуммаХолдаМ
// 		cell.SetFloat(v.hold_amount)
// 		cell.SetFormat("0.00")
// 	}

// }
