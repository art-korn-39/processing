package processing

import (
	"app/config"
	"app/logs"
	"app/util"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/tealeg/xlsx"
)

func Write_SummaryInfo(M map[KeyFields_SummaryInfo]SumFileds) {

	if !config.Get().SummaryInfo.Usage {
		return
	}

	if config.Get().SummaryInfo.Storage == config.PSQL {
		util.Unused()
	} else {
		Write_XLSX_SummaryInfo(M)
	}

}

func Write_XLSX_SummaryInfo(M map[KeyFields_SummaryInfo]SumFileds) {

	if config.Get().SummaryInfo.Filename == "" {
		return
	}

	start_time := time.Now()

	xlsx.SetDefaultFont(11, "Calibri")

	f := xlsx.NewFile()

	//add_page_copy1(f, M)
	add_page_copy2(f, M) // теперь это лист "копируем"
	add_page_svodno(f, M)
	add_page_1_makeTariff(f, M)
	add_page_2_checkBilling(f)
	add_page_3_checkRate(f, M)
	add_page_4_noProviderReg(f)
	add_page_5_no_Perevodix_KGX_verification(f)

	err := f.Save(config.Get().SummaryInfo.Filename)
	if err != nil {
		logs.Add(logs.INFO, "Не удалось сохранить данные в Excel файл: ошибка совместного доступа к файлу")
		return
	}

	logs.Add(logs.INFO, fmt.Sprintf("Сохранение данных в Excel файл: %v", time.Since(start_time)))

}

func add_page_copy1(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("Копируем")

	headers := []string{"Баланс", "IDBALANCE", "Дата", "Проверка", "operation_type", "issuer_country",
		"payment_method_type", "merchant_name", "project_name", "merchant_account_name", "Подразделение", "Рассчетный счет",
		"Поставщик 1С", "real_currency / channel_currency", "Валюта баланса", "Акт. тариф", "Акт. Фикс",
		"Акт. Мин", "Акт. Макс", "Range min", "Range max", "Старт тарифа", "tariff_condition_id", "contract_id",
		"PPрасхолд", "CryptoNetWork", "real_amount / channel_amount", "real_amount, fee", "Сумма в валюте баланса",
		"SR Balance Currency", "ChecFee", "Кол-во операций", "Сумма холда"}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 0, 30)   // баланс
	sheet.SetColWidth(1, 1, 11)   // idbalance
	sheet.SetColWidth(2, 2, 12)   // дата
	sheet.SetColWidth(3, 3, 16)   // проверка
	sheet.SetColWidth(4, 4, 15)   // operation_type
	sheet.SetColWidth(6, 6, 18)   // payment_method_type
	sheet.SetColWidth(9, 9, 35)   // merchant_account_name
	sheet.SetColWidth(10, 10, 16) // подразделение
	sheet.SetColWidth(11, 11, 25) // рассчетный счет
	sheet.SetColWidth(12, 12, 16) // поставщик 1С
	sheet.SetColWidth(13, 13, 14) // real_currency / channel_currency
	sheet.SetColWidth(21, 21, 12) // старт тарифа
	sheet.SetColWidth(22, 22, 16) // tariff_condition_id
	sheet.SetColWidth(23, 23, 12) // contract_id
	sheet.SetColWidth(24, 24, 12) // PPрасхолд
	sheet.SetColWidth(26, 32, 16)

	var cell *xlsx.Cell

	for k, v := range M {

		if k.verification == VRF_CHECK_RATE {
			continue
		}

		row := sheet.AddRow()
		row.AddCell().SetString(k.balance_name) //k.tariff.Balance_name)
		row.AddCell().SetString(fmt.Sprint(k.balance_id, "_", k.tariff.Balance_type))
		row.AddCell().SetDate(k.document_date)
		row.AddCell().SetString(k.verification)
		row.AddCell().SetString(k.operation_type)
		row.AddCell().SetString(k.country)
		row.AddCell().SetString(k.payment_type)
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.project_name)
		row.AddCell().SetString(k.merchant_account_name)
		row.AddCell().SetString(k.tariff.Subdivision1C)
		row.AddCell().SetString(k.tariff.RatedAccount)
		row.AddCell().SetString(k.provider1c)
		row.AddCell().SetString(k.channel_currency.Name)
		row.AddCell().SetString(k.balance_currency.Name)

		cell = row.AddCell()
		cell.SetFloat(k.tariff.Percent)
		cell.SetFormat("0.00%")

		cell = row.AddCell()
		cell.SetFloat(k.tariff.Fix)
		cell.SetFormat("0.00")

		row.AddCell().SetFloat(k.tariff.Min)
		row.AddCell().SetFloat(k.tariff.Max)
		row.AddCell().SetFloat(k.tariff.RangeMIN)
		row.AddCell().SetFloat(k.tariff.RangeMAX)

		if k.tariff.DateStart.IsZero() {
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.tariff.DateStart)
		}

		//if k.tariff.id > 0 {
		row.AddCell().SetInt(k.tariff.id)
		//} else {
		//	row.AddCell().SetInt(k.tariff_condition_id)
		//}

		row.AddCell().SetInt(k.contract_id)

		if k.RR_date.IsZero() {
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.RR_date)
		}

		row.AddCell().SetString(k.crypto_network)

		cell = row.AddCell()
		cell.SetFloat(v.channel_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.SR_channel_currency)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.balance_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(v.SR_balance_currency)
		cell.SetFormat("0.00")

		row.AddCell().SetFloat(v.checkFee)
		row.AddCell().SetInt(v.count_operations)

		cell = row.AddCell()
		cell.SetFloat(v.RR_amount)
		cell.SetFormat("0.00")
	}

}

func add_page_copy2(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("Детализация")

	headers := []string{"Баланс", "IDBALANCE", "Дата", "Проверка", "operation_type",
		"payment_method_type", "merchant_name", "merchant_account_name", "Подразделение", "Рассчетный счет",
		"Поставщик 1С", "real_currency / channel_currency", "Валюта баланса", "Акт. тариф формула", "Range",
		"Старт тарифа", "tariff_condition_id", "contract_id", "PPрасхолд", "ДатаРасхолдМ", "CryptoNetWork",
		"ДК тариф формула", "Компенсация BC", "Компенсация RC",
		"real_amount / channel_amount", "real_amount, fee", "Сумма в валюте баланса",
		"SR Balance Currency", "ChecFee", "Кол-во операций", "Сумма холда", "СуммаХолдаМ",
		"К возврату на баланс, оборот", "К возврату на баланс, комиссия"}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 0, 30)   // баланс
	sheet.SetColWidth(1, 1, 11)   // idbalance
	sheet.SetColWidth(2, 2, 12)   // дата
	sheet.SetColWidth(3, 3, 16)   // проверка
	sheet.SetColWidth(4, 4, 15)   // operation_type
	sheet.SetColWidth(5, 5, 18)   // payment_method_type
	sheet.SetColWidth(7, 7, 35)   // merchant_account_name
	sheet.SetColWidth(8, 8, 16)   // подразделение
	sheet.SetColWidth(9, 9, 25)   // рассчетный счет
	sheet.SetColWidth(10, 10, 16) // поставщик 1С
	sheet.SetColWidth(11, 11, 14) // real_currency / channel_currency

	sheet.SetColWidth(15, 15, 12) // старт тарифа
	sheet.SetColWidth(16, 16, 16) // tariff_condition_id
	sheet.SetColWidth(17, 17, 12) // contract_id
	sheet.SetColWidth(18, 19, 12) // PPрасхолд
	sheet.SetColWidth(20, 33, 16)

	var cell *xlsx.Cell

	for k, v := range M {

		if k.verification == VRF_CHECK_RATE {
			continue
		}

		row := sheet.AddRow()
		row.AddCell().SetString(k.balance_name)          //k.tariff.Balance_name)   //0
		row.AddCell().SetInt(k.balance_id)               //1
		row.AddCell().SetDate(k.document_date)           //2
		row.AddCell().SetString(k.verification)          //3
		row.AddCell().SetString(k.operation_type)        //4
		row.AddCell().SetString(k.payment_type)          //5
		row.AddCell().SetString(k.merchant_name)         //6
		row.AddCell().SetString(k.merchant_account_name) //7
		row.AddCell().SetString(k.tariff.Subdivision1C)  //8
		row.AddCell().SetString(k.tariff.RatedAccount)   //9
		row.AddCell().SetString(k.provider1c)            //10
		row.AddCell().SetString(k.channel_currency.Name) //11
		row.AddCell().SetString(k.balance_currency.Name) //12 Валюта баланса
		row.AddCell().SetString(k.tariff.Formula)        //13 Формула
		row.AddCell().SetString(k.tariff.Range)          //14 Range

		if k.tariff.DateStart.IsZero() { //15
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.tariff.DateStart)
		}

		if k.tariff.id > 0 { //16
			row.AddCell().SetInt(k.tariff.id)
		} else {
			row.AddCell().SetInt(k.tariff_condition_id)
		}

		row.AddCell().SetInt(k.contract_id) //17

		if k.RR_date.IsZero() { //18 PPрасхолд
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.RR_date)
		}

		if k.hold_date.IsZero() { //19 ДатаРасхолдМ
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.hold_date)
		}

		row.AddCell().SetString(k.crypto_network)    //20 CryptoNetWork
		row.AddCell().SetString(k.tariff.DK_formula) //21 ДК тариф формула

		cell = row.AddCell() //22 Компенсация BC
		cell.SetFloat(v.CompensationBC)
		cell.SetFormat("0.00")

		cell = row.AddCell() //23 Компенсация RC
		cell.SetFloat(v.CompensationRC)
		cell.SetFormat("0.00")

		cell = row.AddCell() //24
		cell.SetFloat(v.channel_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //25
		cell.SetFloat(v.SR_channel_currency)
		cell.SetFormat("0.00")

		cell = row.AddCell() //26 Сумма в валюте баланса
		cell.SetFloat(v.balance_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //27
		cell.SetFloat(v.SR_balance_currency)
		cell.SetFormat("0.00")

		row.AddCell().SetFloat(v.checkFee)       //28
		row.AddCell().SetInt(v.count_operations) //29

		cell = row.AddCell() //30
		cell.SetFloat(v.RR_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //31 СуммаХолдаМ
		cell.SetFloat(v.hold_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //32 возврат на баланс, оборот
		cell.SetFloat(v.BalanceRefund_turnover)
		cell.SetFormat("0.00")

		cell = row.AddCell() //33 возврат на баланс, комиссия
		cell.SetFloat(v.BalanceRefund_fee)
		cell.SetFormat("0.00")
	}

}

func add_page_svodno(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	M1 := make(map[KeyFields_SummaryInfo]SumFileds)
	for k, v := range M {

		key := KeyFields_SummaryInfo{
			balance_name:     k.balance_name,
			balance_id:       k.balance_id,
			document_date:    k.document_date,
			verification:     k.verification,
			operation_type:   k.operation_type,
			merchant_name:    k.merchant_name,
			tariff:           k.tariff,
			provider1c:       k.provider1c,
			channel_currency: k.channel_currency,
			balance_currency: k.balance_currency,
		}

		sf := M1[key]
		sf.AddValuesFromSF(v)
		M1[key] = sf
	}

	sheet, _ := f.AddSheet("Сводный")

	headers := []string{"Баланс", "IDBALANCE", "Дата", "Проверка", "operation_type", //"payment_method_type",
		"merchant_name", "Подразделение",
		"Поставщик 1С", "real_currency / channel_currency", "Валюта баланса",
		"real_amount / channel_amount", "real_amount, fee", "Сумма в валюте баланса",
		"SR Balance Currency", "ChecFee", "Кол-во операций",
		"К возврату на баланс, оборот", "К возврату на баланс, комиссия", "Компенсация в валюте баланса"}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 0, 30) // баланс
	sheet.SetColWidth(1, 1, 11) // idbalance
	sheet.SetColWidth(2, 2, 12) // дата
	sheet.SetColWidth(3, 3, 16) // проверка
	sheet.SetColWidth(4, 4, 15) // operation_type
	//sheet.SetColWidth(5, 5, 18) // payment_method_type
	//sheet.SetColWidth(7, 7, 35)  // merchant_account_name
	sheet.SetColWidth(7, 8, 16) // подразделение
	sheet.SetColWidth(8, 9, 14) // real_currency / channel_currency
	sheet.SetColWidth(10, 18, 16)

	var cell *xlsx.Cell

	for k, v := range M1 {

		if k.verification == VRF_CHECK_RATE {
			continue
		}

		row := sheet.AddRow()
		row.AddCell().SetString(k.balance_name)   //k.tariff.Balance_name)   //0
		row.AddCell().SetInt(k.balance_id)        //1
		row.AddCell().SetDate(k.document_date)    //2
		row.AddCell().SetString(k.verification)   //3
		row.AddCell().SetString(k.operation_type) //4
		//row.AddCell().SetString(k.payment_type)          //5
		row.AddCell().SetString(k.merchant_name) //5
		//row.AddCell().SetString(k.merchant_account_name) //6
		row.AddCell().SetString(k.tariff.Subdivision1C) //7

		row.AddCell().SetString(k.provider1c)            //8
		row.AddCell().SetString(k.channel_currency.Name) //9
		row.AddCell().SetString(k.balance_currency.Name) //10 Валюта баланса

		cell = row.AddCell() //11
		cell.SetFloat(v.channel_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //12
		cell.SetFloat(v.SR_channel_currency)
		cell.SetFormat("0.00")

		cell = row.AddCell() //13 Сумма в валюте баланса
		cell.SetFloat(v.balance_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //14
		cell.SetFloat(v.SR_balance_currency)
		cell.SetFormat("0.00")

		row.AddCell().SetFloat(v.checkFee)       //15
		row.AddCell().SetInt(v.count_operations) //16

		cell = row.AddCell() //17 возврат на баланс, оборот
		cell.SetFloat(v.BalanceRefund_turnover)
		cell.SetFormat("0.00")

		cell = row.AddCell() //18 возврат на баланс, комиссия
		cell.SetFloat(v.BalanceRefund_fee)
		cell.SetFormat("0.00")

		cell = row.AddCell() //19 Компенсация BC
		cell.SetFloat(v.CompensationBC)
		cell.SetFormat("0.00")
	}

}

func add_page_1_makeTariff(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("1. Создай Тариф")

	headers := []string{
		"Акт. тариф формула", "merchant_name", "merchant_account_name",
		"merchant_account_id", "balance_id", "real_currency / channel_currency",
		"tariff_condition_id", "operation_type", "Range min", "Range max", "tariff_rate_percent",
		"tariff_rate_fix", "tariff_rate_min", "tariff_rate_max", //"real_amount / channel_amount",
	}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 0, 20) // формула
	sheet.SetColWidth(2, 2, 35) // merchant_account_name
	sheet.SetColWidth(4, 4, 11) // idbalance
	sheet.SetColWidth(5, 5, 14) // real_currency / channel_currency
	sheet.SetColWidth(6, 6, 16) // tariff_condition_id
	sheet.SetColWidth(7, 7, 15) // operation_type
	sheet.SetColWidth(8, 14, 16)

	var cell *xlsx.Cell

	already_write := make([]string, 0, 50)

	for k := range M {

		if k.verification != VRF_NO_TARIFF {
			continue
		}

		t := k.tariff_bof.Percent + k.tariff_bof.Fix + k.tariff_bof.Min + k.tariff_bof.Max
		hash := fmt.Sprint(k.merchant_account_id, k.tariff_condition_id, t)

		if slices.Contains(already_write, hash) {
			continue
		} else {
			already_write = append(already_write, hash)
		}

		row := sheet.AddRow()
		row.AddCell().SetString(k.tariff.Formula) //0
		row.AddCell().SetString(k.merchant_name)
		row.AddCell().SetString(k.merchant_account_name) //2
		row.AddCell().SetInt(k.merchant_account_id)
		row.AddCell().SetInt(k.balance_id) //4
		row.AddCell().SetString(k.channel_currency.Name)
		row.AddCell().SetInt(k.tariff_condition_id) //6
		row.AddCell().SetString(k.operation_type)
		row.AddCell().SetFloat(k.tariff.RangeMIN) //8
		row.AddCell().SetFloat(k.tariff.RangeMAX)

		cell = row.AddCell() //10
		cell.SetFloat(k.tariff_bof.Percent)
		cell.SetFormat("0.00%")

		cell = row.AddCell()
		cell.SetFloat(k.tariff_bof.Fix)
		cell.SetFormat("0.00")

		cell = row.AddCell() //12
		cell.SetFloat(k.tariff_bof.Min)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(k.tariff_bof.Max)
		cell.SetFormat("0.00")

		// cell = row.AddCell() //14
		// cell.SetFloat(v.channel_amount)
		// cell.SetFormat("0.00")
	}

}

func add_page_2_checkBilling(f *xlsx.File) {

	sheet, _ := f.AddSheet("2. Сверься с Биллингом")

	headers := []string{"id / operation_id",
		"Проверка", "Конвертация", "operation_type", "merchant_name", "merchant_account_name",
		"merchant_account_id", "real_currency / channel_currency", "Валюта баланса", "Старт тарифа",
		"DragonPay MA tariff",
		"Акт. тариф формула", "tariff_condition_id", "ФормулаБОФ", "real_amount / channel_amount",
		"Сумма в валюте баланса", "SR Balance Currency", "BOF fee_amount", "Check Fee",
	}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 2, 15)  // operation_type
	sheet.SetColWidth(5, 5, 35)  // merchant_account_name
	sheet.SetColWidth(7, 10, 14) // real_currency / channel_currency
	sheet.SetColWidth(14, 18, 16)

	var cell *xlsx.Cell

	for _, op := range storage.Registry {

		if op.CheckFee == 0 || op.Verification == VRF_NO_IN_REG || op.Verification == VRF_VALID_REG_FEE {
			continue
		}

		var t, t_bof Tariff
		if op.Tariff != nil {
			t = *op.Tariff
		}

		if op.Tariff_bof != nil {
			t_bof = *op.Tariff_bof
		}

		row := sheet.AddRow()

		cell = row.AddCell() //0
		cell.SetString(strconv.Itoa(op.Operation_id))
		cell.SetFormat("0")

		row.AddCell().SetString(op.Verification)          //1
		row.AddCell().SetString(t.Convertation)           //2
		row.AddCell().SetString(op.Operation_type)        //3
		row.AddCell().SetString(op.Merchant_name)         //4
		row.AddCell().SetString(op.Merchant_account_name) //5
		row.AddCell().SetInt(op.Merchant_account_id)      //6
		row.AddCell().SetString(op.Channel_currency.Name) //7
		row.AddCell().SetString(op.Balance_currency.Name) //8 Валюта баланса

		if t.DateStart.IsZero() { //9
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(t.DateStart)
		}

		row.AddCell().SetString("")        //10 dragonpay
		row.AddCell().SetString(t.Formula) //11

		if t.id > 0 { //12
			row.AddCell().SetInt(t.id)
		} else {
			row.AddCell().SetInt(t_bof.id)
		}

		row.AddCell().SetString(t_bof.Formula)
		// if op.Tariff_bof != nil { //13
		// 	row.AddCell().SetString(op.Tariff_bof.Formula)
		// } else {
		// 	row.AddCell().SetString("")
		// }

		cell = row.AddCell() //14 сумма в валюте канала
		cell.SetFloat(op.Channel_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //15 Сумма в валюте баланса
		cell.SetFloat(op.Balance_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //16
		cell.SetFloat(op.SR_balance_currency)
		cell.SetFormat("0.00")

		cell = row.AddCell() //17 BOF fee amount
		cell.SetFloat(op.Fee_amount)
		cell.SetFormat("0.00")

		cell = row.AddCell() //18 check fee
		cell.SetFloat(op.CheckFee)
		cell.SetFormat("0.00")
	}

}

func add_page_3_checkRate(f *xlsx.File, M map[KeyFields_SummaryInfo]SumFileds) {

	sheet, _ := f.AddSheet("3. Проверь Тарифы")

	headers := []string{
		"merchant_account_name", "Старт Тарифа", "operation_type",
		"Акт. тариф", "Акт. Фикс", "Акт. Мин", "Акт. Макс",
		"tariff_condition_id", "tariff_rate_percent",
		"tariff_rate_fix", "tariff_rate_min", "tariff_rate_max", "CHECKRATES",
	}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 0, 35) // merchant_account_name
	sheet.SetColWidth(1, 1, 12) // старт тарифа
	sheet.SetColWidth(2, 2, 15) // operation_type
	sheet.SetColWidth(3, 12, 15)

	sheet.SetColWidth(8, 14, 16)

	var cell *xlsx.Cell

	already_write := make([]string, 0, 50)

	for k, v := range M {

		if v.checkRates == 0 {
			continue
		}

		t1 := k.tariff.Percent + k.tariff.Fix + k.tariff.Min + k.tariff.Max
		t2 := k.tariff_bof.Percent + k.tariff_bof.Fix + k.tariff_bof.Min + k.tariff_bof.Max
		hash := fmt.Sprint(k.tariff_condition_id, t1, t2)

		if slices.Contains(already_write, hash) {
			continue
		} else {
			already_write = append(already_write, hash)
		}

		row := sheet.AddRow()

		row.AddCell().SetString(k.merchant_account_name) //0

		if k.tariff.DateStart.IsZero() { //1
			row.AddCell().SetString("")
		} else {
			row.AddCell().SetDate(k.tariff.DateStart)
		}

		row.AddCell().SetString(k.operation_type) //2

		cell = row.AddCell()
		cell.SetFloat(k.tariff.Percent)
		cell.SetFormat("0.00%")

		cell = row.AddCell() //4
		cell.SetFloat(k.tariff.Fix)
		cell.SetFormat("0.00")

		row.AddCell().SetFloat(k.tariff.Min)
		row.AddCell().SetFloat(k.tariff.Max)

		if k.tariff.id > 0 { //7
			row.AddCell().SetInt(k.tariff.id)
		} else {
			row.AddCell().SetInt(k.tariff_condition_id)
		}

		cell = row.AddCell() //8
		cell.SetFloat(k.tariff_bof.Percent)
		cell.SetFormat("0.00%")

		cell = row.AddCell()
		cell.SetFloat(k.tariff_bof.Fix)
		cell.SetFormat("0.00")

		cell = row.AddCell() //10
		cell.SetFloat(k.tariff_bof.Min)
		cell.SetFormat("0.00")

		cell = row.AddCell()
		cell.SetFloat(k.tariff_bof.Max)
		cell.SetFormat("0.00")

		cell = row.AddCell() //12
		cell.SetFloat(v.checkRates)
		cell.SetFormat("0.00")

	}

}

func add_page_4_noProviderReg(f *xlsx.File) {

	sheet, _ := f.AddSheet("4. Нет в реестре ПС")

	headers := []string{"id / operation_id", "Баланс", "IDBALANCE", "Дата", "Проверка", "operation_type",
		"merchant_name", "merchant_account_name", "Конвертация", "real_currency / channel_currency",
		"Валюта баланса", "Сумма Реестра Провайдера", "real_amount / channel_amount",
		"Сумма в валюте баланса", "SR Balance Currency"}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 0, 20)  // id
	sheet.SetColWidth(1, 1, 30)  // баланс
	sheet.SetColWidth(2, 2, 11)  // idbalance
	sheet.SetColWidth(3, 3, 12)  // дата
	sheet.SetColWidth(4, 4, 16)  // проверка
	sheet.SetColWidth(5, 5, 15)  // operation_type
	sheet.SetColWidth(7, 7, 35)  // MA
	sheet.SetColWidth(9, 10, 14) // real_currency / channel_currency
	sheet.SetColWidth(11, 14, 16)

	var cell *xlsx.Cell

	for _, o := range storage.Registry {

		if o.Verification == VRF_NO_IN_REG || o.Verification == VRF_CHECK_RATE {

			row := sheet.AddRow()

			cell = row.AddCell() //0
			cell.SetString(strconv.Itoa(o.Operation_id))
			cell.SetFormat("0")

			if o.Tariff != nil { //1
				row.AddCell().SetString(o.Tariff.Balance_name)
			} else {
				row.AddCell().SetString("")
			}

			row.AddCell().SetInt(o.Balance_id)
			row.AddCell().SetDate(o.Document_date) //3
			row.AddCell().SetString(o.Verification)
			row.AddCell().SetString(o.Operation_type) //5
			row.AddCell().SetString(o.Merchant_name)
			row.AddCell().SetString(o.Merchant_account_name) //7

			if o.Tariff != nil { //8
				row.AddCell().SetString(o.Tariff.Convertation)
			} else {
				row.AddCell().SetString("")
			}

			row.AddCell().SetString(o.Channel_currency.Name) //9
			row.AddCell().SetString(o.Balance_currency.Name) //10

			cell = row.AddCell() //1
			cell.SetFloat(0)
			cell.SetFormat("0.00")

			cell = row.AddCell() //12
			cell.SetFloat(o.Channel_amount)
			cell.SetFormat("0.00")

			cell = row.AddCell() //13
			cell.SetFloat(o.SR_channel_currency)
			cell.SetFormat("0.00")

			cell = row.AddCell() //14
			cell.SetFloat(o.Balance_amount)
			cell.SetFormat("0.00")
		}
	}

}

func add_page_5_no_Perevodix_KGX_verification(f *xlsx.File) {

	sheet, _ := f.AddSheet("5. Проверка KGX_Perevodix")

	headers := []string{"Баланс", "operation_type", "Валюта баланса",
		"payment_method_type", "Поставщик 1С", "Проверка"}

	style := xlsx.NewStyle()
	style.Fill.FgColor = "5B9BD5"
	style.Fill.PatternType = "solid"
	style.ApplyFill = true
	style.Alignment.WrapText = true
	style.Alignment.Horizontal = "center"
	style.Alignment.Vertical = "center"
	style.ApplyAlignment = true
	style.Font.Bold = true
	style.Font.Color = "FFFFFF"

	row := sheet.AddRow()

	for _, v := range headers {
		cell := row.AddCell()
		cell.SetString(v)
		cell.SetStyle(style)
	}

	sheet.SetColWidth(0, 5, 15) //

	already_write := make([]string, 0, 100)

	for _, o := range storage.Registry {

		if o.Verification_KGX == VRF_OK {
			continue
		}

		hash := fmt.Sprint(o.Provider_name, o.Operation_type, o.Balance_currency.Name, o.Payment_type)

		if slices.Contains(already_write, hash) {
			continue
		} else {
			already_write = append(already_write, hash)
		}

		row := sheet.AddRow()

		row.AddCell().SetString(o.Provider_name)
		row.AddCell().SetString(o.Operation_type)
		row.AddCell().SetString(o.Balance_currency.Name)
		row.AddCell().SetString(o.Payment_type)
		row.AddCell().SetString(o.Provider1c)
		row.AddCell().SetString(o.Verification_KGX)

	}

}
