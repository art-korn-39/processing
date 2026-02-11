package provider_registry

import (
	"app/currency"
	"app/util"
	"time"
)

type Operation struct {
	Id                           int       `db:"operation_id"`
	Transaction_completed_at     time.Time `db:"transaction_completed_at"`
	Transaction_completed_at_day time.Time `db:"transaction_completed_at_day"`
	Transaction_created_at       time.Time `db:"transaction_created_at"`
	Operation_type               string    `db:"operation_type"`
	Country                      string    `db:"country"`
	Payment_type                 string    `db:"payment_method_type"`
	Merchant_name                string    `db:"merchant_name"`
	Rate                         float64   `db:"rate"`
	Amount                       float64   `db:"amount"`
	Channel_amount               float64   `db:"channel_amount"`

	Provider_name          string  `db:"provider_name"`
	Merchant_account_name  string  `db:"merchant_account_name"`
	Provider_payment_id    string  `db:"provider_payment_id"`
	Project_url            string  `db:"project_url"`
	Project_id             int     `db:"project_id"`
	Operation_status       string  `db:"operation_status"`
	Account_number         string  `db:"account_number"`
	BR_amount              float64 `db:"br_amount"`
	Balance                string  `db:"balance"`
	Provider1c             string  `db:"provider1c"`
	Team                   string  `db:"team"`
	Team_id                string  `db:"team_id"`
	BR_fix                 float64 `db:"br_fix"`
	User_tradex            string  `db:"user_tradex"`
	Comission_tradex       float64 `db:"comission_tradex"`
	Bonuses_tradex         string  `db:"bonuses_tradex"`
	Provider_amount_tradex float64 `db:"provider_amount_tradex"`

	Partner_id   string
	Verification string
	Save         bool

	Channel_currency_str  string `db:"channel_currency"`
	Provider_currency_str string `db:"provider_currency"`

	Channel_currency  currency.Currency
	Provider_currency currency.Currency
}

// 0: conversion (file reading), 1: convert (file parsing), 2: psql (loading from psql)
func (o *Operation) StartingFill(option int) {

	// чтение файлов через РЗ
	if option == 0 {

		// if o.Provider_currency.Name == "EUR" && o.Rate != 0 && o.Provider_name != "SepaViaInpay" {
		// 	o.Rate = 1 / o.Rate
		// }

		o.Channel_currency_str = o.Channel_currency.Name
		o.Provider_currency_str = o.Provider_currency.Name

	} else { // при конвертации + загрузка из psql
		o.Channel_currency = currency.New(o.Channel_currency_str)
		o.Provider_currency = currency.New(o.Provider_currency_str)
		o.BR_amount = util.Round(o.BR_amount, 4)
		o.BR_fix = util.Round(o.BR_fix, 4)

		if option == 2 {
			if o.Channel_currency == o.Provider_currency {
				o.Provider_amount_tradex = o.Amount
				o.Amount = o.Channel_amount
			}
		}
	}

	if o.Amount != 0 {
		o.Rate = o.Channel_amount / o.Amount
	} else {
		o.Rate = 0
	}

	o.Transaction_completed_at_day = o.Transaction_completed_at.Truncate(24 * time.Hour)

}

func (o *Operation) SetVerification(bof_usage, bof2_usage, use_daily_rates bool) {

	if (!use_daily_rates && bof_usage) || (use_daily_rates && o.Rate != 0) {
		if o.Channel_currency == o.Provider_currency && o.Amount != o.Provider_amount_tradex {
			o.Verification = "ОК, проверь amount"
		} else {
			o.Verification = "ОК"
		}
	} else if bof2_usage { // нашли по второму ключу
		o.Verification = "ОК, найдено по operation_id" // т.к только для tradex, то второй ключ это всегда operation_id
	} else {
		o.Verification = "Не найдено"
	}

}
