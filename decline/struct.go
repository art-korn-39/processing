package decline

import (
	"app/file"
	"time"
)

type Operation struct {
	Message_id   int `db:"message_id"`
	Operation_id int `db:"operation_id"`

	Date           time.Time `db:"date"`
	Date_day       time.Time `db:"date_day"`
	Created_at     time.Time `db:"created_at"`
	Created_at_day time.Time `db:"created_at_day"`

	Merchant_id   int    `db:"merchant_id"`
	Merchant_name string `db:"merchant_name"`

	Provider_id   int    `db:"provider_id"`
	Provider_name string `db:"provider_name"`

	Merchant_account_id   int    `db:"merchant_account_id"`
	Merchant_account_name string `db:"merchant_account_name"`

	Operation_type string `db:"operation_type"`
	Comment        string `db:"comment"`

	Incoming_amount   float64 `db:"incoming_amount"`
	Incoming_currency string  `db:"incoming_currency"`

	Coverted_amount   float64 `db:"coverted_amount"`
	Coverted_currency string  `db:"coverted_currency"`

	Link      string `db:"link"`
	Bank_card int    `db:"bank_card"`
}

type DeclineFile struct {
	Messages []Message `json:"messages"`
	fileInfo *file.FileInfo
}

type Message struct {
	Id       int                 `json:"id"`
	Date_str string              `json:"date"`
	Text     []map[string]string `json:"text_entities"`
}
