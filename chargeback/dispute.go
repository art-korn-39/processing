package chargeback

import (
	"time"
)

type Dispute struct {
	StateChangeDate time.Time `json:"StateChangeDate"`
	Operation_guid  string    `json:"OperationId"`
	Chargeback_id   string    `json:"ChargebackId"`

	State_name string

	State map[string]string `json:"state"`
}

func (ds *Dispute) fill() {
	ds.State_name = ds.State["Name"]
}
