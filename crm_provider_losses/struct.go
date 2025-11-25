package crm_provider_losses

import (
	"time"
)

type Operation struct {
	ID                     string    `json:"id" db:"id"`
	Title                  string    `json:"title" db:"title"`
	Created_on             time.Time `json:"createdon" db:"created_on"`
	Modified_on            time.Time `json:"modifiedon" db:"modified_on"`
	Created_by_id          string    `json:"createdbyid" db:"created_by_id"`
	Modified_by_id         string    `json:"modifiedbyid" db:"modified_by_id"`
	Payment_provider_id    string    `json:"paymentproviderid" db:"payment_provider_id"`
	Loss_key               string    `json:"losskey" db:"loss_key"`
	Loss_balance_code      string    `json:"lossbalancecode" db:"loss_balance_code"`
	Loss_date              time.Time `json:"lossdate" db:"loss_date"`
	Loss_id                int       `json:"lossid" db:"loss_id"`
	Legal_entity           string    `json:"legalentitysrcname" db:"legal_entity"`
	Provider_solution_name string    `json:"paymentprovidersrcname" db:"provider_solution_name"`
	Provider_margin        float64   `json:"providermargin" db:"provider_margin"`
	Loss_provider_balance  string    `json:"lossproviderbalancecode" db:"loss_provider_balance"`
	Revisor_id             string    `json:"revisorid" db:"revisor_id"`
	Loss_occurence_date    time.Time `json:"lossoccurencedate" db:"loss_occurence_date"`
	Loss_recolection_date  time.Time `json:"lossrecolectiondate" db:"loss_recolection_date"`
	Loss_sum               float64   `json:"losssum" db:"loss_sum"`
	Our_loss_sum           float64   `json:"ourlosssumfact" db:"our_loss_sum"`
	Write_off_date         time.Time `json:"writeoffdate" db:"write_off_date"`
	Provider_loss_sum      float64   `json:"providerlosssum" db:"provider_loss_sum"`
	Comment                string    `json:"ownfundsrevisorcomment" db:"comment"`

	Status_name                string `db:"status"`
	Currency_name              string `db:"currency"`
	Loss_type_name             string `db:"loss_type"`
	Loss_potential_status_name string `db:"loss_potential_status"`

	// вложенные структуры json файла
	// перекладываем их значения на верхний уровень
	Status              map[string]string `json:"status"`
	Currency            map[string]string `json:"currency"` //Alpha3Code
	LegalEntity         map[string]string `json:"legalentity"`
	LossType            map[string]string `json:"losstype"`
	LossPotentialStatus map[string]string `json:"losspotentialstatus"`
}

func (op *Operation) fill() {
	op.Currency_name = op.Currency["Alpha3Code"]
	op.Status_name = op.Status["Name"]
	op.Loss_type_name = op.LossType["Name"]
	op.Loss_type_name = op.LossType["Name"]
}
