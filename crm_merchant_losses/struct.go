package crm_merchant_losses

import (
	"time"
)

type Operation struct {
	ID                               string    `json:"id" db:"id"`
	Title                            string    `json:"title" db:"title"`
	Created_on                       time.Time `json:"createdon" db:"created_on"`
	Modified_on                      time.Time `json:"modifiedon" db:"modified_on"`
	Created_by_id                    string    `json:"createdbyid" db:"created_by_id"`
	Modified_by_id                   string    `json:"modifiedbyid" db:"modified_by_id"`
	Merchant_id                      string    `json:"merchantid" db:"merchant_id"`
	Loss_in_provider_id              string    `json:"lossinproviderid" db:"loss_in_provider_id"`
	Loss_sum                         float64   `json:"losssum" db:"loss_sum"`
	Merchant_name                    string    `json:"merchantsrcname" db:"merchant_name"`
	Hold_comment_revision            string    `json:"holdrevisioncomment" db:"hold_comment_revision"`
	Hold_comment_fin                 string    `json:"holdfincomment" db:"hold_comment_fin"`
	Hold_date_removal                time.Time `json:"holdremovaldate" db:"hold_date_removal"`
	Hold_sum                         float64   `json:"holdsum" db:"hold_sum"`
	Hold_date                        time.Time `json:"holddate" db:"hold_date"`
	Provider_loss_sum                float64   `json:"providerlosssum" db:"provider_loss_sum"`
	Our_loss_sum                     float64   `json:"ourlosssum" db:"our_loss_sum"`
	Write_off_comment_date           time.Time `json:"writeoffkamcommentdate" db:"write_off_comment_date"`
	Write_off_comment                string    `json:"writeoffkamcomment" db:"write_off_comment"`
	Write_off_margin_fact            float64   `json:"writeoffonmarginfact" db:"write_off_margin_fact"`
	Write_off_sum_fact               float64   `json:"writeoffonsumfact" db:"write_off_sum_fact"`
	Write_off_margin                 float64   `json:"writeoffonmargin" db:"write_off_margin"`
	Write_off_sum                    float64   `json:"writeoffonsum" db:"write_off_sum"`
	Merchant_kam_id                  string    `json:"merchantkamid" db:"merchant_kam_id"`
	Merchant_fin_id                  string    `json:"merchanfinmanagerid" db:"merchant_fin_id"`
	Loss_merchant_number             int       `json:"lossinmerchantcaseno" db:"loss_merchant_number"`
	Write_off_fin_comment            string    `json:"writeofffinkamcomment" db:"write_off_fin_comment"`
	Kam_last_date_notif              time.Time `json:"kamlastnotificationdate" db:"kam_last_date_notif"`
	Write_off_done_date              time.Time `json:"writeoffdonedate" db:"write_off_done_date"`
	Kam_done_date                    time.Time `json:"kamdonedate" db:"kam_done_date"`
	Hold_done_date                   time.Time `json:"holddonedate" db:"hold_done_date"`
	Is_partial_write_off             bool      `json:"ispartialwriteoff" db:"is_partial_write_off"`
	Write_off_reported_date          time.Time `json:"writeoffreporteddate" db:"write_off_reported_date"`
	Write_off_last_notification_date time.Time `json:"writeofflastnotificationdate" db:"write_off_last_notification_date"`
	Is_sum_not_match                 bool      `json:"issumnotmatch" db:"is_sum_not_match"`
	Is_cd_approved                   string    `json:"Iscdapprovedyesnoid" db:"is_cd_approved"`
	Our_loss_sum_fact                float64   `json:"ourlosssumfact" db:"our_loss_sum_fact"`
	Revisor_id                       string    `json:"revisorid" db:"revisor_id"`
	Actualization_date               time.Time `json:"actualizationdate" db:"actualization_date"`
	Unhold_cause_id                  string    `json:"unholdcauseid" db:"unhold_cause_id"`
	Merchant_bof_id                  int       `json:"merchantbofid" db:"merchant_bof_id"`
	Merchant_kam_sub_id              string    `json:"merchantkamsubstituteid" db:"merchant_kam_sub_id"`
	Revisor_contact_id               string    `json:"revisorcontactid" db:"revisor_contact_id"`
	Write_off_sum_left               float64   `json:"writeoffonsumleft" db:"write_off_sum_left"`
	Written_off_before               string    `json:"writtenoffbeforeresultid" db:"written_off_before"`
	Date_written_off_before          time.Time `json:"datewrittenoffbefore" db:"date_written_off_before"`
	Sum_written_off_before           float64   `json:"sumwrittenoffbefore" db:"sum_written_off_before"`
	Is_written_off_before            bool      `json:"iswrittenoffbefore" db:"is_written_off_before"`
	Our_loss_full_sum_fact           float64   `json:"ourlossfullsumfact" db:"our_loss_full_sum_fact"`
	Merchant_kam_substitutes         string    `json:"merchantkamsubstitutes" db:"merchant_kam_substitutes"`
	Is_hold_sum_not_match            bool      `json:"isholdsumsnotmatch" db:"is_hold_sum_not_match"`

	Status_name                string `db:"status"`
	Currency_name              string `db:"currency"`
	Loss_potential_status_name string `db:"loss_potential_status"`
	Loss_coverage_method_name  string `db:"loss_coverage_method"`
	Result_name                string `db:"result"`

	// вложенные структуры json файла
	// перекладываем их значения на верхний уровень
	Status              map[string]string `json:"status"`
	Currency            map[string]string `json:"currency"` //Alpha3Code
	LossPotentialStatus map[string]string `json:"losspotentialstatus"`
	LossCoverageMethod  map[string]string `json:"losscoveragemethod"`
	Result              map[string]string `json:"result"`
}

func (op *Operation) fill() {
	op.Currency_name = op.Currency["Alpha3Code"]
	op.Status_name = op.Status["Name"]
	op.Loss_potential_status_name = op.LossPotentialStatus["Name"]
	op.Loss_coverage_method_name = op.LossCoverageMethod["Name"]
	op.Result_name = op.Result["Name"]
}
