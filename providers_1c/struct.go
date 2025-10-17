package providers_1c

type Provider1c struct {
	Guid                string `db:"guid"`
	Name                string `db:"name"`
	Payment_method_name string `db:"payment_method_name"`
	Payment_method_id   int    `db:"payment_method_id"`
	Payment_type_name   string `db:"payment_type_name"`
	Payment_type_guid   string `db:"payment_type_guid"`
	Payment_type_id     int    `db:"payment_type_id"`
	Provider_name       string `db:"provider_name"`
	Provider_guid       string `db:"provider_guid"`
	Currency            string `db:"currency"`
}
