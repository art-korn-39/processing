package providers

// 64 bytes
type Provider struct {
	Contractor_guid string `db:"contractor_guid"`
	Contractor_name string `db:"contractor_name"`
	Provider_name   string `db:"provider_name"`
	Provider_id     int    `db:"provider_id"`
	Is_tradex       bool   `db:"is_tradex"`
}

func Is_tradex(provider_id int) bool {

	return data_tradex[provider_id]

}
