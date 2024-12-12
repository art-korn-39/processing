package merchants

type Merchant struct {
	Contractor_name string `db:"contractor_name"`
	Contractor_guid string `db:"contractor_guid"`
	Merchant_name   string `db:"merchant_name"`
	Merchant_id     int    `db:"merchant_id"`
	Project_name    string `db:"project_name"`
	Project_id      int    `db:"project_id"`
	Project_url     string `db:"project_url"`
}
