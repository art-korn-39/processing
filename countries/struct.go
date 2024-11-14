package countries

type Country struct {
	Region   string `db:"region_name"`
	Name     string `db:"name"`
	Name_en  string `db:"name_en"`
	Code     int    `db:"code"`
	Code2    string `db:"code2"`
	Code3    string `db:"code3"`
	Currency string `db:"currency"`
}
