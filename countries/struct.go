package countries

// 104 bytes
type Country struct {
	Region   string `db:"region_name"`
	Name     string `db:"name"`
	Name_en  string `db:"name_en"`
	Code     int    `db:"code"`
	Code2    string `db:"code2"`
	Code3    string `db:"code3"`
	Currency string `db:"currency"`
}

func (obj Country) IsNil() bool {
	return obj == Country{}
}

func (obj Country) Exist() bool {
	return obj != Country{}
}
