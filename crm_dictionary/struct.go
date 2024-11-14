package crm_dictionary

//id,ParentId,UsrProcessingPaymentMethodId,Name
type Payment_method struct {
	Id        string `json:"Id" db:"id"`
	Parent_id string `json:"ParentId" db:"parent_id"`
	Bof_id    int    `json:"UsrProcessingPaymentMethodId" db:"bof_id"`
	Name      string `json:"Name" db:"name"`
}

//id,name,BOFid,MethodId
type Payment_type struct {
	Id         string `json:"Id" db:"id"`
	Method_id  string `json:"MethodId" db:"method_id"`
	Bof_id     int    `db:"bof_id"`
	Bof_id_str string `json:"BOFid"`
	Name       string `json:"Name" db:"name"`
}
