package bagman

/*
Institution represents an institution in fluctus. Name is the
institution's full name. BriefName is a shortened name.
Identifier is the institution's domain name.
*/
type Institution struct {
	Pid        string `json:"pid"`
	Name       string `json:"name"`
	BriefName  string `json:"brief_name"`
	Identifier string `json:"identifier"`
	DpnUuid    string `json:"dpn_uuid"`
}
