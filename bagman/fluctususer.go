package bagman

// User struct is used for logging in to fluctus.
type User struct {
	Email     string `json:"email"`
	Password  string `json:"password,omitempty"`
	ApiKey    string `json:"api_secret_key,omitempty"`
	AuthToken string `json:"authenticity_token,omitempty"`
}
