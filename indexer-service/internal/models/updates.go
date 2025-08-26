package models

// Update represents a data update with its details.
type Update struct {
	IDs      []int    `json:"items" db:"id"`           // Unique identifier for the update
	Profiles []string `json:"profiles" db:"profiles" ` // Comma-separated list of profiles
}

// IsValid checks if the update is valid.
func (u *Update) IsValid() bool {
	return len(u.IDs) > 0 && len(u.Profiles) > 0
}

