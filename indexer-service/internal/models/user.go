package models

type User struct {
	ID         int    `json:"" db:"id"`
	Username   string `json:"id" db:"username"`
	Karma      int    `json:"karma" db:"karma"`
	About      string `json:"about" db:"about"`
	Created_At int64  `json:"created" db:"created_at"`
	Submitted  []int  `json:"submitted" db:"submitted_ids"`
}

func (u *User) IsValid() bool {
	return u.Username != "" && u.About != "" && u.Karma >= 0 && u.Created_At > 0
}

