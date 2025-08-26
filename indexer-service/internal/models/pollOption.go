package models

type PollOption struct {
	ID         int    `json:"id" db:"id"`
	Type       string `json:"type" db:"type"`
	PollID     int    `json:"poll" db:"poll_id"`
	Author     string `json:"by" db:"author"`
	OptionText string `json:"text" db:"option_text"`
	CreatedAt  int64  `json:"time" db:"created_at"`
	Votes      int    `json:"score" db:"votes"`
}

func (po *PollOption) IsValid() bool {
	return po.ID > 0 && po.Type == "pollOption" && po.PollID > 0 && po.OptionText != "" && po.CreatedAt > 0
}

