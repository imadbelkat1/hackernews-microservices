package models

// Poll represents a Hacker News poll
type Poll struct {
	ID          int    `json:"id" db:"id"`
	Type        string `json:"type" db:"type"`
	Title       string `json:"title" db:"title"`
	Score       int    `json:"score" db:"score"`
	Author      string `json:"by" db:"author"`
	Created_At  int64  `json:"time" db:"created_at"`
	PollOptions []int  `json:"parts" db:"poll_options"`
	Reply_Ids   []int  `json:"kids" db:"reply_ids"`
}

func (p *Poll) IsValid() bool {
	return p.ID > 0 && p.Type == "poll" && p.Title != "" && p.Author != "" && p.Created_At > 0
}

