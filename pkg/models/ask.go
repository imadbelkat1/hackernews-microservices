package models

// Ask represents a Hacker News ask post with its details.
// It includes fields for ID, type, title, text, score, author, creation time, and a list of reply IDs.
type Ask struct {
	ID            int    `json:"id" db:"id"`
	Type          string `json:"type" db:"type"`
	Title         string `json:"title" db:"title"`
	Text          string `json:"text" db:"text"`
	Score         int    `json:"score" db:"score"`
	Author        string `json:"by" db:"author"`
	Reply_ids     []int  `json:"kids" db:"reply_ids"`
	Replies_count int    `json:"descendants" db:"replies_count"`
	Created_At    int64  `json:"time" db:"created_at"`
}

func (a *Ask) IsValid() bool {
	return a.ID > 0 && a.Type == "ask" && a.Title != "" && a.Author != "" && a.Created_At > 0
}

