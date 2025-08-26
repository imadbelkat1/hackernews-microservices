package models

// Job represents a Hacker News job posting with its details.
// It includes fields for ID, type, text, URL, score, author, and creation time.
type Job struct {
	ID         int    `json:"id" db:"id"`
	Type       string `json:"type" db:"type"`
	Title      string `json:"title" db:"title"`
	Text       string `json:"text" db:"text"`
	URL        string `json:"url" db:"url"`
	Score      int    `json:"score" db:"score"`
	Author     string `json:"by" db:"author"`
	Created_At int64  `json:"time" db:"created_at"`
}

func (j *Job) IsValid() bool {
	return j.ID > 0 && j.Type == "job" && j.Title != "" && j.Author != "" && j.Created_At > 0
}

