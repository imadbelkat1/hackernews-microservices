package repository

import (
	"context"
	"database/sql"

	"indexer-service/internal/models"
	"indexer-service/pkg/database"


	"github.com/lib/pq"
)

// StoryRepository implements repository.StoryRepository
type StoryRepository struct {
	db *sql.DB
}

// NewStoryRepository creates a new StoryRepository instance
func NewStoryRepository() StoryRepository {
	return StoryRepository{
		db: database.GetDB(),
	}
}

// Create inserts a new story
func (r *StoryRepository) Create(ctx context.Context, story *models.Story) error {
	CommentsIds := make(pq.Int64Array, len(story.Comments_ids))
	for i, v := range story.Comments_ids {
		CommentsIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO stories (id, type, title, url, score, author, created_at, comments_ids, comments_count) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		story.ID, story.Type, story.Title, story.URL, story.Score,
		story.Author, story.Created_At, CommentsIds, story.Comments_count)
	return err
}

// GetByID retrieves a story by ID
func (r *StoryRepository) GetByID(ctx context.Context, id int) (*models.Story, error) {
	story := &models.Story{}
	var commentsIds pq.Int64Array

	err := r.db.QueryRowContext(ctx,
		`SELECT id, type, title, url, score, author, created_at, comments_ids, comments_count 
		 FROM stories WHERE id = $1`, id).Scan(
		&story.ID, &story.Type, &story.Title, &story.URL, &story.Score,
		&story.Author, &story.Created_At, &commentsIds, &story.Comments_count)
	if err != nil {
		return nil, err
	}

	story.Comments_ids = make([]int, len(commentsIds))
	for i, v := range commentsIds {
		story.Comments_ids[i] = int(v)
	}

	return story, nil
}

// Update updates an existing story
func (r *StoryRepository) Update(ctx context.Context, story *models.Story) error {
	CommentsIds := make(pq.Int64Array, len(story.Comments_ids))
	for i, v := range story.Comments_ids {
		CommentsIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`UPDATE stories SET type=$2, title=$3, url=$4, score=$5, author=$6, 
		 created_at=$7,comments_ids=$8, comments_count=$9 WHERE id=$1`,
		story.ID, story.Type, story.Title, story.URL, story.Score,
		story.Author, story.Created_At, CommentsIds, story.Comments_count)
	return err
}

// Delete removes a story by ID
func (r *StoryRepository) Delete(ctx context.Context, id int) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM stories WHERE id = $1`, id)
	return err
}

// GetAll retrieves all stories
func (r *StoryRepository) GetAll(ctx context.Context) ([]*models.Story, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, url, score, author, created_at, comments_ids, comments_count 
		 FROM stories ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStories(rows)
}

// GetRecent retrieves recent stories
func (r *StoryRepository) GetRecent(ctx context.Context, limit int) ([]*models.Story, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, url, score, author, created_at, comments_ids, comments_count 
		 FROM stories ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStories(rows)
}

// GetByMinScore retrieves stories with minimum score
func (r *StoryRepository) GetByMinScore(ctx context.Context, minScore int) ([]*models.Story, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, url, score, author, created_at, comments_ids, comments_count 
		 FROM stories WHERE score >= $1 ORDER BY score DESC`, minScore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStories(rows)
}

// GetByAuthor retrieves stories by author
func (r *StoryRepository) GetByAuthor(ctx context.Context, author string) ([]*models.Story, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, url, score, author, created_at, comments_ids, comments_count 
		 FROM stories WHERE author = $1 ORDER BY created_at DESC`, author)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStories(rows)
}

// GetByDateRange retrieves stories within date range
func (r *StoryRepository) GetByDateRange(ctx context.Context, start, end int64) ([]*models.Story, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, url, score, author, created_at, comments_ids, comments_count 
		 FROM stories WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at DESC`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStories(rows)
}

// UpdateScore updates story score
func (r *StoryRepository) UpdateScore(ctx context.Context, id int, score int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE stories SET score = $1 WHERE id = $2`, score, id)
	return err
}

// Update comments IDs
func (r *StoryRepository) UpdateCommentsIDs(ctx context.Context, id int, commentsIDs []int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE stories SET comments_ids = $1 WHERE id = $2`,
		pq.Array(commentsIDs), id)
	return err
}

// UpdateCommentsCount updates comments count
func (r *StoryRepository) UpdateCommentsCount(ctx context.Context, id int, count int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE stories SET comments_count = $1 WHERE id = $2`, count, id)
	return err
}

// CreateBatch creates multiple stories
func (r *StoryRepository) CreateBatch(ctx context.Context, stories []*models.Story) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO stories (id, type, title, url, score, author, created_at, comments_ids, comments_count) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO UPDATE`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, story := range stories {
		CommentsIds := make(pq.Int64Array, len(story.Comments_ids))
		for i, v := range story.Comments_ids {
			CommentsIds[i] = int64(v)
		}
		_, err := stmt.ExecContext(ctx, story.ID, story.Type, story.Title, story.URL,
			story.Score, story.Author, story.Created_At, CommentsIds, story.Comments_count)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *StoryRepository) CreateBatchWithExistingIDs(ctx context.Context, stories []*models.Story) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO stories (id, type, title, url, score, author, created_at, comments_ids, comments_count) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO UPDATE SET
		 type = EXCLUDED.type, title = EXCLUDED.title, url = EXCLUDED.url,
		 score = EXCLUDED.score, author = EXCLUDED.author, created_at = EXCLUDED.created_at,
		 comments_ids = EXCLUDED.comments_ids, comments_count = EXCLUDED.comments_count`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, story := range stories {
		CommentsIds := make(pq.Int64Array, len(story.Comments_ids))
		for i, v := range story.Comments_ids {
			CommentsIds[i] = int64(v)
		}
		_, err := stmt.ExecContext(ctx, story.ID, story.Type, story.Title, story.URL,
			story.Score, story.Author, story.Created_At, CommentsIds, story.Comments_count)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteByAuthor deletes all stories by author
func (r *StoryRepository) DeleteByAuthor(ctx context.Context, author string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM stories WHERE author = $1`, author)
	return err
}

// Exists checks if story exists
func (r *StoryRepository) Exists(ctx context.Context, id int) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM stories WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

// GetCount returns total count of stories
func (r *StoryRepository) GetCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM stories`).Scan(&count)
	return count, err
}

// Helper function to scan stories
func scanStories(rows *sql.Rows) ([]*models.Story, error) {
	var stories []*models.Story
	for rows.Next() {
		story := &models.Story{}
		var commentsIds pq.Int64Array

		err := rows.Scan(&story.ID, &story.Type, &story.Title, &story.URL,
			&story.Score, &story.Author, &story.Created_At, &commentsIds, &story.Comments_count)
		if err != nil {
			return nil, err
		}

		story.Comments_ids = make([]int, len(commentsIds))
		for i, v := range commentsIds {
			story.Comments_ids[i] = int(v)
		}

		stories = append(stories, story)
	}
	return stories, rows.Err()
}
