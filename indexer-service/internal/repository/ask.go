package repository

import (
	"context"
	"database/sql"

	"indexer-service/internal/models"

	"indexer-service/pkg/database"

	"github.com/lib/pq"
)

// AskRepository implements repository.AskRepository
type AskRepository struct {
	db *sql.DB
}

// NewAskRepository creates a new AskRepository instance
func NewAskRepository() AskRepository {
	return AskRepository{
		db: database.GetDB(),
	}
}

// Create inserts a new ask
func (r *AskRepository) Create(ctx context.Context, ask *models.Ask) error {
	replyIds := make(pq.Int64Array, len(ask.Reply_ids))
	for i, v := range ask.Reply_ids {
		replyIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO asks (id, type, title, text, score, author, reply_ids, replies_count, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		ask.ID, ask.Type, ask.Title, ask.Text, ask.Score,
		ask.Author, replyIds, ask.Replies_count, ask.Created_At)
	return err
}

// GetByID retrieves an ask by ID
func (r *AskRepository) GetByID(ctx context.Context, id int) (*models.Ask, error) {
	ask := &models.Ask{}
	var replyIds pq.Int64Array

	err := r.db.QueryRowContext(ctx,
		`SELECT id, type, title, text, score, author, reply_ids, replies_count, created_at 
		 FROM asks WHERE id = $1`, id).Scan(
		&ask.ID, &ask.Type, &ask.Title, &ask.Text, &ask.Score,
		&ask.Author, &replyIds, &ask.Replies_count, &ask.Created_At)
	if err != nil {
		return nil, err
	}

	ask.Reply_ids = make([]int, len(replyIds))
	for i, v := range replyIds {
		ask.Reply_ids[i] = int(v)
	}
	return ask, nil
}

// Update updates an existing ask
func (r *AskRepository) Update(ctx context.Context, ask *models.Ask) error {
	replyIds := make(pq.Int64Array, len(ask.Reply_ids))
	for i, v := range ask.Reply_ids {
		replyIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`UPDATE asks SET type=$2, title=$3, text=$4, score=$5, author=$6, 
		 reply_ids=$7, replies_count=$8, created_at=$9 WHERE id=$1`,
		ask.ID, ask.Type, ask.Title, ask.Text, ask.Score,
		ask.Author, replyIds, ask.Replies_count, ask.Created_At)
	return err
}

// Delete removes an ask by ID
func (r *AskRepository) Delete(ctx context.Context, id int) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM asks WHERE id = $1`, id)
	return err
}

// GetAll retrieves all asks
func (r *AskRepository) GetAll(ctx context.Context) ([]*models.Ask, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, score, author, reply_ids, replies_count, created_at 
		 FROM asks ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAsks(rows)
}

// GetRecent retrieves recent asks
func (r *AskRepository) GetRecent(ctx context.Context, limit int) ([]*models.Ask, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, score, author, reply_ids, replies_count, created_at 
		 FROM asks ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAsks(rows)
}

// GetByMinScore retrieves asks with minimum score
func (r *AskRepository) GetByMinScore(ctx context.Context, minScore int) ([]*models.Ask, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, score, author, reply_ids, replies_count, created_at 
		 FROM asks WHERE score >= $1 ORDER BY score DESC`, minScore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAsks(rows)
}

// GetByAuthor retrieves asks by author
func (r *AskRepository) GetByAuthor(ctx context.Context, author string) ([]*models.Ask, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, score, author, reply_ids, replies_count, created_at 
		 FROM asks WHERE author = $1 ORDER BY created_at DESC`, author)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAsks(rows)
}

// GetByDateRange retrieves asks within date range
func (r *AskRepository) GetByDateRange(ctx context.Context, start, end int64) ([]*models.Ask, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, score, author, reply_ids, replies_count, created_at 
		 FROM asks WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at DESC`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAsks(rows)
}

// UpdateScore updates ask score
func (r *AskRepository) UpdateScore(ctx context.Context, id int, score int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE asks SET score = $1 WHERE id = $2`, score, id)
	return err
}

// UpdateRepliesCount updates replies count
func (r *AskRepository) UpdateRepliesCount(ctx context.Context, id int, count int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE asks SET replies_count = $1 WHERE id = $2`, count, id)
	return err
}

// CreateBatch creates multiple asks
func (r *AskRepository) CreateBatch(ctx context.Context, asks []*models.Ask) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO asks (id, type, title, text, score, author, reply_ids, replies_count, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, ask := range asks {
		replyIds := make(pq.Int64Array, len(ask.Reply_ids))
		for i, v := range ask.Reply_ids {
			replyIds[i] = int64(v)
		}

		_, err := stmt.ExecContext(ctx, ask.ID, ask.Type, ask.Title, ask.Text,
			ask.Score, ask.Author, replyIds, ask.Replies_count, ask.Created_At)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CreateBatchWithExistingIDs creates multiple asks with existing IDs
func (r *AskRepository) CreateBatchWithExistingIDs(ctx context.Context, asks []*models.Ask) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO asks (id, type, title, text, score, author, reply_ids, replies_count, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO UPDATE SET
		 type = EXCLUDED.type, title = EXCLUDED.title, text = EXCLUDED.text, score = EXCLUDED.score, author = EXCLUDED.author,
		 reply_ids = EXCLUDED.reply_ids, replies_count = EXCLUDED.replies_count, created_at = EXCLUDED.created_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, ask := range asks {
		replyIds := make(pq.Int64Array, len(ask.Reply_ids))
		for i, v := range ask.Reply_ids {
			replyIds[i] = int64(v)
		}
		_, err := stmt.ExecContext(ctx, ask.ID, ask.Type, ask.Title, ask.Text,
			ask.Score, ask.Author, replyIds, ask.Replies_count, ask.Created_At)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteByAuthor deletes all asks by author
func (r *AskRepository) DeleteByAuthor(ctx context.Context, author string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM asks WHERE author = $1`, author)
	return err
}

// Exists checks if ask exists
func (r *AskRepository) Exists(ctx context.Context, id int) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asks WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

// GetCount returns total count of asks
func (r *AskRepository) GetCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM asks`).Scan(&count)
	return count, err
}

// Helper function to scan asks
func scanAsks(rows *sql.Rows) ([]*models.Ask, error) {
	var asks []*models.Ask
	for rows.Next() {
		ask := &models.Ask{}
		var replyIds pq.Int64Array

		err := rows.Scan(&ask.ID, &ask.Type, &ask.Title, &ask.Text, &ask.Score,
			&ask.Author, &replyIds, &ask.Replies_count, &ask.Created_At)
		if err != nil {
			return nil, err
		}

		ask.Reply_ids = make([]int, len(replyIds))
		for i, v := range replyIds {
			ask.Reply_ids[i] = int(v)
		}
		asks = append(asks, ask)
	}
	return asks, rows.Err()
}
