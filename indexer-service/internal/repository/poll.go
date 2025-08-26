package repository

import (
	"context"
	"database/sql"

	"indexer-service/internal/models"
	"indexer-service/pkg/database"


	"github.com/lib/pq"
)

// PollRepository implements repository.PollRepoitory
type PollRepository struct {
	db *sql.DB
}

// NewPollRepository creates a new PollRepository instance
func NewPollRepository() PollRepository {
	return PollRepository{
		db: database.GetDB(),
	}
}

// Create inserts a new poll
func (r *PollRepository) Create(ctx context.Context, poll *models.Poll) error {
	pollOptions := make(pq.Int64Array, len(poll.PollOptions))
	for i, v := range poll.PollOptions {
		pollOptions[i] = int64(v)
	}

	replyIds := make(pq.Int64Array, len(poll.Reply_Ids))
	for i, v := range poll.Reply_Ids {
		replyIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO polls (id, type, title, score, author, poll_options, reply_ids, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		poll.ID, poll.Type, poll.Title, poll.Score,
		poll.Author, pollOptions, replyIds, poll.Created_At)
	return err
}

// GetByID retrieves a poll by ID
func (r *PollRepository) GetByID(ctx context.Context, id int) (*models.Poll, error) {
	poll := &models.Poll{}
	var pollOptions pq.Int64Array
	var replyIds pq.Int64Array

	err := r.db.QueryRowContext(ctx,
		`SELECT id, type, title, score, author, poll_options, reply_ids, created_at 
		 FROM polls WHERE id = $1`, id).Scan(
		&poll.ID, &poll.Type, &poll.Title, &poll.Score,
		&poll.Author, &pollOptions, &replyIds, &poll.Created_At)
	if err != nil {
		return nil, err
	}

	poll.PollOptions = make([]int, len(pollOptions))
	for i, v := range pollOptions {
		poll.PollOptions[i] = int(v)
	}

	poll.Reply_Ids = make([]int, len(replyIds))
	for i, v := range replyIds {
		poll.Reply_Ids[i] = int(v)
	}
	return poll, nil
}

// Update updates an existing poll
func (r *PollRepository) Update(ctx context.Context, poll *models.Poll) error {
	pollOptions := make(pq.Int64Array, len(poll.PollOptions))
	for i, v := range poll.PollOptions {
		pollOptions[i] = int64(v)
	}

	replyIds := make(pq.Int64Array, len(poll.Reply_Ids))
	for i, v := range poll.Reply_Ids {
		replyIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`UPDATE polls SET type=$2, title=$3, score=$4, author=$5, 
		 poll_options=$6, reply_ids=$7, created_at=$8 WHERE id=$1`,
		poll.ID, poll.Type, poll.Title, poll.Score,
		poll.Author, pollOptions, replyIds, poll.Created_At)
	return err
}

// Delete removes a poll by ID
func (r *PollRepository) Delete(ctx context.Context, id int) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM polls WHERE id = $1`, id)
	return err
}

// GetAll retrieves all polls
func (r *PollRepository) GetAll(ctx context.Context) ([]*models.Poll, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, score, author, poll_options, reply_ids, created_at 
		 FROM polls ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPolls(rows)
}

// GetRecent retrieves recent polls
func (r *PollRepository) GetRecent(ctx context.Context, limit int) ([]*models.Poll, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, score, author, poll_options, reply_ids, created_at 
		 FROM polls ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPolls(rows)
}

// GetByMinScore retrieves polls with minimum score
func (r *PollRepository) GetByMinScore(ctx context.Context, minScore int) ([]*models.Poll, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, score, author, poll_options, reply_ids, created_at 
		 FROM polls WHERE score >= $1 ORDER BY score DESC`, minScore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPolls(rows)
}

// GetByAuthor retrieves polls by author
func (r *PollRepository) GetByAuthor(ctx context.Context, author string) ([]*models.Poll, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, score, author, poll_options, reply_ids, created_at 
		 FROM polls WHERE author = $1 ORDER BY created_at DESC`, author)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPolls(rows)
}

// GetByDateRange retrieves polls within date range
func (r *PollRepository) GetByDateRange(ctx context.Context, start, end int64) ([]*models.Poll, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, score, author, poll_options, reply_ids, created_at 
		 FROM polls WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at DESC`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPolls(rows)
}

// UpdateScore updates poll score
func (r *PollRepository) UpdateScore(ctx context.Context, id int, score int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE polls SET score = $1 WHERE id = $2`, score, id)
	return err
}

// CreateBatch creates multiple polls
func (r *PollRepository) CreateBatch(ctx context.Context, polls []*models.Poll) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO polls (id, type, title, score, author, poll_options, reply_ids, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, poll := range polls {
		pollOptions := make(pq.Int64Array, len(poll.PollOptions))
		for i, v := range poll.PollOptions {
			pollOptions[i] = int64(v)
		}

		replyIds := make(pq.Int64Array, len(poll.Reply_Ids))
		for i, v := range poll.Reply_Ids {
			replyIds[i] = int64(v)
		}

		_, err := stmt.ExecContext(ctx, poll.ID, poll.Type, poll.Title, poll.Score,
			poll.Author, pollOptions, replyIds, poll.Created_At)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CreateBatchWithExistingIDs creates multiple polls with existing IDs
func (r *PollRepository) CreateBatchWithExistingIDs(ctx context.Context, polls []*models.Poll) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO polls (id, type, title, score, author, poll_options, reply_ids, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO UPDATE SET
		 type = EXCLUDED.type, title = EXCLUDED.title, score = EXCLUDED.score, author = EXCLUDED.author,
		 poll_options = EXCLUDED.poll_options, reply_ids = EXCLUDED.reply_ids, created_at = EXCLUDED.created_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, poll := range polls {
		pollOptions := make(pq.Int64Array, len(poll.PollOptions))
		for i, v := range poll.PollOptions {
			pollOptions[i] = int64(v)
		}

		replyIds := make(pq.Int64Array, len(poll.Reply_Ids))
		for i, v := range poll.Reply_Ids {
			replyIds[i] = int64(v)
		}
		_, err := stmt.ExecContext(ctx, poll.ID, poll.Type, poll.Title, poll.Score,
			poll.Author, pollOptions, replyIds, poll.Created_At)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteByAuthor deletes all polls by author
func (r *PollRepository) DeleteByAuthor(ctx context.Context, author string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM polls WHERE author = $1`, author)
	return err
}

// Exists checks if poll exists
func (r *PollRepository) Exists(ctx context.Context, id int) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM polls WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

// GetCount returns total count of polls
func (r *PollRepository) GetCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM polls`).Scan(&count)
	return count, err
}

// Helper function to scan polls
func scanPolls(rows *sql.Rows) ([]*models.Poll, error) {
	var polls []*models.Poll
	for rows.Next() {
		poll := &models.Poll{}
		var pollOptions pq.Int64Array
		var replyIds pq.Int64Array

		err := rows.Scan(&poll.ID, &poll.Type, &poll.Title, &poll.Score,
			&poll.Author, &pollOptions, &replyIds, &poll.Created_At)
		if err != nil {
			return nil, err
		}

		poll.PollOptions = make([]int, len(pollOptions))
		for i, v := range pollOptions {
			poll.PollOptions[i] = int(v)
		}

		poll.Reply_Ids = make([]int, len(replyIds))
		for i, v := range replyIds {
			poll.Reply_Ids[i] = int(v)
		}
		polls = append(polls, poll)
	}
	return polls, rows.Err()
}
