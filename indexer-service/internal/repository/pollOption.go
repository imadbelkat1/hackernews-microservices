package repository

import (
	"context"
	"database/sql"
	"fmt"

	"indexer-service/internal/models"
	"indexer-service/pkg/database"

)

// PollOptionRepository implements repository.PollOptionRepository
type PollOptionRepository struct {
	db *sql.DB
}

// NewPollOptionRepository creates a new PollOptionRepository instance
func NewPollOptionRepository() PollOptionRepository {
	return PollOptionRepository{
		db: database.GetDB(),
	}
}

// CRUD Operations

// Create inserts a new poll option
func (r *PollOptionRepository) Create(ctx context.Context, pollOption *models.PollOption) error {
	if !pollOption.IsValid() {
		return fmt.Errorf("invalid poll option data")
	}

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO poll_options (id, type, poll_id, author, option_text, created_at, votes) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		pollOption.ID, pollOption.Type, pollOption.PollID, pollOption.Author,
		pollOption.OptionText, pollOption.CreatedAt, pollOption.Votes)
	return err
}

// GetByID retrieves a poll option by ID
func (r *PollOptionRepository) GetByID(ctx context.Context, id int) (*models.PollOption, error) {
	pollOption := &models.PollOption{}
	err := r.db.QueryRowContext(ctx,
		`SELECT id, type, poll_id, author, option_text, created_at, votes 
		 FROM poll_options WHERE id = $1`, id).Scan(
		&pollOption.ID, &pollOption.Type, &pollOption.PollID,
		&pollOption.Author, &pollOption.OptionText, &pollOption.CreatedAt, &pollOption.Votes)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("poll option not found with id: %d", id)
	}
	return pollOption, err
}

// Update updates an existing poll option
func (r *PollOptionRepository) Update(ctx context.Context, pollOption *models.PollOption) error {
	if !pollOption.IsValid() {
		return fmt.Errorf("invalid poll option data")
	}

	result, err := r.db.ExecContext(ctx,
		`UPDATE poll_options SET type=$2, poll_id=$3, author=$4, option_text=$5, 
		 created_at=$6, votes=$7 WHERE id=$1`,
		pollOption.ID, pollOption.Type, pollOption.PollID, pollOption.Author,
		pollOption.OptionText, pollOption.CreatedAt, pollOption.Votes)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return fmt.Errorf("poll option not found with id: %d", pollOption.ID)
	}

	return nil
}

// Delete removes a poll option by ID
func (r *PollOptionRepository) Delete(ctx context.Context, id int) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM poll_options WHERE id = $1`, id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return fmt.Errorf("poll option not found with id: %d", id)
	}

	return nil
}

// Query Operations

// GetAll retrieves all poll options
func (r *PollOptionRepository) GetAll(ctx context.Context) ([]*models.PollOption, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, poll_id, author, option_text, created_at, votes 
		 FROM poll_options ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPollOptions(rows)
}

// GetByPollID retrieves all poll options for a specific poll
func (r *PollOptionRepository) GetByPollID(ctx context.Context, pollID int) ([]*models.PollOption, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, poll_id, author, option_text, created_at, votes 
		 FROM poll_options WHERE poll_id = $1 ORDER BY created_at ASC`, pollID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPollOptions(rows)
}

// GetRecent retrieves recent poll options
func (r *PollOptionRepository) GetRecent(ctx context.Context, limit int) ([]*models.PollOption, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, poll_id, author, option_text, created_at, votes 
		 FROM poll_options ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPollOptions(rows)
}

// GetByAuthor retrieves poll options by author
func (r *PollOptionRepository) GetByAuthor(ctx context.Context, author string) ([]*models.PollOption, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, poll_id, author, option_text, created_at, votes 
		 FROM poll_options WHERE author = $1 ORDER BY created_at DESC`, author)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPollOptions(rows)
}

// GetByDateRange retrieves poll options within date range
func (r *PollOptionRepository) GetByDateRange(ctx context.Context, start, end int64) ([]*models.PollOption, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, poll_id, author, option_text, created_at, votes 
		 FROM poll_options WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at DESC`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPollOptions(rows)
}

// Update specific fields

// UpdateVotes updates the vote count for a poll option
func (r *PollOptionRepository) UpdateVotes(ctx context.Context, id int, votes int) error {
	if votes < 0 {
		return fmt.Errorf("votes cannot be negative")
	}

	result, err := r.db.ExecContext(ctx, `UPDATE poll_options SET votes = $1 WHERE id = $2`, votes, id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return fmt.Errorf("poll option not found with id: %d", id)
	}

	return nil
}

// Batch operations

// CreateBatch creates multiple poll options
func (r *PollOptionRepository) CreateBatch(ctx context.Context, pollOptions []*models.PollOption) error {
	if len(pollOptions) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO poll_options (id, type, poll_id, author, option_text, created_at, votes) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, pollOption := range pollOptions {
		if !pollOption.IsValid() {
			return fmt.Errorf("invalid poll option data in batch")
		}

		_, err := stmt.ExecContext(ctx,
			pollOption.ID, pollOption.Type, pollOption.PollID, pollOption.Author,
			pollOption.OptionText, pollOption.CreatedAt, pollOption.Votes)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// CreateBatchWithExistingIDs creates multiple poll options with existing IDs
func (r *PollOptionRepository) CreateBatchWithExistingIDs(ctx context.Context, pollOptions []*models.PollOption) error {
	if len(pollOptions) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO poll_options (id, type, poll_id, author, option_text, created_at, votes)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO UPDATE SET
		 type = EXCLUDED.type, poll_id = EXCLUDED.poll_id, author = EXCLUDED.author,
		 option_text = EXCLUDED.option_text, created_at = EXCLUDED.created_at, votes = EXCLUDED.votes`)
	if err != nil {
		return err
	}

	defer stmt.Close()
	for _, pollOption := range pollOptions {
		if !pollOption.IsValid() {
			return fmt.Errorf("invalid poll option data in batch")
		}
		_, err := stmt.ExecContext(ctx,
			pollOption.ID, pollOption.Type, pollOption.PollID, pollOption.Author,
			pollOption.OptionText, pollOption.CreatedAt, pollOption.Votes)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteByAuthor deletes all poll options by author
func (r *PollOptionRepository) DeleteByAuthor(ctx context.Context, author string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM poll_options WHERE author = $1`, author)
	return err
}

// DeleteByPollID deletes all poll options for a specific poll
func (r *PollOptionRepository) DeleteByPollID(ctx context.Context, pollID int) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM poll_options WHERE poll_id = $1`, pollID)
	return err
}

// Additional helper methods

// GetVoteCount retrieves the current vote count
func (r *PollOptionRepository) GetVoteCount(ctx context.Context, id int) (int, error) {
	var votes int
	err := r.db.QueryRowContext(ctx,
		`SELECT votes FROM poll_options WHERE id = $1`, id).Scan(&votes)

	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("poll option not found with id: %d", id)
	}
	return votes, err
}

// Exists checks if poll option exists
func (r *PollOptionRepository) Exists(ctx context.Context, id int) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM poll_options WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

// CountByPollID returns the number of options for a poll
func (r *PollOptionRepository) CountByPollID(ctx context.Context, pollID int) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM poll_options WHERE poll_id = $1`, pollID).Scan(&count)
	return count, err
}

// GetTopVoted retrieves the top voted options for a poll
func (r *PollOptionRepository) GetTopVoted(ctx context.Context, pollID int, limit int) ([]*models.PollOption, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, poll_id, author, option_text, created_at, votes 
		 FROM poll_options WHERE poll_id = $1 ORDER BY votes DESC LIMIT $2`, pollID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPollOptions(rows)
}

func (r *PollOptionRepository) GetCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM polls`).Scan(&count)
	return count, err
}

// Helper function to scan poll options
func scanPollOptions(rows *sql.Rows) ([]*models.PollOption, error) {
	var pollOptions []*models.PollOption
	for rows.Next() {
		pollOption := &models.PollOption{}
		err := rows.Scan(
			&pollOption.ID, &pollOption.Type, &pollOption.PollID,
			&pollOption.Author, &pollOption.OptionText, &pollOption.CreatedAt, &pollOption.Votes)
		if err != nil {
			return nil, err
		}
		pollOptions = append(pollOptions, pollOption)
	}
	return pollOptions, rows.Err()
}
