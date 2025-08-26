package repository

import (
	"context"
	"database/sql"

	"indexer-service/internal/models"
	"indexer-service/pkg/database"


	"github.com/lib/pq"
)

// UserRepository implements repository.UserRepository
type UserRepository struct {
	db *sql.DB
}

// NewUserRepository creates a new UserRepository instance
func NewUserRepository() UserRepository {
	return UserRepository{
		db: database.GetDB(),
	}
}

// Create inserts a new user
func (r *UserRepository) Create(ctx context.Context, user *models.User) error {
	submittedIds := make(pq.Int64Array, len(user.Submitted))
	for i, v := range user.Submitted {
		submittedIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO users (username, karma, about, created_at, submitted_ids) 
		 VALUES ($1, $2, $3, $4, $5)`,
		user.Username, user.Karma, user.About, user.Created_At, submittedIds)
	return err
}

// GetByIDString retrieves a user by username (keeping interface compatibility)
func (r *UserRepository) GetByIDString(ctx context.Context, username string) (*models.User, error) {
	user := &models.User{}
	var submittedIds pq.Int64Array

	err := r.db.QueryRowContext(ctx,
		`SELECT id, username, karma, about, created_at, submitted_ids 
		 FROM users WHERE username = $1`, username).Scan(
		&user.ID, &user.Username, &user.Karma, &user.About, &user.Created_At, &submittedIds)
	if err != nil {
		return nil, err
	}

	user.Submitted = make([]int, len(submittedIds))
	for i, v := range submittedIds {
		user.Submitted[i] = int(v)
	}

	return user, nil
}

// Update updates an existing user
func (r *UserRepository) Update(ctx context.Context, user *models.User) error {
	submittedIds := make(pq.Int64Array, len(user.Submitted))
	for i, v := range user.Submitted {
		submittedIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`UPDATE users SET username=$2, karma=$3, about=$4, created_at=$5, submitted_ids=$6 WHERE id=$1`,
		user.ID, user.Username, user.Karma, user.About, user.Created_At, submittedIds)
	return err
}

// Delete removes a user by username (keeping interface compatibility)
func (r *UserRepository) Delete(ctx context.Context, username string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM users WHERE username = $1`, username)
	return err
}

// GetAll retrieves all users
func (r *UserRepository) GetAll(ctx context.Context) ([]*models.User, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, username, karma, about, created_at, submitted_ids 
		 FROM users ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return r.scanUsers(rows)
}

// GetRecent retrieves recent users
func (r *UserRepository) GetRecent(ctx context.Context, limit int) ([]*models.User, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, username, karma, about, created_at, submitted_ids 
		 FROM users ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return r.scanUsers(rows)
}

// GetByMinKarma retrieves users with minimum karma
func (r *UserRepository) GetByMinKarma(ctx context.Context, minKarma int) ([]*models.User, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, username, karma, about, created_at, submitted_ids 
		 FROM users WHERE karma >= $1 ORDER BY karma DESC`, minKarma)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return r.scanUsers(rows)
}

// GetByDateRange retrieves users within date range
func (r *UserRepository) GetByDateRange(ctx context.Context, start, end int64) ([]*models.User, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, username, karma, about, created_at, submitted_ids 
		 FROM users WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at DESC`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return r.scanUsers(rows)
}

// GetTopByKarma retrieves top users by karma
func (r *UserRepository) GetTopByKarma(ctx context.Context, limit int) ([]*models.User, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, username, karma, about, created_at, submitted_ids 
		 FROM users ORDER BY karma DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return r.scanUsers(rows)
}

// GetByKarmaRange retrieves users within karma range
func (r *UserRepository) GetByKarmaRange(ctx context.Context, minKarma, maxKarma int) ([]*models.User, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, username, karma, about, created_at, submitted_ids 
		 FROM users WHERE karma BETWEEN $1 AND $2 ORDER BY karma DESC`, minKarma, maxKarma)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return r.scanUsers(rows)
}

// GetUsersWithSubmissions retrieves users with minimum submissions
func (r *UserRepository) GetUsersWithSubmissions(ctx context.Context, minSubmissions int) ([]*models.User, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, username, karma, about, created_at, submitted_ids 
		 FROM users WHERE COALESCE(array_length(submitted_ids, 1), 0) >= $1 
		 ORDER BY COALESCE(array_length(submitted_ids, 1), 0) DESC`, minSubmissions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return r.scanUsers(rows)
}

// UpdateKarma updates user karma
func (r *UserRepository) UpdateKarma(ctx context.Context, username string, karma int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE users SET karma = $1 WHERE username = $2`, karma, username)
	return err
}

// UpdateAbout updates user about field
func (r *UserRepository) UpdateAbout(ctx context.Context, username string, about string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE users SET about = $1 WHERE username = $2`, about, username)
	return err
}

// AddSubmission adds a submission ID to user's submitted_ids array
func (r *UserRepository) AddSubmission(ctx context.Context, username string, itemID int) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE users SET submitted_ids = array_append(submitted_ids, $1) WHERE username = $2`,
		itemID, username)
	return err
}

// RemoveSubmission removes a submission ID from user's submitted_ids array
func (r *UserRepository) RemoveSubmission(ctx context.Context, username string, itemID int) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE users SET submitted_ids = array_remove(submitted_ids, $1) WHERE username = $2`,
		itemID, username)
	return err
}

// CreateBatch creates multiple users
func (r *UserRepository) CreateBatch(ctx context.Context, users []*models.User) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO users (username, karma, about, created_at, submitted_ids) 
		 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (username) DO NOTHING`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, user := range users {
		submittedIds := make(pq.Int64Array, len(user.Submitted))
		for i, v := range user.Submitted {
			submittedIds[i] = int64(v)
		}

		_, err := stmt.ExecContext(ctx, user.Username, user.Karma, user.About, user.Created_At, submittedIds)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CreateBatchWithExistingIDs creates multiple users with existing usernames
func (r *UserRepository) CreateBatchWithExistingIDs(ctx context.Context, users []*models.User) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO users (username, karma, about, created_at, submitted_ids)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (username) DO UPDATE SET karma = EXCLUDED.karma, about = EXCLUDED.about, 
				created_at = EXCLUDED.created_at,submitted_ids = EXCLUDED.submitted_ids; `)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, user := range users {
		submittedIds := make(pq.Int64Array, len(user.Submitted))
		for i, v := range user.Submitted {
			submittedIds[i] = int64(v)
		}
		_, err := stmt.ExecContext(ctx, user.Username, user.Karma, user.About, user.Created_At, submittedIds)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UpdateKarmaBatch updates karma for multiple users
func (r *UserRepository) UpdateKarmaBatch(ctx context.Context, karmaUpdates map[int]int) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `UPDATE users SET karma = $1 WHERE id = $2`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for userID, karma := range karmaUpdates {
		_, err := stmt.ExecContext(ctx, karma, userID)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetSubmittedIDsByID retrieves submitted IDs for a user
func (r *UserRepository) GetSubmittedIDsByID(ctx context.Context, username string) ([]int, error) {
	var submittedIds pq.Int64Array
	err := r.db.QueryRowContext(ctx,
		`SELECT submitted_ids FROM users WHERE username = $1`, username).Scan(&submittedIds)
	if err != nil {
		return nil, err
	}

	result := make([]int, len(submittedIds))
	for i, v := range submittedIds {
		result[i] = int(v)
	}
	return result, nil
}

// GetSubmissionCount returns the count of submissions for a user
func (r *UserRepository) GetSubmissionCount(ctx context.Context, username string) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		`SELECT COALESCE(array_length(submitted_ids, 1), 0) FROM users WHERE username = $1`, username).Scan(&count)
	return count, err
}

// UserExists checks if user exists
func (r *UserRepository) UserExists(ctx context.Context, username string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)`, username).Scan(&exists)
	return exists, err
}

// GetUserIDByUsername returns user ID by username
func (r *UserRepository) GetUserIDByUsername(ctx context.Context, username string) (int, error) {
	var userID int
	err := r.db.QueryRowContext(ctx,
		`SELECT id FROM users WHERE username = $1`, username).Scan(&userID)
	return userID, err
}

// Exists checks if user exists by ID
func (r *UserRepository) Exists(ctx context.Context, id int) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

// GetCount returns total count of users
func (r *UserRepository) GetCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count)
	return count, err
}

// scanUsers scans rows into user slice
func (r *UserRepository) scanUsers(rows *sql.Rows) ([]*models.User, error) {
	var users []*models.User
	for rows.Next() {
		user := &models.User{}
		var submittedIds pq.Int64Array

		err := rows.Scan(&user.ID, &user.Username, &user.Karma, &user.About, &user.Created_At, &submittedIds)
		if err != nil {
			return nil, err
		}

		user.Submitted = make([]int, len(submittedIds))
		for i, v := range submittedIds {
			user.Submitted[i] = int(v)
		}

		users = append(users, user)
	}
	return users, rows.Err()
}
