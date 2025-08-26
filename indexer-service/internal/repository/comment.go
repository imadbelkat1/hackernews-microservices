package repository

import (
	"context"
	"database/sql"

	"indexer-service/internal/models"
	"indexer-service/pkg/database"

	"github.com/lib/pq"
)

// CommentRepository implements repository.CommentRepository
type CommentRepository struct {
	db *sql.DB
}

// NewCommentRepository creates a new CommentRepository instance
func NewCommentRepository() CommentRepository {
	return CommentRepository{
		db: database.GetDB(),
	}
}

// Create inserts a new comment
func (r *CommentRepository) Create(ctx context.Context, comment *models.Comment) error {
	replyIds := make(pq.Int64Array, len(comment.Replies))
	for i, v := range comment.Replies {
		replyIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO comments (id, type, text, author, created_at, parent_id, reply_ids) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		comment.ID, comment.Type, comment.Text,
		comment.Author, comment.Created_At, comment.Parent, replyIds)
	return err
}

// CreateBatch inserts multiple comments
func (r *CommentRepository) CreateBatchWithExistingIDs(ctx context.Context, comments []*models.Comment) error {
	if len(comments) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO comments (id, type, text, author, created_at, parent_id, reply_ids) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO UPDATE SET
		 type = EXCLUDED.type, text = EXCLUDED.text, author = EXCLUDED.author, created_at = EXCLUDED.created_at, 
		 parent_id = EXCLUDED.parent_id, reply_ids = EXCLUDED.reply_ids`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, comment := range comments {
		replyIds := make(pq.Int64Array, len(comment.Replies))
		for i, v := range comment.Replies {
			replyIds[i] = int64(v)
		}

		if _, err := stmt.ExecContext(ctx,
			comment.ID, comment.Type, comment.Text,
			comment.Author, comment.Created_At, comment.Parent, replyIds); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetByID retrieves a comment by ID
func (r *CommentRepository) GetByID(ctx context.Context, id int) (*models.Comment, error) {
	comment := &models.Comment{}
	var replyIds pq.Int64Array

	err := r.db.QueryRowContext(ctx,
		`SELECT id, type, text, author, created_at, parent_id, reply_ids 
		 FROM comments WHERE id = $1`, id).Scan(
		&comment.ID, &comment.Type, &comment.Text,
		&comment.Author, &comment.Created_At, &comment.Parent, &replyIds)
	if err != nil {
		return nil, err
	}

	comment.Replies = make([]int, len(replyIds))
	for i, v := range replyIds {
		comment.Replies[i] = int(v)
	}
	return comment, nil
}

// Update updates an existing comment
func (r *CommentRepository) Update(ctx context.Context, comment *models.Comment) error {
	replyIds := make(pq.Int64Array, len(comment.Replies))
	for i, v := range comment.Replies {
		replyIds[i] = int64(v)
	}

	_, err := r.db.ExecContext(ctx,
		`UPDATE comments SET  type=$2, text=$3, author=$4, 
		 created_at=$5, parent_id=$6, reply_ids=$7 WHERE id=$1`,
		comment.ID, comment.Type, comment.Text,
		comment.Author, comment.Created_At, comment.Parent, replyIds)
	return err
}

// Delete removes a comment by ID
func (r *CommentRepository) Delete(ctx context.Context, id int) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM comments WHERE id = $1`, id)
	return err
}

// GetAll retrieves all comments
func (r *CommentRepository) GetAll(ctx context.Context) ([]*models.Comment, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, text, author, created_at, parent_id, reply_ids 
		 FROM comments ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanComments(rows)
}

// GetRecent retrieves recent comments
func (r *CommentRepository) GetRecent(ctx context.Context, limit int) ([]*models.Comment, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, text, author, created_at, parent_id, reply_ids 
		 FROM comments ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanComments(rows)
}

// GetByAuthor retrieves comments by author
func (r *CommentRepository) GetByAuthor(ctx context.Context, author string) ([]*models.Comment, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, text, author, created_at, parent_id, reply_ids 
		 FROM comments WHERE author = $1 ORDER BY created_at DESC`, author)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanComments(rows)
}

// GetByDateRange retrieves comments within date range
func (r *CommentRepository) GetByDateRange(ctx context.Context, start, end int64) ([]*models.Comment, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, text, author, created_at, parent_id, reply_ids 
		 FROM comments WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at DESC`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanComments(rows)
}

// DeleteByAuthor deletes all comments by author
func (r *CommentRepository) DeleteByAuthor(ctx context.Context, author string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM comments WHERE author = $1`, author)
	return err
}

// Exists checks if comment exists
func (r *CommentRepository) Exists(ctx context.Context, id int) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM comments WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

// GetCount returns total count of comments
func (r *CommentRepository) GetCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM comments`).Scan(&count)
	return count, err
}

// Helper function to scan comments
func scanComments(rows *sql.Rows) ([]*models.Comment, error) {
	var comments []*models.Comment
	for rows.Next() {
		comment := &models.Comment{}
		var replyIds pq.Int64Array

		err := rows.Scan(&comment.ID, &comment.Type, &comment.Text,
			&comment.Author, &comment.Created_At, &comment.Parent, &replyIds)
		if err != nil {
			return nil, err
		}

		comment.Replies = make([]int, len(replyIds))
		for i, v := range replyIds {
			comment.Replies[i] = int(v)
		}
		comments = append(comments, comment)
	}
	return comments, rows.Err()
}
