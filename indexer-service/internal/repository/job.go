package repository

import (
	"context"
	"database/sql"

	"indexer-service/internal/models"
	"indexer-service/pkg/database"

)

// JobRepository implements repository.JobRepository
type JobRepository struct {
	db *sql.DB
}

// NewJobRepository creates a new JobRepository instance
func NewJobRepository() JobRepository {
	return JobRepository{
		db: database.GetDB(),
	}
}

// Create inserts a new job
func (r *JobRepository) Create(ctx context.Context, job *models.Job) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO jobs (id, type, title, text, url, score, author, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		job.ID, job.Type, job.Title, job.Text, job.URL,
		job.Score, job.Author, job.Created_At)
	return err
}

// GetByID retrieves a job by ID
func (r *JobRepository) GetByID(ctx context.Context, id int) (*models.Job, error) {
	job := &models.Job{}
	err := r.db.QueryRowContext(ctx,
		`SELECT id, type, title, text, url, score, author, created_at 
		 FROM jobs WHERE id = $1`, id).Scan(
		&job.ID, &job.Type, &job.Title, &job.Text, &job.URL,
		&job.Score, &job.Author, &job.Created_At)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// Update updates an existing job
func (r *JobRepository) Update(ctx context.Context, job *models.Job) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE jobs SET type=$2, title=$3, text=$4, url=$5, score=$6, author=$7, created_at=$8 
		 WHERE id=$1`,
		job.ID, job.Type, job.Title, job.Text, job.URL,
		job.Score, job.Author, job.Created_At)
	return err
}

// Delete removes a job by ID
func (r *JobRepository) Delete(ctx context.Context, id int) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = $1`, id)
	return err
}

// GetAll retrieves all jobs
func (r *JobRepository) GetAll(ctx context.Context) ([]*models.Job, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, url, score, author, created_at 
		 FROM jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// GetRecent retrieves recent jobs
func (r *JobRepository) GetRecent(ctx context.Context, limit int) ([]*models.Job, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, url, score, author, created_at 
		 FROM jobs ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// GetByMinScore retrieves jobs with minimum score
func (r *JobRepository) GetByMinScore(ctx context.Context, minScore int) ([]*models.Job, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, url, score, author, created_at 
		 FROM jobs WHERE score >= $1 ORDER BY score DESC`, minScore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// GetByAuthor retrieves jobs by author
func (r *JobRepository) GetByAuthor(ctx context.Context, author string) ([]*models.Job, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, url, score, author, created_at 
		 FROM jobs WHERE author = $1 ORDER BY created_at DESC`, author)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// GetByDateRange retrieves jobs within date range
func (r *JobRepository) GetByDateRange(ctx context.Context, start, end int64) ([]*models.Job, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, type, title, text, url, score, author, created_at 
		 FROM jobs WHERE created_at BETWEEN $1 AND $2 ORDER BY created_at DESC`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// UpdateScore updates job score
func (r *JobRepository) UpdateScore(ctx context.Context, id int, score int) error {
	_, err := r.db.ExecContext(ctx, `UPDATE jobs SET score = $1 WHERE id = $2`, score, id)
	return err
}

// CreateBatch creates multiple jobs
func (r *JobRepository) CreateBatch(ctx context.Context, jobs []*models.Job) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO jobs (id, type, title, text, url, score, author, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, job := range jobs {
		_, err := stmt.ExecContext(ctx, job.ID, job.Type, job.Title, job.Text,
			job.URL, job.Score, job.Author, job.Created_At)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CreateBatchWithExistingIDs creates multiple jobs with existing IDs
func (r *JobRepository) CreateBatchWithExistingIDs(ctx context.Context, jobs []*models.Job) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO jobs (id, type, title, text, url, score, author, created_at) 
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO UPDATE SET type = EXCLUDED.type,
		 title = EXCLUDED.title, text = EXCLUDED.text, url = EXCLUDED.url,	score = EXCLUDED.score,
		 author = EXCLUDED.author, created_at = EXCLUDED.created_at`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, job := range jobs {
		_, err := stmt.ExecContext(ctx, job.ID, job.Type, job.Title, job.Text,
			job.URL, job.Score, job.Author, job.Created_At)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteByAuthor deletes all jobs by author
func (r *JobRepository) DeleteByAuthor(ctx context.Context, author string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM jobs WHERE author = $1`, author)
	return err
}

// Exists checks if job exists
func (r *JobRepository) Exists(ctx context.Context, id int) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

// GetCount returns total count of jobs
func (r *JobRepository) GetCount(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&count)
	return count, err
}

// Helper function to scan jobs
func scanJobs(rows *sql.Rows) ([]*models.Job, error) {
	var jobs []*models.Job
	for rows.Next() {
		job := &models.Job{}
		err := rows.Scan(&job.ID, &job.Type, &job.Title, &job.Text,
			&job.URL, &job.Score, &job.Author, &job.Created_At)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}
