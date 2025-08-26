package cronjob

import "context"

type CronJobInterface interface {
	// Start the cron scheduler
	Start(ctx context.Context) error

	// Stop the cron scheduler
	Stop() error

	// Add a new cron job
	AddJob(name string, schedule string, task func()) error

	// Remove a cron job
	RemoveJob(name string) error

	// List all job names
	ListJobs() []string
}

