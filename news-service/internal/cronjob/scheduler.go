package cronjob

import (
	"context"
	"fmt"
	"sync"

	"github.com/robfig/cron/v3"
)

type CronJobManager struct {
	cron    *cron.Cron
	jobs    map[string]cron.EntryID
	mutex   sync.RWMutex
	running bool
}

func NewCronJobManager() CronJobInterface {
	return &CronJobManager{
		cron: cron.New(),
		jobs: make(map[string]cron.EntryID),
	}
}

// Start begins the cron job scheduler
func (c *CronJobManager) Start(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return fmt.Errorf("scheduler already running")
	}

	c.cron.Start()
	c.running = true
	return nil
}

// Stop gracefully shuts down the cron job scheduler
func (c *CronJobManager) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return fmt.Errorf("scheduler not running")
	}

	c.cron.Stop()
	c.running = false
	return nil
}

// AddJob adds a new cron job
func (c *CronJobManager) AddJob(name string, schedule string, task func()) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.jobs[name]; exists {
		return fmt.Errorf("job '%s' already exists", name)
	}

	entryID, err := c.cron.AddFunc(schedule, task)
	if err != nil {
		return fmt.Errorf("failed to add job '%s': %w", name, err)
	}

	c.jobs[name] = entryID
	return nil
}

// RemoveJob removes a cron job by name
func (c *CronJobManager) RemoveJob(name string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	entryID, exists := c.jobs[name]
	if !exists {
		return fmt.Errorf("job '%s' does not exist", name)
	}

	c.cron.Remove(entryID)
	delete(c.jobs, name)
	return nil
}

// ListJobs returns all job names
func (c *CronJobManager) ListJobs() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	names := make([]string, 0, len(c.jobs))
	for name := range c.jobs {
		names = append(names, name)
	}
	return names
}

