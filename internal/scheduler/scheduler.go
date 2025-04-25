package scheduler

import (
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/andy/mdf/internal/executor"
	"github.com/andy/mdf/internal/parser"
	"github.com/robfig/cron/v3"
)

// Scheduler handles the scheduling of data ingestion jobs
type Scheduler struct {
	config *parser.Config
	cron   *cron.Cron
}

// New creates a new scheduler instance
func New(config *parser.Config) *Scheduler {
	return &Scheduler{
		config: config,
		cron:   cron.New(),
	}
}

// Start starts the scheduler
func (s *Scheduler) Start() error {
	// Schedule each data source
	for _, ds := range s.config.DataSources {
		err := s.scheduleDataSource(ds)
		if err != nil {
			return fmt.Errorf("failed to schedule data source %s.%s: %w", ds.Domain, ds.Name, err)
		}
	}

	// Log total number of scheduled jobs
	entries := s.cron.Entries()
	slog.Info("Scheduler starting", "jobs_count", len(entries))

	// Start the cron scheduler
	s.cron.Start()
	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.cron.Stop()
}

// scheduleDataSource schedules a single data source
func (s *Scheduler) scheduleDataSource(ds parser.DataSource) error {
	// Log the scheduling event
	slog.Info("Scheduling data source",
		"domain", ds.Domain,
		"name", ds.Name,
		"cron", ds.Schedule.Cron,
		"random_offset", ds.Schedule.RandomOffset)

	// Create the job function
	jobFunc := func() {
		// Apply random offset if configured
		if ds.Schedule.RandomOffset {
			offset := time.Duration(rand.Intn(60)) * time.Second
			slog.Info("Applying random offset",
				"domain", ds.Domain,
				"name", ds.Name,
				"offset_seconds", offset.Seconds())
			time.Sleep(offset)
		}

		// Create and run the executor
		exec := executor.New(ds, s.config.InitializedConnectors)
		err := exec.Execute()
		if err != nil {
			slog.Error("Job execution failed",
				"domain", ds.Domain,
				"name", ds.Name,
				"error", err)
		} else {
			slog.Info("Job execution completed",
				"domain", ds.Domain,
				"name", ds.Name)
		}
	}

	// Add the job to the cron scheduler
	_, err := s.cron.AddFunc(ds.Schedule.Cron, jobFunc)
	if err != nil {
		return fmt.Errorf("invalid cron expression '%s': %w", ds.Schedule.Cron, err)
	}

	return nil
}
