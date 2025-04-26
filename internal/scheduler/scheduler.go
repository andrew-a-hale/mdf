package scheduler

import (
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/andrew-a-hale/mdf/internal/executor"
	"github.com/andrew-a-hale/mdf/internal/parser"
	"github.com/robfig/cron/v3"
)

// Scheduler handles the scheduling of data ingestion jobs
type Scheduler struct {
	configs *parser.Configs
	cron    *cron.Cron
}

// New creates a new scheduler instance
func New(configs *parser.Configs) *Scheduler {
	return &Scheduler{
		configs: configs,
		cron:    cron.New(),
	}
}

// Start starts the scheduler
func (s *Scheduler) Start() error {
	// Schedule each data source
	for _, conf := range *s.configs {
		err := s.schedule(conf)
		if err != nil {
			return fmt.Errorf("failed to schedule data source %s.%s: %w", conf.DataSource.Domain, conf.DataSource.Name, err)
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
func (s *Scheduler) schedule(conf parser.Config) error {
	// Log the scheduling event
	slog.Info("Scheduling data source",
		"domain", conf.DataSource.Domain,
		"name", conf.DataSource.Name,
		"cron", conf.DataSource.Schedule.Cron,
		"random_offset", conf.DataSource.Schedule.RandomOffset)

	// Create the job function
	jobFunc := func() {
		// Apply random offset if configured
		if conf.DataSource.Schedule.RandomOffset {
			offset := time.Duration(rand.Intn(60)) * time.Second
			slog.Info("Applying random offset",
				"domain", conf.DataSource.Domain,
				"name", conf.DataSource.Name,
				"offset_seconds", offset.Seconds())
			time.Sleep(offset)
		}

		// Create and run the executor
		exec := executor.New(conf)
		err := exec.Execute()
		if err != nil {
			slog.Error("Job execution failed",
				"domain", conf.DataSource.Domain,
				"name", conf.DataSource.Name,
				"error", err)
		} else {
			slog.Info("Job execution completed",
				"domain", conf.DataSource.Domain,
				"name", conf.DataSource.Name)
		}
	}

	// Add the job to the cron scheduler
	_, err := s.cron.AddFunc(conf.DataSource.Schedule.Cron, jobFunc)
	if err != nil {
		return fmt.Errorf("invalid cron expression '%s': %w", conf.DataSource.Schedule.Cron, err)
	}

	return nil
}
