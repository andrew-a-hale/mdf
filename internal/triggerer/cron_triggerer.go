package triggerer

import (
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/andrew-a-hale/mdf/internal/parser"
	"github.com/robfig/cron/v3"
)

// CronTriggerer handles the triggering of data ingestion jobs
type CronTriggerer struct {
	configs *parser.Configs
	cron    *cron.Cron
}

// New creates a new triggerer instance
func New(configs *parser.Configs) *CronTriggerer {
	return &CronTriggerer{
		configs: configs,
		cron:    cron.New(),
	}
}

// Start starts the triggerer
func (c *CronTriggerer) Start() error {
	for _, conf := range *c.configs {
		if conf.DataSource.Trigger.Cron == "" {
			continue
		}
		err := c.trigger(conf)
		if err != nil {
			return fmt.Errorf("failed to trigger data source %s.%s: %w", conf.DataSource.Domain, conf.DataSource.Name, err)
		}
	}

	// Log total number of jobs with cron triggerers
	entries := c.cron.Entries()
	slog.Info("Triggerer starting", "jobs_count", len(entries))

	// Start the cron triggerer
	c.cron.Start()
	return nil
}

// Stop stops the triggerer
func (c *CronTriggerer) Stop() {
	c.cron.Stop()
}

func (c *CronTriggerer) Post(configId string) error                   { return nil }
func (c *CronTriggerer) RegisterQueue(config map[string]string) error { return nil }
func (c *CronTriggerer) DeregisterQueue(queueId string) error         { return nil }

// trigger triggers a single data source
func (c *CronTriggerer) trigger(conf parser.Config) error {
	// Log the scheduling event
	slog.Info("triggering data source",
		"domain", conf.DataSource.Domain,
		"name", conf.DataSource.Name,
		"cron", conf.DataSource.Trigger.Cron,
		"random_offset", conf.DataSource.Trigger.RandomOffset)

	jobFunc := func() {
		// Apply random offset if configured
		if conf.DataSource.Trigger.RandomOffset {
			offset := time.Duration(rand.Intn(60)) * time.Second
			slog.Info("Applying random offset",
				"domain", conf.DataSource.Domain,
				"name", conf.DataSource.Name,
				"offset_seconds", offset.Seconds())
			time.Sleep(offset)
		}

		err := c.Post(conf.Id)
		if err != nil {
			slog.Error("Job failed posted to queue",
				"domain", conf.DataSource.Domain,
				"name", conf.DataSource.Name,
				"error", err)
		} else {
			slog.Info("Job posted to queue",
				"domain", conf.DataSource.Domain,
				"name", conf.DataSource.Name)
		}
	}

	// Add the job to the cron scheduler
	_, err := c.cron.AddFunc(conf.DataSource.Trigger.Cron, jobFunc)
	if err != nil {
		return fmt.Errorf("invalid cron expression '%s': %w", conf.DataSource.Trigger.Cron, err)
	}

	return nil
}
