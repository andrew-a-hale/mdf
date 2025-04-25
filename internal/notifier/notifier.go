package notifier

import (
	"log/slog"
	"time"
)

// Notifier handles notifications about job status
type Notifier struct{}

// New creates a new notifier instance
func New() *Notifier {
	return &Notifier{}
}

// SendNotification sends a notification about a job
func (n *Notifier) SendNotification(domain, name, message string, success bool) error {
	// Log the notification
	if success {
		slog.Info("Job notification",
			"domain", domain,
			"name", name,
			"message", message,
			"success", true,
			"timestamp", time.Now().Format(time.RFC3339))
	} else {
		slog.Error("Job notification",
			"domain", domain,
			"name", name,
			"message", message,
			"success", false,
			"timestamp", time.Now().Format(time.RFC3339))
	}

	// TODO:
	// In a real implementation, we would send notifications via email, Slack, etc.
	// For now, just return success
	return nil
}

// NotifySuccess sends a success notification (for backward compatibility)
func (n *Notifier) NotifySuccess(dataSource, message string) {
	// Extract domain and name from dataSource (assumes format "domain.name")
	domain := dataSource
	name := dataSource

	n.SendNotification(domain, name, message, true)
}

// NotifyFailure sends a failure notification (for backward compatibility)
func (n *Notifier) NotifyFailure(dataSource, message string, err error) {
	// Extract domain and name from dataSource (assumes format "domain.name")
	domain := dataSource
	name := dataSource

	// Append error to message
	fullMessage := message
	if err != nil {
		fullMessage += ": " + err.Error()
	}

	n.SendNotification(domain, name, fullMessage, false)
}
