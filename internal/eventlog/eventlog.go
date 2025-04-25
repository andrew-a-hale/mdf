package eventlog

import (
	"log/slog"
	"time"
)

// EventLogger provides a unified event logging mechanism
type EventLogger struct {
	logger *slog.Logger
}

// New creates a new event logger
func New(logger *slog.Logger) *EventLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &EventLogger{
		logger: logger,
	}
}

// LogEvent logs a general event
func (e *EventLogger) LogEvent(eventType string, message string, attrs map[string]any) {
	// Convert attrs to slog.Attr
	var slogAttrs []any
	for k, v := range attrs {
		slogAttrs = append(slogAttrs, k, v)
	}

	// Add event type and timestamp
	slogAttrs = append(slogAttrs,
		"event_type", eventType,
		"timestamp", time.Now().Format(time.RFC3339),
	)

	// Log the event
	e.logger.Info(message, slogAttrs...)
}

// Log logs an event (for backward compatibility)
func (e *EventLogger) Log(component, message string, attrs ...any) {
	logAttrs := []any{"component", component, "timestamp", time.Now()}
	logAttrs = append(logAttrs, attrs...)
	
	e.logger.Info(message, logAttrs...)
}

// LogError logs an error event
func (e *EventLogger) LogError(component, message string, err error, attrs ...any) {
	logAttrs := []any{"component", component, "timestamp", time.Now(), "error", err}
	logAttrs = append(logAttrs, attrs...)
	
	e.logger.Error(message, logAttrs...)
}

// LogJobStart logs a job start event
func (e *EventLogger) LogJobStart(domain string, name string) {
	e.LogEvent("job_start", "Job started", map[string]any{
		"domain": domain,
		"name":   name,
		"job_id": domain + "-" + name + "-" + time.Now().Format("20060102150405"),
	})
}

// LogJobEnd logs a job end event
func (e *EventLogger) LogJobEnd(domain string, name string, success bool, err error) {
	attrs := map[string]any{
		"domain":  domain,
		"name":    name,
		"success": success,
		"job_id":  domain + "-" + name + "-" + time.Now().Format("20060102150405"),
	}

	if err != nil {
		attrs["error"] = err.Error()
	}

	message := "Job completed"
	if !success {
		message = "Job failed"
	}

	e.LogEvent("job_end", message, attrs)
}