package eventlog

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestLogEvent(t *testing.T) {
	// Create a buffer to capture log output
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	// Create event logger
	evLogger := New(logger)

	// Log an event
	evLogger.LogEvent("test_event", "Test event message", map[string]any{
		"domain": "test",
		"name":   "test_job",
	})

	// Verify log output
	logOutput := buf.String()

	// Check that the event type and message are in the log
	if !strings.Contains(logOutput, "test_event") {
		t.Errorf("Log output missing event type: %s", logOutput)
	}

	if !strings.Contains(logOutput, "Test event message") {
		t.Errorf("Log output missing event message: %s", logOutput)
	}

	// Check that the custom fields are in the log
	if !strings.Contains(logOutput, "test_job") {
		t.Errorf("Log output missing custom field: %s", logOutput)
	}
}

func TestLogStartEnd(t *testing.T) {
	// Create a buffer to capture log output
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	// Create event logger
	evLogger := New(logger)

	// Log start event
	evLogger.LogJobStart("test", "test_job")

	// Verify start event is logged
	startLogOutput := buf.String()
	if !strings.Contains(startLogOutput, "job_start") {
		t.Errorf("Log output missing job_start event: %s", startLogOutput)
	}

	// Reset buffer
	buf.Reset()

	// Log end event
	evLogger.LogJobEnd("test", "test_job", true, nil)

	// Verify end event is logged
	endLogOutput := buf.String()
	if !strings.Contains(endLogOutput, "job_end") {
		t.Errorf("Log output missing job_end event: %s", endLogOutput)
	}

	if strings.Contains(endLogOutput, "success\": true") {
		t.Errorf("Log output missing success status: %s", endLogOutput)
	}

	// Reset buffer
	buf.Reset()

	// Log end event with error
	err := fmt.Errorf("test error")
	evLogger.LogJobEnd("test", "test_job", false, err)

	// Verify error is logged
	errorLogOutput := buf.String()
	if strings.Contains(errorLogOutput, "success\": false") {
		t.Errorf("Log output missing failure status: %s", errorLogOutput)
	}

	if !strings.Contains(errorLogOutput, "test error") {
		t.Errorf("Log output missing error message: %s", errorLogOutput)
	}
}
