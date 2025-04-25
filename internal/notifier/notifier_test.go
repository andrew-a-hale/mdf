package notifier

import (
	"testing"
)

func TestNew(t *testing.T) {
	// Test notifier creation
	notifier := New()
	if notifier == nil {
		t.Fatal("New() returned nil")
	}
}

func TestSendNotification(t *testing.T) {
	// Create notifier
	notifier := New()

	// Test sending notification
	err := notifier.SendNotification("test", "test_job", "Job completed", true)
	if err != nil {
		t.Errorf("SendNotification() error = %v, want nil", err)
	}

	// Test sending failure notification
	err = notifier.SendNotification("test", "test_job", "Job failed", false)
	if err != nil {
		t.Errorf("SendNotification() error = %v, want nil", err)
	}
}
