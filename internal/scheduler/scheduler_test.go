package scheduler

import (
	"testing"
)

func TestNewCronTriggerer(t *testing.T) {
	v := New()
	if v == nil {
		t.Fatal("New() returned nil")
	}
}
