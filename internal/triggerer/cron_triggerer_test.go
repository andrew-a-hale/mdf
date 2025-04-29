package triggerer

import (
	"testing"

	"github.com/andrew-a-hale/mdf/internal/parser"
)

func TestNewCronTriggerer(t *testing.T) {
	configs := parser.Configs{
		parser.Config{
			Id:         "a",
			Connectors: nil,
			DataSource: parser.DataSource{
				Name:    "",
				Domain:  "",
				Trigger: parser.TriggerConfig{Cron: "* * * * *", RandomOffset: false},
			},
		},
	}
	v := New(&configs)
	if v == nil {
		t.Fatal("New() returned nil")
	}
}
