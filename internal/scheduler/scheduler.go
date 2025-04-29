package scheduler

import (
	"log"

	rabbitmq "github.com/wagslane/go-rabbitmq"
)

type Scheduler interface {
	Start() error
	Stop() error
}

type rmqScheduler struct {
	Client *rabbitmq.Conn
}

func New() *rmqScheduler {
	conn, err := rabbitmq.NewConn("amqp://guest:guest@localhost:5672/", rabbitmq.WithConnectionOptionsLogging)
	if err != nil {
		log.Fatal(err)
	}
	return &rmqScheduler{conn}
}

func (s *rmqScheduler) Start() error { return nil }
func (s *rmqScheduler) Stop() error  { return nil }
