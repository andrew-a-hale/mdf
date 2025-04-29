package triggerer

type Triggerer interface {
	Start() error
	Stop() error
	Post(configId string) error
	RegisterQueue(config map[string]string) error
	DeregisterQueue(queueId string) error
}
