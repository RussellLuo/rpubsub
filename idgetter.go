package rpubsub

// IDGetter is a manager that returns the last message ID for each topic.
type IDGetter interface {
	Get(topic string) string
}

// ZeroIDGetter is a manager that returns a zero ID for any topic.
type ZeroIDGetter struct{}

func (g *ZeroIDGetter) Get(topic string) string {
	return "0-0"
}
