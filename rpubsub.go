package rpubsub

import (
	"strings"
	"sync"

	"github.com/go-redis/redis"
)

type Message struct {
	ID     string
	Values map[string]interface{}
}

type Stream struct {
	Topic    string
	Messages []Message
}

type PubOpts struct {
	// host:port address of the Redis server.
	Addr string

	// Database to be selected after connecting to the Redis server.
	DB int

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int
}

type PubArgs struct {
	// The topic to which messages will be published.
	Topic string

	// Maximum number of messages each topic (i.e. Redis stream) can holds.
	// Zero value means no limit.
	//
	// Note that we use `MAXLEN ~` for performance,
	// see https://redis.io/commands/xadd#capped-streams.
	MaxLen int64

	// The ID of the message.
	// If not specified, the ID will be auto-generated,
	// see https://redis.io/commands/xadd#specifying-a-stream-id-as-an-argument.
	ID string

	// The message values to be published.
	Values map[string]interface{}
}

func (a *PubArgs) init() {
	if a.ID == "" {
		a.ID = "*" // Use auto-generated ID if not specified.
	}
}

// Publisher provides reliable publishing capability backed by Redis Streams.
type Publisher struct {
	// The Redis client for publishing.
	client *redis.Client
}

// NewPublisher creates an instance of Publisher with the given options.
func NewPublisher(opts *PubOpts) *Publisher {
	return &Publisher{
		client: redis.NewClient(&redis.Options{
			Addr:     opts.Addr,
			DB:       opts.DB,
			PoolSize: opts.PoolSize,
		}),
	}
}

// Publish sends a message to the given topic.
// It returns the ID of the message if successful, or an error if failed.
func (p *Publisher) Publish(args *PubArgs) (string, error) {
	args.init()
	return p.client.XAdd(&redis.XAddArgs{
		Stream:       args.Topic,
		MaxLenApprox: args.MaxLen, // Use `MAXLEN ~` for performance.
		ID:           args.ID,
		Values:       args.Values,
	}).Result()
}

type SubOpts struct {
	// host:port address of the Redis server.
	Addr string

	// Database to be selected after connecting to the Redis server.
	DB int

	// The maximum number of messages to return per topic at each read.
	Count int64

	// The manager that will return the initial last message ID for a given topic.
	IDGetter IDGetter
}

func (o *SubOpts) init() {
	if o.IDGetter == nil {
		o.IDGetter = &ZeroIDGetter{}
	}
}

// Subscriber provides reliable subscribing capability backed by Redis Streams.
type Subscriber struct {
	opts *SubOpts

	// Provides concurrency safety for topics.
	mu sync.Mutex
	// The mapping from "topic name" to "topic reader".
	topics map[string]*reader
}

// NewSubscriber creates an instance of Subscriber with the given options.
func NewSubscriber(opts *SubOpts) *Subscriber {
	opts.init()
	return &Subscriber{
		opts:   opts,
		topics: make(map[string]*reader),
	}
}

// Subscribe registers an interest in the given topics.
func (s *Subscriber) Subscribe(c chan<- Stream, topics ...string) {
	s.mu.Lock()
	for _, topic := range topics {
		// Only add topics that have not been subscribed.
		if _, ok := s.topics[topic]; !ok {
			s.topics[topic] = newReader(topic, c, s.opts)
			s.topics[topic].Start()
		}
	}
	s.mu.Unlock()
}

// Unsubscribe unregisters the interest in the given topics.
// Specifying no topics indicates to unsubscribe all the topics.
func (s *Subscriber) Unsubscribe(topics ...string) {
	s.mu.Lock()
	if len(topics) == 0 {
		// Unsubscribe all the topics.
		for topic, reader := range s.topics {
			reader.Stop()
			delete(s.topics, topic)
		}
	} else {
		// Unsubscribe the specified topics.
		for _, topic := range topics {
			// Only remove topics that have been subscribed.
			if reader, ok := s.topics[topic]; ok {
				reader.Stop()
				delete(s.topics, topic)
			}
		}
	}
	s.mu.Unlock()
}

// LastIDs returns a mapping snapshot from topics to their respective last message IDs.
func (s *Subscriber) LastIDs() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.topics) == 0 {
		return nil
	}

	lastIDs := make(map[string]string)
	for topic, reader := range s.topics {
		lastIDs[topic] = reader.lastID
	}
	return lastIDs
}

// reader is a message reader dedicated to a specific topic.
type reader struct {
	topic string
	count int64

	// The last message ID.
	lastID string

	// The channel to which the message is sent.
	sendC chan<- Stream

	// The Redis client for reading.
	client *redis.Client

	// Used to gracefully shutdown the reading goroutine.
	exitC     chan struct{}
	waitGroup sync.WaitGroup
}

// newReader creates an instance of reader with the given options.
func newReader(topic string, sendC chan<- Stream, opts *SubOpts) *reader {
	return &reader{
		topic:  topic,
		count:  opts.Count,
		lastID: opts.IDGetter.Get(topic),
		sendC:  sendC,
		client: redis.NewClient(&redis.Options{
			Addr: opts.Addr,
			DB:   opts.DB,
			// Only need one connection for reading.
			PoolSize: 1,
		}),
		exitC: make(chan struct{}),
	}
}

// Start starts a goroutine to continuously read data from subscribed topics.
func (r *reader) Start() {
	r.waitGroup.Add(1)
	go func() {
		defer r.waitGroup.Done()

		for {
			streams, err := r.client.XRead(&redis.XReadArgs{
				Streams: []string{r.topic, r.lastID},
				Count:   r.count,
				Block:   0, // Wait for new messages without a timeout.
			}).Result()
			if err != nil {
				str := err.Error()
				if strings.Contains(str, "use of closed network connection") { //||
					//strings.Contains(str, "client is closed") {
					// s.client has been closed.
					return
				}
			}

			// We are reading from only one topic.
			xStream := streams[0]

			count := len(xStream.Messages)
			stream := Stream{
				Topic:    xStream.Stream,
				Messages: make([]Message, count),
			}
			for i := 0; i < count; i++ {
				stream.Messages[i] = (Message)(xStream.Messages[i])
			}

			select {
			case r.sendC <- stream:
				// stream has been sent successfully.

				// Update the last message ID.
				r.lastID = stream.Messages[count-1].ID
			case <-r.exitC:
				return
			}
		}
	}()
}

// Stop closes the Redis client for reading, and gracefully stops the reading goroutine.
func (r *reader) Stop() {
	// Unblock the connection which is waiting for reading.
	// FIXME: Wait until https://github.com/go-redis/redis/issues/933 is fixed.
	r.client.Close()

	close(r.exitC)
	r.waitGroup.Wait()
}
