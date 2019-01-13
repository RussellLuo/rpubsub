package rpubsub

import (
	"strings"
	"sync"
	"sync/atomic"

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
	// Zero value means no limit.
	Count int64

	// The manager that will return the initial last message ID for a given topic.
	Snapshotter Snapshotter
}

func (o *SubOpts) init() {
	if o.Snapshotter == nil {
		o.Snapshotter = &NilSnapshotter{}
	}
}

// State represents the subscribing state of a specific topic.
type SubState struct {
	// The ID of the last received message.
	LastID string
	// The number of times LastID is changed since the last snapshot.
	Changes uint64
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
	before := len(s.topics)

	for _, topic := range topics {
		// Only add topics that have not been subscribed.
		if _, ok := s.topics[topic]; !ok {
			s.topics[topic] = newReader(topic, c, s.opts)
			s.topics[topic].Start()
		}
	}

	after := len(s.topics)
	s.mu.Unlock()

	if before == 0 && after > 0 {
		// Subscribed topics become non-empty.
		s.opts.Snapshotter.Open(s)
	}
}

// Unsubscribe unregisters the interest in the given topics.
// Specifying no topics indicates to unsubscribe all the topics.
func (s *Subscriber) Unsubscribe(topics ...string) {
	s.mu.Lock()
	before := len(s.topics)

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

	after := len(s.topics)
	s.mu.Unlock()

	if before > 0 && after == 0 {
		// Subscribed topics become empty.
		s.opts.Snapshotter.Close()
	}
}

// States returns a mapping from topics to their respective subscribing state.
func (s *Subscriber) States() map[string]SubState {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.topics) == 0 {
		return nil
	}

	states := make(map[string]SubState)
	for topic, reader := range s.topics {
		states[topic] = reader.State()
	}
	return states
}

// Snapshot notifies the subscriber that the given states have already been snapshotted.
func (s *Subscriber) Snapshot(states map[string]SubState) {
	s.mu.Lock()

	for topic, state := range states {
		if reader, ok := s.topics[topic]; ok {
			// Decrement the number of changes that has been snapshotted.
			reader.DecChanges(state.Changes)
		}
	}

	s.mu.Unlock()
}

// reader is a message reader dedicated to a specific topic.
type reader struct {
	topic string
	count int64

	lastID  *atomic.Value // string
	changes uint64

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
	lastID := new(atomic.Value)
	lastID.Store(opts.Snapshotter.Load(topic))

	return &reader{
		topic:  topic,
		count:  opts.Count,
		lastID: lastID,
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

// State atomically returns the subscribing state.
func (r *reader) State() SubState {
	return SubState{
		LastID:  r.lastID.Load().(string),
		Changes: atomic.LoadUint64(&r.changes),
	}
}

// DecChanges atomically decrements delta from the internal count of changes.
func (r *reader) DecChanges(delta uint64) {
	atomic.AddUint64(&r.changes, ^uint64(delta-1))
}

// Start starts a goroutine to continuously read data from subscribed topics.
func (r *reader) Start() {
	r.waitGroup.Add(1)
	go func() {
		defer r.waitGroup.Done()

		for {
			lastID := r.lastID.Load().(string)
			streams, err := r.client.XRead(&redis.XReadArgs{
				Streams: []string{r.topic, lastID},
				Count:   r.count,
				Block:   0, // Wait for new messages without a timeout.
			}).Result()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					// r.client has been closed.
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

				// Update the state.
				r.lastID.Store(stream.Messages[count-1].ID)
				atomic.AddUint64(&r.changes, uint64(count))
			case <-r.exitC:
				return
			}
		}
	}()
}

// Stop closes the Redis client for reading, and gracefully stops the reading goroutine.
func (r *reader) Stop() {
	// Unblock the connection which is waiting for reading.
	// See https://github.com/go-redis/redis/issues/933.
	r.client.Close()

	close(r.exitC)
	r.waitGroup.Wait()
}
