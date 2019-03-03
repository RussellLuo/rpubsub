package rpubsub

import (
	"sync"
	"time"
)

// SubMonitor is a monitor that can sample and return the current subscribing
// state for a specific subscriber.
type SubMonitor interface {
	States() map[string]SubState
}

// Snapshotter is a manager that will snapshot the subscribing state
// for a specific subscriber.
type Snapshotter interface {
	// Load returns the ID of the last received message for topic.
	Load(topic string) string

	// Store saves lastID as the ID of the last received message for topic.
	Store(topic, lastID string) error

	// Start starts a loop that will periodically save the subscribing
	// state for all the topics subscribed by the subscriber, which is
	// monitored by monitor.
	Start(monitor SubMonitor)

	// Stop stops the loop started by Start.
	Stop()
}

// SavePoint configures a snapshotter to have it save the last message ID
// for a topic every `Duration`, if there are at least `Changes` messages
// received in the topic.
type SavePoint struct {
	Duration time.Duration
	Changes  uint64
}

// NilSnapshotter is a fake manager that does no snapshotting.
type NilSnapshotter struct{}

func (s *NilSnapshotter) Load(topic string) string {
	return "0-0"
}

func (s *NilSnapshotter) Store(topic, lastID string) error {
	return nil
}

func (s *NilSnapshotter) Start(monitor SubMonitor) {}

func (s *NilSnapshotter) Stop() {}

type RedisSnapshotterOpts struct {
	// The prefix of the key, which holds the ID of the last received message
	// for a topic. The final key name is KeyPrefix + topic.
	KeyPrefix string
	// The TTL (time to live) of the value holden by the above key.
	Expiration time.Duration

	SavePoint *SavePoint
}

// RedisSnapshotter is a manager that will save the subscribing state to Redis.
type RedisSnapshotter struct {
	client RedisClient

	opts *RedisSnapshotterOpts

	// Used to gracefully shutdown the snapshotting goroutine.
	exitC     chan struct{}
	waitGroup sync.WaitGroup
}

func NewRedisSnapshotter(client RedisClient, opts *RedisSnapshotterOpts) *RedisSnapshotter {
	return &RedisSnapshotter{
		client: client,
		opts:   opts,
		exitC:  make(chan struct{}),
	}
}

func (s *RedisSnapshotter) key(topic string) string {
	return s.opts.KeyPrefix + topic
}

func (s *RedisSnapshotter) Load(topic string) string {
	if lastID, err := s.client.Get(s.key(topic)).Result(); err == nil {
		return lastID
	}
	return "0-0"
}

func (s *RedisSnapshotter) Store(topic, lastID string) error {
	if _, err := s.client.Set(s.key(topic), lastID, s.opts.Expiration).Result(); err != nil {
		return err
	}
	return nil
}

func (s *RedisSnapshotter) snapshot(states map[string]SubState, force bool) {
	for topic, state := range states {
		// See https://github.com/antirez/redis/blob/aced0328e3fb532496afa1a30eb4227316aef3bd/src/server.c#L1266-L1267.
		if force || state.Changes >= s.opts.SavePoint.Changes {
			if err := s.Store(topic, state.LastID); err != nil {
				// TODO: logging?
				continue
			}
		}
	}
}

func (s *RedisSnapshotter) Start(monitor SubMonitor) {
	s.waitGroup.Add(1)
	go func() {
		defer s.waitGroup.Done()

		saveTicker := time.NewTicker(s.opts.SavePoint.Duration)
		defer saveTicker.Stop()

		for {
			select {
			case <-saveTicker.C:
			case <-s.exitC:
				s.snapshot(monitor.States(), true)
				return
			}

			s.snapshot(monitor.States(), false)
		}
	}()
}

func (s *RedisSnapshotter) Stop() {
	s.exitC <- struct{}{}
	s.waitGroup.Wait()
}
