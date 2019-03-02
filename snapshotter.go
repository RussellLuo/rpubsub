package rpubsub

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// Snapshotter is a manager that will snapshot the subscribing state for a specific subscriber.
type Snapshotter interface {
	// Load returns the ID of the last received message for topic.
	Load(topic string) string

	// Store saves lastID as the ID of the last received message for topic.
	Store(topic, lastID string) error

	// Start starts a loop that will periodically save the subscribing state for all the topics subscribed by sub.
	Start(sub *Subscriber)

	// Stop stops the loop started by Start.
	Stop()
}

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

func (s *NilSnapshotter) Start(sub *Subscriber) {}

func (s *NilSnapshotter) Stop() {}

type RedisSnapshotterOpts struct {
	Addr       string
	Key        string
	Expiration time.Duration

	SaveInterval time.Duration
	SavePoint    *SavePoint
}

// RedisSnapshotter is a manager that will save the subscribing state to Redis.
type RedisSnapshotter struct {
	client *redis.Client

	opts         *RedisSnapshotterOpts
	lastSaveTime time.Time

	// Used to gracefully shutdown the snapshotting goroutine.
	exitC     chan struct{}
	waitGroup sync.WaitGroup

	ids map[string]string
}

func NewRedisSnapshotter(opts *RedisSnapshotterOpts) *RedisSnapshotter {
	client := redis.NewClient(&redis.Options{
		Addr: opts.Addr,
		// Only need one connection for reading or writing.
		PoolSize: 1,
	})
	idsStr, err := client.Get(opts.Key).Result()
	if err != nil {
		panic(err)
	}

	var ids map[string]string
	if err := json.Unmarshal([]byte(idsStr), &ids); err != nil {
		panic(err)
	}

	return &RedisSnapshotter{
		client:       client,
		opts:         opts,
		lastSaveTime: time.Now(),
		exitC:        make(chan struct{}),
		ids:          ids,
	}
}

func (s *RedisSnapshotter) Load(topic string) string {
	if id, ok := s.ids[topic]; ok {
		return id
	}
	return "0-0"
}

func (s *RedisSnapshotter) Store(topic, lastID string) error {
	if _, err := s.client.Set(s.opts.Key, lastID, s.opts.Expiration).Result(); err != nil {
		return err
	}
	return nil
}

func (s *RedisSnapshotter) Start(sub *Subscriber) {
	s.waitGroup.Add(1)
	go func() {
		defer s.waitGroup.Done()

		saveTicker := time.NewTicker(s.opts.SaveInterval)

		for {
			select {
			case <-saveTicker.C:
			case <-s.exitC:
				break
			}

			now := time.Now()
			if now.Sub(s.lastSaveTime) > s.opts.SavePoint.Duration {
				for topic, state := range sub.States() {
					// See https://github.com/antirez/redis/blob/aced0328e3fb532496afa1a30eb4227316aef3bd/src/server.c#L1266-L1267.
					if state.Changes >= s.opts.SavePoint.Changes {
						if err := s.Store(topic, state.LastID); err != nil {
							// TODO: logging?
							continue
						}
					}
				}
				s.lastSaveTime = now
			}
		}

		saveTicker.Stop()
	}()
}

func (s *RedisSnapshotter) Stop() {
	close(s.exitC)
	s.waitGroup.Wait()
}
