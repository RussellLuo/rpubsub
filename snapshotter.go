package rpubsub

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// Snapshotter is a manager that will periodically snapshot the subscribing
// state for a specific subscriber.
type Snapshotter interface {
	Load(topic string) string
	Open(sub *Subscriber)
	Close()
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

func (s *NilSnapshotter) Open(sub *Subscriber) {}

func (s *NilSnapshotter) Close() {}

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

func (s *RedisSnapshotter) getStates(sub *Subscriber) (map[string]SubState, uint64, map[string]string) {
	states := sub.States()
	if len(states) == 0 {
		return states, uint64(0), nil
	}

	totalChanges := uint64(0)
	ids := make(map[string]string, len(states))
	for topic, state := range states {
		totalChanges = totalChanges + state.Changes
		ids[topic] = state.LastID
	}
	return states, totalChanges, ids
}

func (s *RedisSnapshotter) save(ids map[string]string) error {
	if len(ids) == 0 {
		return nil
	}

	bytes, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	if _, err := s.client.Set(s.opts.Key, bytes, s.opts.Expiration).Result(); err != nil {
		return err
	}

	return nil
}

func (s *RedisSnapshotter) Open(sub *Subscriber) {
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

			states, totalChanges, ids := s.getStates(sub)

			now := time.Now()
			// See https://github.com/antirez/redis/blob/aced0328e3fb532496afa1a30eb4227316aef3bd/src/server.c#L1266-L1267.
			if totalChanges >= s.opts.SavePoint.Changes &&
				now.Sub(s.lastSaveTime) > s.opts.SavePoint.Duration {
				if err := s.save(ids); err != nil {
					// TODO: logging?
					continue
				}
				s.lastSaveTime = now
				sub.Snapshot(states)
			}
		}

		saveTicker.Stop()

		// Question: should we save once again before exits?
	}()
}

func (s *RedisSnapshotter) Close() {
	close(s.exitC)
	s.waitGroup.Wait()
}
