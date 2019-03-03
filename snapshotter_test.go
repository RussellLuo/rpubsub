package rpubsub_test

import (
	"testing"
	"time"

	"github.com/RussellLuo/rpubsub"
	"github.com/go-redis/redis"
)

func TestRedisSnapshotter_LoadAndStore(t *testing.T) {
	snap := rpubsub.NewRedisSnapshotter(
		redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
		&rpubsub.RedisSnapshotterOpts{
			KeyPrefix:    "node1:",
			Expiration:   24 * time.Hour,
			SaveInterval: 100 * time.Millisecond,
			SavePoint: &rpubsub.SavePoint{
				Duration: time.Second,
				Changes:  10,
			},
		},
	)

	topic := genTopic()

	cases := []struct {
		storer func()
		want   string
	}{
		{
			func() {},
			"0-0",
		},
		{
			func() { snap.Store(topic, "0-1") },
			"0-1",
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			c.storer()
			lastID := snap.Load(topic)
			if lastID != c.want {
				t.Errorf("Got (%+v) != Want (%+v)", lastID, c.want)
			}
		})
	}
}

type mockSubMonitor struct {
	statesGetter func() map[string]rpubsub.SubState
}

func (s *mockSubMonitor) States() map[string]rpubsub.SubState {
	return s.statesGetter()
}

func TestRedisSnapshotter_StartAndStop(t *testing.T) {
	snap := rpubsub.NewRedisSnapshotter(
		redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
		&rpubsub.RedisSnapshotterOpts{
			KeyPrefix:    "node1:",
			Expiration:   24 * time.Hour,
			SaveInterval: 100 * time.Millisecond,
			SavePoint: &rpubsub.SavePoint{
				Duration: time.Second,
				Changes:  10,
			},
		},
	)

	topic := genTopic()

	cases := []struct {
		statesGetter func() map[string]rpubsub.SubState
		waitTime     time.Duration
		want         string
	}{
		{
			func() map[string]rpubsub.SubState {
				return map[string]rpubsub.SubState{
					topic: {
						LastID:  "0-1",
						Changes: 5,
					},
				}
			},
			1100 * time.Millisecond,
			"0-0",
		},
		{
			func() map[string]rpubsub.SubState {
				return map[string]rpubsub.SubState{
					topic: {
						LastID:  "0-1",
						Changes: 10,
					},
				}
			},
			1100 * time.Millisecond,
			"0-1",
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			snap.Start(&mockSubMonitor{c.statesGetter})

			time.Sleep(c.waitTime)
			lastID := snap.Load(topic)

			snap.Stop()

			if lastID != c.want {
				t.Errorf("Got (%+v) != Want (%+v)", lastID, c.want)
			}
		})
	}
}
