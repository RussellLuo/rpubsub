package rpubsub

import (
	"time"
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
