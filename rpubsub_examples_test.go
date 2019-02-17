package rpubsub_test

import (
	"fmt"
	"time"

	"github.com/RussellLuo/rpubsub"
	"github.com/go-redis/redis"
)

func Example_publish() {
	pub := rpubsub.NewPublisher(redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}))

	id, _ := pub.Publish(&rpubsub.PubArgs{
		Topic: "test-0",
		// This topic only holds the latest 10 messages.
		MaxLen: 10,
		Values: map[string]interface{}{
			"greeting": "hello world",
		},
	})

	fmt.Printf("Sent a message with ID: %s\n", id)
}

func Example_subscribe() {
	sub := rpubsub.NewSubscriber(&rpubsub.SubOpts{
		NewRedisClient: func() rpubsub.RedisClient {
			return redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
			})
		},
		Count: 10,
	})

	streams := make(chan rpubsub.Stream)
	sub.Subscribe(streams, "test-0")
	defer sub.Unsubscribe()

	for stream := range streams {
		fmt.Printf("Received messages %+v from topic %+v\n", stream.Messages, stream.Topic)
	}
}

func Example_subscribe_with_snapshot() {
	snap := rpubsub.NewRedisSnapshotter(&rpubsub.RedisSnapshotterOpts{
		Addr:         "localhost:6379",
		Key:          "node1",
		Expiration:   24 * time.Hour,
		SaveInterval: 100 * time.Millisecond,
		SavePoint: &rpubsub.SavePoint{
			Duration: time.Second,
			Changes:  10,
		},
	})

	sub := rpubsub.NewSubscriber(&rpubsub.SubOpts{
		NewRedisClient: func() rpubsub.RedisClient {
			return redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
			})
		},
		Count:       10,
		Snapshotter: snap,
	})

	streams := make(chan rpubsub.Stream)
	sub.Subscribe(streams, "test-0")
	defer sub.Unsubscribe()

	for stream := range streams {
		fmt.Printf("Received messages %+v from topic %+v\n", stream.Messages, stream.Topic)
	}
}
