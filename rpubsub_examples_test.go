package rpubsub_test

import (
	"fmt"

	"github.com/RussellLuo/rpubsub"
)

func Example_publish() {
	pub := rpubsub.NewPublisher(&rpubsub.PubOpts{
		Addr: "localhost:6379",
	})

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
		Addr: "localhost:6379",
	})

	streams := make(chan rpubsub.Stream)
	sub.Subscribe(streams, "test-0")
	defer sub.Unsubscribe()

	for stream := range streams {
		fmt.Printf("Received messages %+v from topic %+v\n", stream.Messages, stream.Topic)
	}
}
