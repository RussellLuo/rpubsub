package rpubsub_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/RussellLuo/rpubsub"
)

// NOTE: run the following command to delete all the data generated by tests.
// $ redis-cli del $(redis-cli keys test-*)

func genTopic() string {
	return fmt.Sprintf("test-%d", time.Now().UnixNano())
}

func TestPublisher_Publish(t *testing.T) {
	pub := rpubsub.NewPublisher(&rpubsub.PubOpts{
		Addr: "localhost:6379",
	})

	topic := genTopic()

	cases := []struct {
		name   string
		id     string
		isWant func(string, error) bool
	}{
		{
			"with-invalid-id",
			"0-0",
			func(id string, err error) bool {
				return err != nil && err.Error() == "ERR The ID specified in XADD is equal or smaller than the target stream top item"
			},
		},
		{
			"with-valid-id",
			"0-1",
			func(id string, err error) bool {
				return id == "0-1" && err == nil
			},
		},
		{
			"with-no-id",
			"",
			func(id string, err error) bool {
				return id != "" && err == nil
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, err := pub.Publish(&rpubsub.PubArgs{
				Topic: topic,
				ID:    c.id,
				Values: map[string]interface{}{
					"greeting": "hello world",
				},
			})
			if !c.isWant(id, err) {
				t.Errorf("Publish returns: id (%+v), err (%+v)", id, err)
			}
		})
	}
}

func TestSubscriber_Subscribe(t *testing.T) {
	topic := genTopic()

	// Publish a message.
	pub := rpubsub.NewPublisher(&rpubsub.PubOpts{
		Addr: "localhost:6379",
	})
	id, _ := pub.Publish(&rpubsub.PubArgs{
		Topic: topic,
		Values: map[string]interface{}{
			"greeting": "hello world",
		},
	})

	// Subscribe the topic.
	sub := rpubsub.NewSubscriber(&rpubsub.SubOpts{
		Addr: "localhost:6379",
	})

	streams := make(chan rpubsub.Stream)
	sub.Subscribe(streams, topic)
	defer sub.Unsubscribe()

	stream := <-streams
	want := rpubsub.Stream{
		Topic: topic,
		Messages: []rpubsub.Message{
			{
				ID: id,
				Values: map[string]interface{}{
					"greeting": "hello world",
				},
			},
		},
	}

	if !reflect.DeepEqual(stream, want) {
		t.Errorf("Stream read: Got (%+v) != Want (%+v)", stream, want)
	}
}