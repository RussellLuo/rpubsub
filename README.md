# rpubsub

Reliable implementation of [Publish–subscribe messaging pattern][1] backed by [Redis Streams][2].


## Installation


```bash
$ go get -u github.com/RussellLuo/rpubsub
```


## Motivation

- Learn [Redis Streams][2] by doing.
- Implement a more reliable Publish–subscribe messaging mechanism than [Pub/Sub][3].


## Design & Implementation

- Publishing
    + Just use [XADD][4] directly.
- Subscribing
    + API (inspired by [Principles of designing Go APIs with channels][5]).
    + [Blocking][6] instead of polling (which means one goroutine per topic).
- Snapshotting
    + Built-in snapshotters ([NilSnapshotter][7] and [RedisSnapshotter][8]).
    + Save point (inspired by Redis's [RDB persistence][9]).


## Documentation

Check out the [Godoc][10].


## License

[MIT][11]


[1]: https://en.wikipedia.org/wiki/Publish–subscribe_pattern
[2]: https://redis.io/topics/streams-intro
[3]: https://redis.io/topics/pubsub
[4]: https://redis.io/commands/xadd
[5]: https://inconshreveable.com/07-08-2014/principles-of-designing-go-apis-with-channels/
[6]: https://redis.io/commands/xread#blocking-for-data
[7]: https://godoc.org/github.com/RussellLuo/rpubsub#NilSnapshotter
[8]: https://godoc.org/github.com/RussellLuo/rpubsub#RedisSnapshotter
[9]: https://redis.io/topics/persistence#snapshotting
[10]: https://godoc.org/github.com/RussellLuo/rpubsub
[11]: http://opensource.org/licenses/MIT
