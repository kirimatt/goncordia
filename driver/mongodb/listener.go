package mongodriver

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/kirimatt/goncordia/driver"
)

// listener uses a collection change stream. Transactional inserts become
// visible only after commit, which preserves the outbox guarantee.
type listener struct {
	db   *mongo.Database
	mu   sync.Mutex
	subs map[string]*changeSubscription
}

type changeSubscription struct {
	stream *mongo.ChangeStream
	cancel context.CancelFunc
	done   chan struct{}
}

func (l *listener) Listen(ctx context.Context, queue string) (<-chan driver.Notification, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: "insert"},
			{Key: "fullDocument.queue", Value: queue},
		}}},
	}
	subCtx, cancel := context.WithCancel(ctx)
	stream, err := l.db.Collection(jobsCollection).Watch(subCtx, pipeline)
	if err != nil {
		cancel()
		return nil, err
	}
	sub := &changeSubscription{stream: stream, cancel: cancel, done: make(chan struct{})}
	l.mu.Lock()
	if l.subs == nil {
		l.subs = make(map[string]*changeSubscription)
	}
	previous := l.subs[queue]
	l.subs[queue] = sub
	l.mu.Unlock()
	if previous != nil {
		previous.cancel()
	}
	out := make(chan driver.Notification, 32)
	go func() {
		defer close(out)
		defer close(sub.done)
		defer func() {
			l.mu.Lock()
			if l.subs[queue] == sub {
				delete(l.subs, queue)
			}
			l.mu.Unlock()
			_ = stream.Close(context.Background())
		}()
		for stream.Next(subCtx) {
			select {
			case out <- driver.Notification{Queue: queue}:
			default:
				// The worker pool has a polling fallback for dropped notifications.
			}
		}
	}()
	return out, nil
}

func (l *listener) Unlisten(ctx context.Context, queue string) error {
	l.mu.Lock()
	sub := l.subs[queue]
	delete(l.subs, queue)
	l.mu.Unlock()
	if sub == nil {
		return nil
	}
	sub.cancel()
	select {
	case <-sub.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *listener) Close() error {
	l.mu.Lock()
	subs := make([]*changeSubscription, 0, len(l.subs))
	for _, sub := range l.subs {
		subs = append(subs, sub)
	}
	l.subs = make(map[string]*changeSubscription)
	l.mu.Unlock()
	for _, sub := range subs {
		sub.cancel()
	}
	for _, sub := range subs {
		<-sub.done
	}
	return nil
}

var _ driver.Listener = (*listener)(nil)
