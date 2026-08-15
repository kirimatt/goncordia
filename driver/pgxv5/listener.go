package pgxv5

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kirimatt/goncordia/driver"
)

// listener implements driver.Listener using PostgreSQL LISTEN/NOTIFY.
// Each Listen call acquires a dedicated connection from the pool.
type listener struct {
	pool *pgxpool.Pool
	mu   sync.Mutex
	subs map[string]*subscription
}

type subscription struct {
	conn   *pgxpool.Conn
	ch     chan driver.Notification
	queue  string
	cancel context.CancelFunc
}

func (l *listener) Listen(ctx context.Context, queue string) (<-chan driver.Notification, error) {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for LISTEN: %w", err)
	}

	channel := "goncordia:" + queue
	if _, err := conn.Exec(ctx, "LISTEN "+pgQuoteIdentifier(channel)); err != nil {
		conn.Release()
		return nil, fmt.Errorf("LISTEN %s: %w", channel, err)
	}

	ch := make(chan driver.Notification, 32)
	subCtx, cancel := context.WithCancel(ctx)

	sub := &subscription{conn: conn, ch: ch, queue: queue, cancel: cancel}
	l.mu.Lock()
	if l.subs == nil {
		l.subs = make(map[string]*subscription)
	}
	previous := l.subs[queue]
	l.subs[queue] = sub
	l.mu.Unlock()
	if previous != nil {
		previous.cancel()
	}
	go l.receiveLoop(subCtx, sub)

	return ch, nil
}

func (l *listener) receiveLoop(ctx context.Context, s *subscription) {
	defer s.conn.Release()
	defer close(s.ch)
	defer func() {
		l.mu.Lock()
		if l.subs[s.queue] == s {
			delete(l.subs, s.queue)
		}
		l.mu.Unlock()
	}()

	for {
		n, err := s.conn.Conn().WaitForNotification(ctx)
		if err != nil {
			// ctx cancelled or connection closed — normal shutdown
			return
		}
		_ = n // payload is the job ID; we only need the queue name
		select {
		case s.ch <- driver.Notification{Queue: s.queue}:
		default:
			// channel full — notification dropped; fallback ticker will cover it
		}
	}
}

func (l *listener) Unlisten(_ context.Context, queue string) error {
	l.mu.Lock()
	sub := l.subs[queue]
	delete(l.subs, queue)
	l.mu.Unlock()
	if sub != nil {
		sub.cancel()
	}
	return nil
}

func (l *listener) Close() error {
	l.mu.Lock()
	subs := make([]*subscription, 0, len(l.subs))
	for _, sub := range l.subs {
		subs = append(subs, sub)
	}
	l.subs = make(map[string]*subscription)
	l.mu.Unlock()
	for _, sub := range subs {
		sub.cancel()
	}
	return nil
}

// pgQuoteIdentifier wraps an identifier in double quotes and escapes internal quotes.
func pgQuoteIdentifier(s string) string {
	// Simple safe escaping: double any existing double-quotes
	escaped := ""
	for _, c := range s {
		if c == '"' {
			escaped += `""`
		} else {
			escaped += string(c)
		}
	}
	return `"` + escaped + `"`
}
