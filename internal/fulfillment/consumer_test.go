package fulfillment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// fakeReader returns the messages one by one, then blocks until ctx is done.
type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	committed []int64
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if len(r.msgs) > 0 {
		m := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeReader) commits() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...)
}

type fakeWriter struct {
	mu   sync.Mutex
	msgs []kafka.Message
}

func (w *fakeWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, msgs...)
	return nil
}

// scriptedProcessor returns the next error from the list for every call.
type scriptedProcessor struct {
	mu    sync.Mutex
	errs  []error
	calls int
}

func (p *scriptedProcessor) Process(ctx context.Context, value []byte) (Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if len(p.errs) == 0 {
		return ResultConfirmed, nil
	}
	err := p.errs[0]
	p.errs = p.errs[1:]
	return ResultConfirmed, err
}

func runUntil(t *testing.T, c *Consumer, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(finished)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-finished
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestConsumerRetriesTemporaryErrorsBeforeCommit(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{{Offset: 1}, {Offset: 2}}}
	proc := &scriptedProcessor{errs: []error{errors.New("db down"), errors.New("db down")}}
	c := NewConsumer(reader, &fakeWriter{}, proc, quiet)
	c.RetryDelay = time.Millisecond

	runUntil(t, c, func() bool { return len(reader.commits()) == 2 })

	if got := reader.commits(); got[0] != 1 || got[1] != 2 {
		t.Errorf("commits = %v, want [1 2] in order", got)
	}
	if proc.calls != 4 {
		t.Errorf("calls = %d, want 4 (two failures, then two successes)", proc.calls)
	}
}

func TestConsumerSendsBadMessageToDLQ(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{{Topic: "orders", Offset: 7, Value: []byte("garbage")}}}
	dlq := &fakeWriter{}
	proc := &scriptedProcessor{errs: []error{ErrBadMessage}}
	c := NewConsumer(reader, dlq, proc, quiet)

	runUntil(t, c, func() bool { return len(reader.commits()) == 1 })

	if len(dlq.msgs) != 1 || string(dlq.msgs[0].Value) != "garbage" {
		t.Fatalf("dlq = %+v", dlq.msgs)
	}
	headers := map[string]string{}
	for _, h := range dlq.msgs[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["source-topic"] != "orders" || headers["error"] == "" {
		t.Errorf("headers = %v", headers)
	}
	if proc.calls != 1 {
		t.Errorf("a bad message must not be retried, calls = %d", proc.calls)
	}
}
