package qq_test

// Uses only the exported API, the way another Go service would.

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gonzalo-gigena/job-queue/apps/api/qq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSDK(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := "sdk_" + strings.ToLower(rand.Text()[:10])

	// A job enqueued in a transaction that rolls back never existed.
	rollback := errors.New("rollback")
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, _, err := qq.EnqueueTx(ctx, tx, qq.Job{Queue: q, Kind: "sdk_greet"}); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if _, _, err := qq.Enqueue(ctx, db, qq.Job{Queue: q, Kind: "wf:x.y"}); err == nil {
		t.Fatal("enqueued a reserved workflow-step kind")
	}

	done := make(chan string, 1)
	qq.Handle("sdk_greet", func(_ context.Context, _ *pgxpool.Pool, d qq.Delivery) error {
		done <- string(d.Payload)
		return nil
	})
	wctx, cancel := context.WithCancel(ctx)
	stopped := make(chan error)
	go func() { stopped <- qq.Work(wctx, db, url, "sdk-worker", []string{q}) }()
	defer func() { cancel(); <-stopped }()

	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		_, _, err := qq.EnqueueTx(ctx, tx, qq.Job{Queue: q, Kind: "sdk_greet", Payload: []byte(`{"name":"ada"}`)})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got != `{"name": "ada"}` && got != `{"name":"ada"}` {
			t.Fatalf("payload %s", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the handler never ran (or the rolled-back job ran instead)")
	}
	select {
	case extra := <-done:
		t.Fatalf("handler ran twice, second with %s: the rolled-back job exists", extra)
	case <-time.After(300 * time.Millisecond):
	}
}
