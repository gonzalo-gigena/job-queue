package qq

import (
	"context"
	"testing"
	"time"
)

func TestArchiveMovesFinishedJobsAndDropsOldPartitions(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	now := time.Now()
	if err := partitions(ctx, db, now); err != nil {
		t.Fatal(err)
	}
	key := "k1"
	db.Exec(ctx, `INSERT INTO jobs (queue, kind, status, finished_at, idempotency_key) VALUES ($1, 'noop', 'succeeded', now() - interval '2 h', $2)`, q, key)
	db.Exec(ctx, `INSERT INTO jobs (queue, kind, status, finished_at) VALUES ($1, 'noop', 'succeeded', now()), ($1, 'noop', 'dead', now() - interval '2 h'), ($1, 'noop', 'queued', NULL)`, q)
	for {
		n, err := archive(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if n < archiveBatch {
			break
		}
	}
	var hot, archived int
	db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = $1`, q).Scan(&hot)
	db.QueryRow(ctx, `SELECT count(*) FROM jobs_archive WHERE queue = $1`, q).Scan(&archived)
	if hot != 3 || archived != 1 {
		t.Fatalf("%d hot, %d archived; want the old succeeded one archived, recent, dead and queued kept", hot, archived)
	}
	// Its idempotency key is free again.
	if _, created, err := Enqueue(ctx, db, Job{Queue: q, Kind: "noop", Payload: []byte("{}"), MaxAttempts: 1, IdempotencyKey: &key}); err != nil || !created {
		t.Fatal("re-enqueue after archive:", created, err)
	}

	// Ten days on, today's partition is past retention and gets dropped; new ones are created.
	if err := partitions(ctx, db, now.Add(10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(ctx, `SELECT count(*) FROM jobs_archive WHERE queue = $1`, q).Scan(&archived)
	if archived != 0 {
		t.Fatalf("%d rows survived their partition's drop", archived)
	}
	if err := partitions(ctx, db, now); err != nil { // put today's back for the running stack
		t.Fatal(err)
	}
}
