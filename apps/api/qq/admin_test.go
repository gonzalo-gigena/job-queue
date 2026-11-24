package qq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequireAdmin(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	for name, c := range map[string]struct {
		token, user, pass, bearer string
		want                      int
	}{
		"unset":  {token: "", pass: "", want: 503},
		"none":   {token: "s3cret", want: 401},
		"wrong":  {token: "s3cret", user: "admin", pass: "nope", want: 401},
		"basic":  {token: "s3cret", user: "anyone", pass: "s3cret", want: 204},
		"bearer": {token: "s3cret", bearer: "s3cret", want: 204},
	} {
		r := httptest.NewRequest("GET", "/api/admin/queues", nil)
		if c.user != "" {
			r.SetBasicAuth(c.user, c.pass)
		}
		if c.bearer != "" {
			r.Header.Set("Authorization", "Bearer "+c.bearer)
		}
		w := httptest.NewRecorder()
		RequireAdmin(c.token, ok).ServeHTTP(w, r)
		if w.Code != c.want {
			t.Errorf("%s: %d, want %d", name, w.Code, c.want)
		}
		if w.Code == 401 && w.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: 401 without WWW-Authenticate, so the browser won't prompt", name)
		}
	}
}

func TestCancelRunningJobFencesItsWorker(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	Enqueue(ctx, db, Job{Queue: q, Kind: "noop", Payload: []byte("{}"), MaxAttempts: 5})
	jobs, _, err := dequeue(ctx, db, "w", []string{q}, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatal(jobs, err)
	}
	if ok, err := cancelJob(ctx, db, jobs[0].ID); !ok || err != nil {
		t.Fatal("cancel:", ok, err)
	}
	if held, _ := heartbeat(ctx, db, jobs[0], "w"); held {
		t.Fatal("the worker still holds a cancelled job")
	}
	if held, _ := complete(ctx, db, jobs[0], "w", nil, nil); held {
		t.Fatal("a cancelled job was completed")
	}
	if ok, _ := cancelJob(ctx, db, jobs[0].ID); ok {
		t.Fatal("cancelled twice")
	}
}

func TestQueueOverviewAndPause(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	db.Exec(ctx, `INSERT INTO jobs (queue, kind) SELECT $1, 'noop' FROM generate_series(1, 3)`, q)
	db.Exec(ctx, `INSERT INTO jobs (queue, kind, status, finished_at) VALUES ($1, 'noop', 'succeeded', now()), ($1, 'noop', 'dead', now())`, q)
	if err := pauseQueue(ctx, db, q, true); err != nil {
		t.Fatal(err)
	}
	if jobs, _, _ := dequeue(ctx, db, "w", []string{q}, 1); len(jobs) != 0 {
		t.Fatal("claimed from a paused queue")
	}
	qs, err := queueOverview(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	var got *queueStats
	for i := range qs {
		if qs[i].Queue == q {
			got = &qs[i]
		}
	}
	if got == nil || got.Ready != 3 || got.Dead != 1 || got.DonePerMin != 1 || !got.Paused || got.FailureRate == nil || *got.FailureRate != 0.5 {
		t.Fatalf("overview %+v", got)
	}
	pauseQueue(ctx, db, q, false)
	if jobs, _, _ := dequeue(ctx, db, "w", []string{q}, 1); len(jobs) != 1 {
		t.Fatal("resumed queue claimed nothing")
	}
}
