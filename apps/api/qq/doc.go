// Package qq is a Postgres-backed job queue and workflow engine, and its Go SDK.
//
// Enqueue from any Go service, on its own or inside the transaction that makes the
// change the job is about:
//
//	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
//		if _, err := tx.Exec(ctx, `INSERT INTO users …`); err != nil {
//			return err
//		}
//		_, _, err := qq.EnqueueTx(ctx, tx, qq.Job{Kind: "welcome_email", Payload: payload})
//		return err
//	})
//
// Run jobs: register handlers, then call Work. Delivery is at-least-once, so handlers
// must be idempotent.
//
//	qq.Handle("welcome_email", func(ctx context.Context, db *pgxpool.Pool, d qq.Delivery) error { … })
//	err := qq.Work(ctx, db, directURL, hostname, []string{"default"})
//
// Workflows are DAGs of steps; see NewWorkflow.
package qq
