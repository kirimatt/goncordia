// Package drivertest provides a reusable behavioral contract suite for drivers.
package drivertest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

var sequence atomic.Uint64

// Run verifies the common executor, rescue, and administrative contracts.
// Drivers should call it from their integration tests after Migrate.
func Run(t *testing.T, exec driver.Executor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queue := fmt.Sprintf("conformance-%d", sequence.Add(1))
	now := time.Now().UTC().Add(-time.Second)

	if err := exec.QueuePause(ctx, queue); err != nil {
		t.Fatalf("pause queue: %v", err)
	}
	if err := exec.QueueResume(ctx, queue); err != nil {
		t.Fatalf("resume queue: %v", err)
	}
	leaderName := "leader-" + queue
	if elected, err := exec.LeaderAttemptElect(ctx, driver.LeaderElectParams{
		Name: leaderName, WorkerID: "owner-a", TTL: time.Hour,
	}); err != nil || !elected {
		t.Fatalf("elect owner-a: elected=%v err=%v", elected, err)
	}
	if err := exec.LeaderResign(ctx, driver.LeaderResignParams{Name: leaderName, WorkerID: "owner-b"}); err != nil {
		t.Fatalf("non-owner resign: %v", err)
	}
	if elected, err := exec.LeaderAttemptElect(ctx, driver.LeaderElectParams{
		Name: leaderName, WorkerID: "owner-b", TTL: time.Hour,
	}); err != nil || elected {
		t.Fatalf("non-owner resignation released lease: elected=%v err=%v", elected, err)
	}
	if err := exec.LeaderResign(ctx, driver.LeaderResignParams{Name: leaderName, WorkerID: "owner-a"}); err != nil {
		t.Fatalf("owner resign: %v", err)
	}
	if elected, err := exec.LeaderAttemptElect(ctx, driver.LeaderElectParams{
		Name: leaderName, WorkerID: "owner-b", TTL: time.Hour,
	}); err != nil || !elected {
		t.Fatalf("elect after owner resign: elected=%v err=%v", elected, err)
	}
	t.Cleanup(func() {
		_ = exec.LeaderResign(context.Background(), driver.LeaderResignParams{Name: leaderName, WorkerID: "owner-b"})
	})
	cursorID := "cursor-" + queue
	initialCursor := now.Truncate(time.Second)
	cursor, err := exec.ScheduleCursorGetOrCreate(ctx, driver.ScheduleCursorCreateParams{ID: cursorID, InitialAt: initialCursor})
	if err != nil || !cursor.Created || !cursor.At.Equal(initialCursor) {
		t.Fatalf("create schedule cursor: cursor=%+v err=%v", cursor, err)
	}
	cursorAgain, err := exec.ScheduleCursorGetOrCreate(ctx, driver.ScheduleCursorCreateParams{
		ID: cursorID, InitialAt: initialCursor.Add(-time.Hour),
	})
	if err != nil || cursorAgain.Created || !cursorAgain.At.Equal(initialCursor) {
		t.Fatalf("reload schedule cursor: cursor=%+v err=%v", cursorAgain, err)
	}
	advanced, err := exec.ScheduleCursorAdvance(ctx, driver.ScheduleCursorAdvanceParams{
		ID: cursorID, Expected: initialCursor.Add(-time.Hour), Next: initialCursor.Add(time.Hour),
	})
	if err != nil || advanced {
		t.Fatalf("stale schedule cursor advance: advanced=%v err=%v", advanced, err)
	}
	nextCursor := initialCursor.Add(time.Hour)
	advanced, err = exec.ScheduleCursorAdvance(ctx, driver.ScheduleCursorAdvanceParams{
		ID: cursorID, Expected: initialCursor, Next: nextCursor,
	})
	if err != nil || !advanced {
		t.Fatalf("advance schedule cursor: advanced=%v err=%v", advanced, err)
	}
	cursorAgain, err = exec.ScheduleCursorGetOrCreate(ctx, driver.ScheduleCursorCreateParams{ID: cursorID, InitialAt: initialCursor})
	if err != nil || !cursorAgain.At.Equal(nextCursor) {
		t.Fatalf("read advanced schedule cursor: cursor=%+v err=%v", cursorAgain, err)
	}

	insert := driver.JobInsertParams{
		Queue: queue, Kind: "conformance", Args: []byte(`{"value":1}`),
		RunAt: now, UniqueKey: "unique-" + queue, Timeout: time.Second,
		Tags: []string{"contract"}, PipelineID: "entity-1",
	}
	results, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{insert})
	if err != nil || len(results) != 1 || results[0].Job == nil {
		t.Fatalf("insert: results=%+v err=%v", results, err)
	}
	id := results[0].Job.ID
	t.Cleanup(func() { _ = exec.JobDelete(context.Background(), id) })

	duplicates, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{insert})
	if err != nil || len(duplicates) != 1 || !duplicates[0].UniqueSkip {
		t.Fatalf("unique insert: results=%+v err=%v", duplicates, err)
	}
	crossQueueDuplicate := insert
	crossQueueDuplicate.Queue = queue + "-other"
	duplicates, err = exec.JobInsertMany(ctx, []driver.JobInsertParams{crossQueueDuplicate})
	if err != nil || len(duplicates) != 1 || !duplicates[0].UniqueSkip {
		t.Fatalf("global unique insert: results=%+v err=%v", duplicates, err)
	}

	row, err := exec.JobGetByID(ctx, id)
	if err != nil || row == nil || row.PipelineID != "entity-1" || row.Timeout != time.Second {
		t.Fatalf("get inserted job: row=%+v err=%v", row, err)
	}

	adminExec, ok := exec.(driver.AdminExecutor)
	if !ok {
		t.Fatal("executor does not implement driver.AdminExecutor")
	}
	waitUntil(t, func() bool {
		listed, err := adminExec.JobList(ctx, driver.JobListParams{Queue: queue, Limit: 10})
		return err == nil && len(listed) == 1
	}, "job listing")
	waitUntil(t, func() bool {
		stats, err := adminExec.QueueStats(ctx, queue)
		return err == nil && stats.Total == 1 && stats.States[driver.JobStateAvailable] == 1
	}, "queue stats")

	claimed := fetchEventually(t, ctx, exec, queue)
	if claimed.WorkerID != "conformance-worker" || claimed.AttemptNum != 1 {
		t.Fatalf("claim metadata: %+v", claimed)
	}
	if claimed.AttemptedAt == nil || claimed.LeaseExpiresAt == nil || !claimed.LeaseExpiresAt.After(*claimed.AttemptedAt) {
		t.Fatalf("claim lease metadata: %+v", claimed)
	}
	firstClaim := claimed
	rescuer, ok := exec.(driver.StuckJobRescuer)
	if !ok {
		t.Fatal("executor does not implement driver.StuckJobRescuer")
	}
	rescueDeadline := time.Now().Add(5 * time.Second)
	for {
		rescued, rescueErr := rescuer.JobRescueStuck(ctx, driver.JobRescueParams{
			Queue: queue, At: claimed.LeaseExpiresAt.Add(time.Second), Before: time.Now().UTC().Add(time.Hour),
		})
		if rescueErr == nil && rescued >= 1 {
			break
		}
		if time.Now().After(rescueDeadline) {
			current, getErr := exec.JobGetByID(ctx, id)
			t.Fatalf("timed out waiting for stuck-job rescue: rescue_err=%v row=%+v get_err=%v", rescueErr, current, getErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	claimed = fetchEventually(t, ctx, exec, queue)
	if claimed.AttemptNum < 2 {
		t.Fatalf("rescued job did not retain attempt count: %+v", claimed)
	}
	heartbeater, ok := exec.(driver.JobHeartbeater)
	if !ok {
		t.Fatal("executor does not implement driver.JobHeartbeater")
	}
	if claimed.AttemptedAt == nil || claimed.LeaseExpiresAt == nil {
		t.Fatalf("reclaimed job lease metadata: %+v", claimed)
	}
	persistedClaim, err := exec.JobGetByID(ctx, id)
	if err != nil || persistedClaim == nil || persistedClaim.AttemptedAt == nil || persistedClaim.LeaseExpiresAt == nil {
		t.Fatalf("read persisted reclaimed lease: row=%+v err=%v", persistedClaim, err)
	}
	attemptedAt := *persistedClaim.AttemptedAt
	heartbeatAt := attemptedAt.Add(time.Hour)
	heartbeatExpiresAt := heartbeatAt.Add(time.Hour)
	renewed, err := heartbeater.JobHeartbeat(ctx, driver.JobHeartbeatParams{
		ID: id, WorkerID: claimed.WorkerID, Attempt: claimed.AttemptNum,
		At: heartbeatAt, LeaseExpiresAt: heartbeatExpiresAt,
	})
	if err != nil || !renewed {
		t.Fatalf("heartbeat: renewed=%v err=%v", renewed, err)
	}
	rescued, err := rescuer.JobRescueStuck(ctx, driver.JobRescueParams{
		Queue: queue, At: heartbeatExpiresAt.Add(-time.Second), Before: heartbeatAt.Add(-time.Second),
	})
	if err != nil || rescued != 0 {
		t.Fatalf("heartbeat did not protect running job: rescued=%d err=%v", rescued, err)
	}
	current, err := exec.JobGetByID(ctx, id)
	if err != nil || current == nil || current.AttemptedAt == nil || !current.AttemptedAt.Equal(attemptedAt) ||
		current.LeaseExpiresAt == nil || !current.LeaseExpiresAt.Equal(heartbeatExpiresAt) {
		t.Fatalf("heartbeat mutated claim time or failed to extend lease: row=%+v err=%v", current, err)
	}
	if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
		ID: id, State: driver.JobStateCompleted,
		ExpectedWorkerID: firstClaim.WorkerID, ExpectedAttempt: firstClaim.AttemptNum,
	}); !errors.Is(err, driver.ErrStaleClaim) {
		t.Fatalf("stale completion error=%v, want ErrStaleClaim", err)
	}
	current, err = exec.JobGetByID(ctx, id)
	if err != nil || current == nil || current.State != driver.JobStateRunning || current.AttemptNum != claimed.AttemptNum {
		t.Fatalf("stale worker overwrote current claim: row=%+v err=%v", current, err)
	}
	completionErr, completionTrace := "conformance failure context", "conformance stack trace"
	if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
		ID: id, State: driver.JobStateCompleted,
		Err: &completionErr, Trace: &completionTrace, Attempt: claimed.AttemptNum,
		ExpectedWorkerID: claimed.WorkerID, ExpectedAttempt: claimed.AttemptNum,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	completed, err := exec.JobGetByID(ctx, id)
	if err != nil || completed == nil || len(completed.Errors) != 1 || completed.Errors[0].Trace != completionTrace {
		t.Fatalf("persist attempt trace: row=%+v err=%v", completed, err)
	}
	reinserted, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{insert})
	if err != nil || len(reinserted) != 1 || reinserted[0].UniqueSkip || reinserted[0].Job == nil {
		t.Fatalf("terminal job retained unique slot: results=%+v err=%v", reinserted, err)
	}
	t.Cleanup(func() { _ = exec.JobDelete(context.Background(), reinserted[0].Job.ID) })

	permanent := driver.JobInsertParams{
		Queue: queue, Kind: "permanent-unique", Args: []byte(`{"value":2}`),
		RunAt: now, UniqueKey: "uf2_" + queue,
	}
	permanentResults, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{permanent})
	if err != nil || len(permanentResults) != 1 || permanentResults[0].Job == nil || permanentResults[0].UniqueSkip {
		t.Fatalf("insert permanent unique job: results=%+v err=%v", permanentResults, err)
	}
	permanentID := permanentResults[0].Job.ID
	if err := exec.JobCancel(ctx, permanentID); err != nil {
		t.Fatalf("cancel permanent unique job: %v", err)
	}
	permanentDuplicate, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{permanent})
	if err != nil || len(permanentDuplicate) != 1 || !permanentDuplicate[0].UniqueSkip {
		t.Fatalf("terminal permanent key was released: results=%+v err=%v", permanentDuplicate, err)
	}
	if err := exec.JobDelete(ctx, permanentID); err != nil {
		t.Fatalf("delete permanent unique job: %v", err)
	}
	permanentAfterDelete, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{permanent})
	if err != nil || len(permanentAfterDelete) != 1 || permanentAfterDelete[0].UniqueSkip || permanentAfterDelete[0].Job == nil {
		t.Fatalf("delete did not release permanent key: results=%+v err=%v", permanentAfterDelete, err)
	}
	t.Cleanup(func() { _ = exec.JobDelete(context.Background(), permanentAfterDelete[0].Job.ID) })

	// A backend must choose priority globally across all due candidates, not
	// merely sort a small storage-ordered subset after fetching it.
	var ordered []driver.JobInsertParams
	for i := range 8 {
		ordered = append(ordered, driver.JobInsertParams{
			Queue: queue, Kind: "ordering-low", Args: []byte(fmt.Sprintf(`{"n":%d}`, i)),
			Priority: 0, RunAt: now.Add(-2 * time.Hour),
		})
	}
	ordered = append(ordered, driver.JobInsertParams{
		Queue: queue, Kind: "ordering-high", Args: []byte(`{"n":99}`),
		Priority: 100, RunAt: now.Add(-time.Hour),
	})
	orderedResults, err := exec.JobInsertMany(ctx, ordered)
	if err != nil || len(orderedResults) != len(ordered) {
		t.Fatalf("insert ordering jobs: results=%d err=%v", len(orderedResults), err)
	}
	for _, result := range orderedResults {
		if result.Job != nil {
			id := result.Job.ID
			t.Cleanup(func() { _ = exec.JobDelete(context.Background(), id) })
		}
	}
	if adminExec, ok := exec.(driver.AdminExecutor); ok {
		firstPage, listErr := adminExec.JobList(ctx, driver.JobListParams{Kind: "ordering-low", Limit: 3})
		if listErr != nil || len(firstPage) != 3 {
			t.Fatalf("first job page: rows=%+v err=%v", firstPage, listErr)
		}
		secondPage, listErr := adminExec.JobList(ctx, driver.JobListParams{
			Kind: "ordering-low", Limit: 3, Cursor: driver.EncodeJobCursor(firstPage[len(firstPage)-1]),
		})
		if listErr != nil || len(secondPage) != 3 {
			t.Fatalf("second job page: rows=%+v err=%v", secondPage, listErr)
		}
		seen := make(map[string]struct{}, len(firstPage))
		for _, row := range firstPage {
			seen[row.ID] = struct{}{}
		}
		for _, row := range secondPage {
			if _, duplicate := seen[row.ID]; duplicate {
				t.Fatalf("job %q appeared on consecutive pages", row.ID)
			}
		}
		if _, listErr := adminExec.JobList(ctx, driver.JobListParams{Cursor: "invalid", Limit: 1}); !errors.Is(listErr, driver.ErrInvalidCursor) {
			t.Fatalf("invalid job cursor error=%v, want ErrInvalidCursor", listErr)
		}
	}
	for _, suffix := range []string{"-pagination-a", "-pagination-b", "-pagination-c"} {
		if err := exec.QueuePause(ctx, queue+suffix); err != nil {
			t.Fatalf("create pagination queue: %v", err)
		}
	}
	firstQueues, err := exec.QueueList(ctx, driver.QueueListParams{Limit: 2})
	if err != nil || len(firstQueues) != 2 {
		t.Fatalf("first queue page: rows=%+v err=%v", firstQueues, err)
	}
	secondQueues, err := exec.QueueList(ctx, driver.QueueListParams{
		Limit: 2, Cursor: driver.EncodeQueueCursor(*firstQueues[len(firstQueues)-1]),
	})
	if err != nil || len(secondQueues) == 0 || secondQueues[0].Name <= firstQueues[len(firstQueues)-1].Name {
		t.Fatalf("second queue page: rows=%+v err=%v", secondQueues, err)
	}
	if _, err := exec.QueueList(ctx, driver.QueueListParams{Cursor: "invalid", Limit: 1}); !errors.Is(err, driver.ErrInvalidCursor) {
		t.Fatalf("invalid queue cursor error=%v, want ErrInvalidCursor", err)
	}
	orderedClaim := fetchEventually(t, ctx, exec, queue)
	if orderedClaim.Kind != "ordering-high" || orderedClaim.Priority != 100 {
		t.Fatalf("priority contract selected %+v", orderedClaim)
	}
}

// RunScheduled verifies that an executor does not claim a future job and does
// claim it once the injected clock reaches run_at. Drivers must construct the
// executor with clk; the test advances time without sleeping.
func RunScheduled(t *testing.T, exec driver.Executor, clk *clock.Manual) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queue := fmt.Sprintf("scheduled-conformance-%d", sequence.Add(1))
	runAt := clk.Now().Add(time.Hour)

	results, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{{
		Queue: queue, Kind: "scheduled-conformance", Args: []byte(`{"value":1}`),
		RunAt: runAt,
	}})
	if err != nil || len(results) != 1 || results[0].Job == nil {
		t.Fatalf("insert scheduled job: results=%+v err=%v", results, err)
	}
	id := results[0].Job.ID
	t.Cleanup(func() { _ = exec.JobDelete(context.Background(), id) })
	if results[0].Job.State != driver.JobStateScheduled {
		t.Fatalf("future job state: got %q, want %q", results[0].Job.State, driver.JobStateScheduled)
	}

	rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{
		Queue: queue, Limit: 1, WorkerID: "scheduled-conformance-worker", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatalf("fetch future job: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("future job was claimable before run_at: %+v", rows)
	}

	clk.Advance(time.Hour)
	claimed := fetchEventually(t, ctx, exec, queue)
	if claimed.ID != id || claimed.State != driver.JobStateRunning || claimed.RunAt.After(clk.Now()) {
		t.Fatalf("claim due scheduled job: %+v", claimed)
	}
}

func waitUntil(t *testing.T, condition func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func fetchEventually(t *testing.T, ctx context.Context, exec driver.Executor, queue string) driver.JobRow {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{
			Queue: queue, Limit: 1, WorkerID: "conformance-worker", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		if len(rows) == 1 {
			return rows[0]
		}
		if time.Now().After(deadline) {
			var current []driver.JobRow
			var listErr error
			if adminExec, ok := exec.(driver.AdminExecutor); ok {
				current, listErr = adminExec.JobList(ctx, driver.JobListParams{Queue: queue, Limit: 10})
			}
			t.Fatalf("timed out fetching conformance job: rows=%+v list_err=%v", current, listErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
