package firestoredriver

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kirimatt/goncordia/v2/driver"
)

func pruneLimit(limit int) int {
	if limit <= 0 {
		return 500
	}
	return limit
}

// JobPruneFinalized deletes jobs of params.State finalized at or before
// params.Before. The query needs the composite index
// goncordia_jobs (state ASC, finalized_at ASC); see Migrate. Each delete is
// conditional on the document's update time, so a job changed since the
// query stays.
func (e *executor) JobPruneFinalized(ctx context.Context, params driver.JobPruneParams) (int, error) {
	snaps, err := e.client.Collection(colJobs).
		Where("state", "==", string(params.State)).
		Where("finalized_at", "<=", params.Before.UTC()).
		OrderBy("finalized_at", firestore.Asc).
		Limit(pruneLimit(params.Limit)).
		Documents(ctx).GetAll()
	if err != nil {
		return 0, fmt.Errorf("query expired jobs: %w", err)
	}
	deleted := 0
	for _, snap := range snaps {
		var j jobDoc
		if err := snap.DataTo(&j); err != nil {
			return deleted, err
		}
		if _, err := snap.Ref.Delete(ctx, firestore.LastUpdateTime(snap.UpdateTime)); err != nil {
			if code := status.Code(err); code == codes.FailedPrecondition || code == codes.NotFound {
				continue
			}
			return deleted, fmt.Errorf("delete job %s: %w", snap.Ref.ID, err)
		}
		if err := jobResultDelete(ctx, e.client, snap.Ref.ID); err != nil {
			return deleted, err
		}
		if j.UniqueKey != "" {
			e.client.Collection(colUniq).Doc(j.UniqueKey).Delete(ctx) //nolint:errcheck
		}
		deleted++
	}
	return deleted, nil
}

// JobResultPrune deletes results written at or before params.Before through
// the automatic single-field index on updated_at.
func (e *executor) JobResultPrune(ctx context.Context, params driver.JobPruneParams) (int, error) {
	snaps, err := e.client.Collection(colResults).
		Where("updated_at", "<=", params.Before.UTC()).
		OrderBy("updated_at", firestore.Asc).
		Limit(pruneLimit(params.Limit)).
		Documents(ctx).GetAll()
	if err != nil {
		return 0, fmt.Errorf("query expired results: %w", err)
	}
	deleted := 0
	for _, snap := range snaps {
		if _, err := snap.Ref.Delete(ctx, firestore.LastUpdateTime(snap.UpdateTime)); err != nil {
			if code := status.Code(err); code == codes.FailedPrecondition || code == codes.NotFound {
				continue
			}
			return deleted, fmt.Errorf("delete result %s: %w", snap.Ref.ID, err)
		}
		deleted++
	}
	return deleted, nil
}

var _ driver.JobPruner = (*executor)(nil)
