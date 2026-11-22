package mongodbv2

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/kirimatt/goncordia/v2/driver"
)

// JobCompleteMany completes many running jobs with one find and one
// unordered bulkWrite, instead of a find and an update per job. Fencing and
// the unique-key release match jobSetStateIfRunning for JobStateCompleted.
func (e *executor) JobCompleteMany(ctx context.Context, params []driver.JobSetStateParams) ([]bool, error) {
	out := make([]bool, len(params))
	if len(params) == 0 {
		return out, nil
	}
	oids := make([]bson.ObjectID, len(params))
	for i, p := range params {
		oid, err := bson.ObjectIDFromHex(p.ID)
		if err != nil {
			return nil, fmt.Errorf("invalid job id %q: %w", p.ID, err)
		}
		oids[i] = oid
	}
	jobs := e.db.Collection(jobsCollection)

	// Read the current claims to drop stale ones and learn unique keys.
	cur, err := jobs.Find(ctx,
		bson.M{"_id": bson.M{"$in": oids}, "state": string(driver.JobStateRunning)},
		options.Find().SetProjection(bson.M{"unique_key": 1, "worker_id": 1, "attempt_num": 1}))
	if err != nil {
		return nil, fmt.Errorf("complete jobs: %w", err)
	}
	type claim struct {
		ID         bson.ObjectID `bson:"_id"`
		UniqueKey  string        `bson:"unique_key"`
		WorkerID   string        `bson:"worker_id"`
		AttemptNum int           `bson:"attempt_num"`
	}
	var claims []claim
	if err := cur.All(ctx, &claims); err != nil {
		return nil, fmt.Errorf("complete jobs: %w", err)
	}
	running := make(map[bson.ObjectID]claim, len(claims))
	for _, c := range claims {
		running[c.ID] = c
	}

	now := e.clk.Now()
	var models []mongo.WriteModel
	var eligible []int
	for i, p := range params {
		c, ok := running[oids[i]]
		if !ok || !p.MatchesClaim(c.WorkerID, c.AttemptNum) {
			continue
		}
		filter := bson.M{"_id": oids[i], "state": string(driver.JobStateRunning)}
		if p.ExpectedWorkerID != "" {
			filter["worker_id"] = p.ExpectedWorkerID
		}
		if p.ExpectedAttempt > 0 {
			filter["attempt_num"] = p.ExpectedAttempt
		}
		unset := bson.M{"worker_id": "", "lease_expires_at": ""}
		if !driver.IsPermanentUniqueKey(c.UniqueKey) {
			unset["unique_key"] = ""
		}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(bson.M{
			"$set":   bson.M{"state": string(driver.JobStateCompleted), "finalized_at": now},
			"$unset": unset,
		}))
		eligible = append(eligible, i)
	}
	if len(models) == 0 {
		return out, nil
	}
	res, err := jobs.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return nil, fmt.Errorf("complete jobs: %w", err)
	}
	if res.ModifiedCount == int64(len(models)) {
		for _, i := range eligible {
			out[i] = true
		}
		return out, nil
	}

	// A claim changed between the find and the write (rescue, re-claim):
	// read back which of the eligible jobs this batch finalized.
	ids := make([]bson.ObjectID, len(eligible))
	for k, i := range eligible {
		ids[k] = oids[i]
	}
	cur, err = jobs.Find(ctx,
		bson.M{"_id": bson.M{"$in": ids}, "state": string(driver.JobStateCompleted), "finalized_at": now},
		options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("complete jobs: %w", err)
	}
	var done []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := cur.All(ctx, &done); err != nil {
		return nil, fmt.Errorf("complete jobs: %w", err)
	}
	finalized := make(map[bson.ObjectID]bool, len(done))
	for _, d := range done {
		finalized[d.ID] = true
	}
	for _, i := range eligible {
		out[i] = finalized[oids[i]]
	}
	return out, nil
}
