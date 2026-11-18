package dynamodbdriver

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/kirimatt/goncordia/v2/driver"
)

// Retention index layout.
//
// Terminal jobs carry rt_pk = "<bucket>#<shard>" and rt_at = the
// finalization time in a fixed-width UTC format, so string order is time
// order. gsi_retention is keyed on them and is sparse: active jobs have no
// rt_pk and stay out of it. Buckets are the terminal states plus
// completed_result for completed jobs whose result is still stored; pruning
// a result moves the job to the completed bucket, so later passes do not
// revisit it. Sixteen shards spread writes, since a GSI partition key that
// takes every finalization would throttle the base table.
const (
	retentionShards        = 16
	bucketCompletedResult  = "completed_result"
	retentionTimeFormat    = "2006-01-02T15:04:05.000000000Z"
	retentionMigrationID   = "goncordia:migration:retention-v1"
	retentionIndexWaitTime = 10 * time.Minute
)

func retentionBucket(state driver.JobState, hasResult bool) string {
	if state == driver.JobStateCompleted && hasResult {
		return bucketCompletedResult
	}
	return string(state)
}

func retentionShard(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % retentionShards)
}

func retentionPK(bucket, id string) string {
	return bucket + "#" + strconv.Itoa(retentionShard(id))
}

func retentionTime(t time.Time) string { return t.UTC().Format(retentionTimeFormat) }

func retentionIndex() types.GlobalSecondaryIndex {
	return types.GlobalSecondaryIndex{
		IndexName: aws.String(gsiRetention),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("rt_pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("rt_at"), KeyType: types.KeyTypeRange},
		},
		Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
	}
}

// migrateRetention adds gsi_retention to a jobs table created before v2.7.0
// and indexes its existing terminal jobs once.
func (d *Driver) migrateRetention(ctx context.Context) error {
	out, err := d.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableJobs)})
	if err != nil {
		return fmt.Errorf("dynamodb migrate: describe jobs: %w", err)
	}
	if !hasIndex(out.Table, gsiRetention) {
		index := retentionIndex()
		if _, err := d.svc.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName: aws.String(tableJobs),
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("rt_pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("rt_at"), AttributeType: types.ScalarAttributeTypeS},
			},
			GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName: index.IndexName, KeySchema: index.KeySchema, Projection: index.Projection,
			}}},
		}); err != nil {
			return fmt.Errorf("dynamodb migrate: add %s: %w", gsiRetention, err)
		}
	}
	if err := d.waitIndexActive(ctx); err != nil {
		return err
	}
	return d.backfillRetention(ctx)
}

func hasIndex(table *types.TableDescription, name string) bool {
	if table == nil {
		return false
	}
	for _, index := range table.GlobalSecondaryIndexes {
		if aws.ToString(index.IndexName) == name {
			return true
		}
	}
	return false
}

func (d *Driver) waitIndexActive(ctx context.Context) error {
	deadline := d.clk.Now().Add(retentionIndexWaitTime)
	for {
		out, err := d.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableJobs)})
		if err != nil {
			return fmt.Errorf("dynamodb migrate: describe jobs: %w", err)
		}
		for _, index := range out.Table.GlobalSecondaryIndexes {
			if aws.ToString(index.IndexName) == gsiRetention && index.IndexStatus == types.IndexStatusActive {
				return nil
			}
		}
		if d.clk.Now().After(deadline) {
			return fmt.Errorf("dynamodb migrate: %s is not active after %s; run Migrate again later", gsiRetention, retentionIndexWaitTime)
		}
		timer := d.clk.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C():
		}
	}
}

// backfillRetention indexes terminal jobs finalized before rt_pk existed. A
// marker in the schedule cursors table makes it run once.
func (d *Driver) backfillRetention(ctx context.Context) error {
	marker, err := d.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableCursors),
		Key:       map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: retentionMigrationID}},
	})
	if err != nil {
		return fmt.Errorf("dynamodb migrate: read retention marker: %w", err)
	}
	if marker.Item != nil {
		return nil
	}
	paginator := dynamodb.NewScanPaginator(d.svc, &dynamodb.ScanInput{
		TableName:                aws.String(tableJobs),
		FilterExpression:         aws.String("attribute_not_exists(#rtpk) AND #state IN (:c, :d, :x)"),
		ExpressionAttributeNames: map[string]string{"#rtpk": "rt_pk", "#state": "state"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":c": &types.AttributeValueMemberS{Value: string(driver.JobStateCompleted)},
			":d": &types.AttributeValueMemberS{Value: string(driver.JobStateDiscarded)},
			":x": &types.AttributeValueMemberS{Value: string(driver.JobStateCancelled)},
		},
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("dynamodb migrate: scan terminal jobs: %w", err)
		}
		for _, item := range page.Items {
			var j dynamoJob
			if err := attributevalue.UnmarshalMap(item, &j); err != nil || j.FinalizedAt == "" {
				continue
			}
			hasResult := false
			if j.State == string(driver.JobStateCompleted) {
				res, err := d.svc.GetItem(ctx, &dynamodb.GetItemInput{
					TableName:            aws.String(tableResults),
					Key:                  map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: j.ID}},
					ProjectionExpression: aws.String("id"),
				})
				if err != nil {
					return fmt.Errorf("dynamodb migrate: read result %s: %w", j.ID, err)
				}
				hasResult = res.Item != nil
			}
			_, err := d.svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:                aws.String(tableJobs),
				Key:                      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: j.ID}},
				UpdateExpression:         aws.String("SET #rtpk = :pk, #rtat = :at"),
				ConditionExpression:      aws.String("attribute_not_exists(#rtpk) AND #state = :state"),
				ExpressionAttributeNames: map[string]string{"#rtpk": "rt_pk", "#rtat": "rt_at", "#state": "state"},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk":    &types.AttributeValueMemberS{Value: retentionPK(retentionBucket(driver.JobState(j.State), hasResult), j.ID)},
					":at":    &types.AttributeValueMemberS{Value: retentionTime(parseTime(j.FinalizedAt))},
					":state": &types.AttributeValueMemberS{Value: j.State},
				},
			})
			var cce *types.ConditionalCheckFailedException
			if err != nil && !errors.As(err, &cce) {
				return fmt.Errorf("dynamodb migrate: index job %s: %w", j.ID, err)
			}
		}
	}
	_, err = d.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableCursors),
		Item:      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: retentionMigrationID}},
	})
	if err != nil {
		return fmt.Errorf("dynamodb migrate: write retention marker: %w", err)
	}
	return nil
}

type retentionEntry struct {
	id string
	pk string
}

// expiredEntries queries every shard of the buckets for entries finalized at
// or before before, up to limit in total.
func (e *executor) expiredEntries(ctx context.Context, buckets []string, before time.Time, limit int) ([]retentionEntry, error) {
	if limit <= 0 {
		limit = 500
	}
	var entries []retentionEntry
	for _, bucket := range buckets {
		for shard := 0; shard < retentionShards && len(entries) < limit; shard++ {
			pk := bucket + "#" + strconv.Itoa(shard)
			out, err := e.svc.Query(ctx, &dynamodb.QueryInput{
				TableName:                aws.String(tableJobs),
				IndexName:                aws.String(gsiRetention),
				KeyConditionExpression:   aws.String("#rtpk = :pk AND #rtat <= :at"),
				ExpressionAttributeNames: map[string]string{"#rtpk": "rt_pk", "#rtat": "rt_at"},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":pk": &types.AttributeValueMemberS{Value: pk},
					":at": &types.AttributeValueMemberS{Value: retentionTime(before)},
				},
				Limit: aws.Int32(int32(limit - len(entries))),
			})
			if err != nil {
				return nil, fmt.Errorf("query %s: %w", gsiRetention, err)
			}
			for _, item := range out.Items {
				if id, ok := item["id"].(*types.AttributeValueMemberS); ok {
					entries = append(entries, retentionEntry{id: id.Value, pk: pk})
				}
			}
		}
	}
	return entries, nil
}

// JobPruneFinalized deletes jobs of params.State finalized at or before
// params.Before through gsi_retention. Each delete is conditional on rt_pk,
// so a job revived since the index read stays.
func (e *executor) JobPruneFinalized(ctx context.Context, params driver.JobPruneParams) (int, error) {
	buckets := []string{string(params.State)}
	if params.State == driver.JobStateCompleted {
		buckets = append(buckets, bucketCompletedResult)
	}
	entries, err := e.expiredEntries(ctx, buckets, params.Before, params.Limit)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, entry := range entries {
		out, err := e.svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName:                 aws.String(tableJobs),
			Key:                       map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: entry.id}},
			ConditionExpression:       aws.String("#rtpk = :pk"),
			ExpressionAttributeNames:  map[string]string{"#rtpk": "rt_pk"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: entry.pk}},
			ReturnValues:              types.ReturnValueAllOld,
		})
		var cce *types.ConditionalCheckFailedException
		if errors.As(err, &cce) {
			continue
		}
		if err != nil {
			return deleted, fmt.Errorf("delete job %s: %w", entry.id, err)
		}
		if err := deleteResult(ctx, e.svc, entry.id); err != nil {
			return deleted, err
		}
		if key, ok := out.Attributes["unique_key"].(*types.AttributeValueMemberS); ok && key.Value != "" {
			_, _ = e.svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(tableUniq),
				Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: key.Value}},
			})
		}
		deleted++
	}
	return deleted, nil
}

// JobResultPrune deletes the results of completed jobs finalized at or
// before params.Before and moves those jobs to the completed bucket.
func (e *executor) JobResultPrune(ctx context.Context, params driver.JobPruneParams) (int, error) {
	entries, err := e.expiredEntries(ctx, []string{bucketCompletedResult}, params.Before, params.Limit)
	if err != nil {
		return 0, err
	}
	for i, entry := range entries {
		if err := deleteResult(ctx, e.svc, entry.id); err != nil {
			return i, err
		}
		_, err := e.svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                aws.String(tableJobs),
			Key:                      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: entry.id}},
			UpdateExpression:         aws.String("SET #rtpk = :pk REMOVE #hr"),
			ConditionExpression:      aws.String("#rtpk = :old"),
			ExpressionAttributeNames: map[string]string{"#rtpk": "rt_pk", "#hr": "has_result"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":  &types.AttributeValueMemberS{Value: retentionPK(string(driver.JobStateCompleted), entry.id)},
				":old": &types.AttributeValueMemberS{Value: entry.pk},
			},
		})
		var cce *types.ConditionalCheckFailedException
		if err != nil && !errors.As(err, &cce) {
			return i, fmt.Errorf("move job %s out of %s: %w", entry.id, bucketCompletedResult, err)
		}
	}
	return len(entries), nil
}

var _ driver.JobPruner = (*executor)(nil)
