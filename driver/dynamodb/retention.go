package dynamodbdriver

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	retentionShards       = 16
	bucketCompletedResult = "completed_result"
	retentionTimeFormat   = "2006-01-02T15:04:05.000000000Z"
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
