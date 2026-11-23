package goncordia_test

import (
	"context"
	"slices"
	"testing"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

type tagArgs struct {
	N int `json:"n"`
}

func (tagArgs) Kind() string { return "tag_args" }

type spanKey struct{}

// spanObserver puts a marker in the enqueue context, the way the OTel
// instrumentation puts its producer span there.
type spanObserver struct{}

func (spanObserver) StartEnqueue(ctx context.Context, _ goncordia.EnqueueStart) (context.Context, func(goncordia.EnqueueFinish)) {
	return context.WithValue(ctx, spanKey{}, "enqueue-span"), func(goncordia.EnqueueFinish) {}
}

func TestClientContextTags(t *testing.T) {
	ctx := context.Background()
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{
		Observer: spanObserver{},
		ContextTags: func(ctx context.Context) []string {
			span, _ := ctx.Value(spanKey{}).(string)
			return []string{"span=" + span}
		},
	})

	// Capacity left on purpose: an append without a copy would write into
	// the shared backing array.
	userTags := make([]string, 1, 4)
	userTags[0] = "user"
	opts := &core.InsertOpts{Tags: userTags}

	tagsOf := func(id string) []string {
		t.Helper()
		row, err := d.Executor().JobGetByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return row.Tags
	}
	want := []string{"user", "span=enqueue-span"}

	one, err := client.Enqueue(ctx, tagArgs{N: 1}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got := tagsOf(one.Job.ID); !slices.Equal(got, want) {
		t.Fatalf("Enqueue tags = %v, want %v", got, want)
	}

	many, err := client.EnqueueMany(ctx, []core.JobArgs{tagArgs{N: 2}, tagArgs{N: 3}}, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range many {
		if got := tagsOf(r.Job.ID); !slices.Equal(got, want) {
			t.Fatalf("EnqueueMany tags = %v, want %v", got, want)
		}
	}

	batch, err := client.EnqueueBatch(ctx, []goncordia.InsertRequest{{Args: tagArgs{N: 4}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := tagsOf(batch[0].Job.ID); !slices.Equal(got, []string{"span=enqueue-span"}) {
		t.Fatalf("EnqueueBatch tags = %v", got)
	}

	if !slices.Equal(opts.Tags, []string{"user"}) || userTags[:2][1] != "" {
		t.Fatalf("caller's InsertOpts.Tags was modified: %v / %v", opts.Tags, userTags[:2])
	}
}

func TestClientContextTagsEmpty(t *testing.T) {
	ctx := context.Background()
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{
		ContextTags: func(context.Context) []string { return nil },
	})
	res, err := client.Enqueue(ctx, tagArgs{N: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	row, err := d.Executor().JobGetByID(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(row.Tags) != 0 {
		t.Fatalf("tags = %v, want none", row.Tags)
	}
}
